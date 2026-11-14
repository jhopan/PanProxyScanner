// Package scraper menjalankan satu siklus lengkap: ambil semua sumber, hitung prioritas,
// verifikasi dengan handshake nyata, lalu simpan hasilnya.
package scraper

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jhopan/Pan-ProxyScanner/internal/checker"
	"github.com/jhopan/Pan-ProxyScanner/internal/settings"
	"github.com/jhopan/Pan-ProxyScanner/internal/sources"
	"github.com/jhopan/Pan-ProxyScanner/internal/store"
)

// deadRefreshFactor menentukan seberapa jarang baris **mati** diuji ulang dibanding baris
// hidup: 24× dari refresh_after (mis. 30 menit → 12 jam). Verifikasi ulang ribuan kandidat
// mass-scan yang sudah mati hanya membakar waktu; proxy yang hidup kembali tetap terdeteksi
// dalam hitungan jam.
const deadRefreshFactor = 24

// Port yang paling sering dipakai proxy publik — dipakai sebagai tie-breaker.
var preferredPorts = map[int]bool{
	1080: true, 1081: true, 3128: true, 4145: true, 4146: true, 8080: true, 8081: true,
	8000: true, 8888: true, 9090: true, 9999: true, 443: true, 80: true, 10808: true,
}

// Options mengatur satu siklus scraping.
type Options struct {
	SourceList     []sources.Source
	Cap            int           // batas kandidat baru yang diverifikasi
	Offset         int           // geser slice (untuk rotasi antar siklus)
	Jobs           int           // jumlah verifikasi paralel
	FetchTimeout   time.Duration // timeout per sumber
	Checker        checker.Options
	RefreshLimit   int            // berapa baris tersimpan yang diverifikasi ulang
	RefreshAfter   time.Duration  // umur catatan sebelum diverifikasi ulang
	MinSources     int            // hanya verifikasi kandidat yang muncul di >= N sumber (0 = semua)
	CountryTargets map[string]int // target minimal proxy sanggup TLS per negara (kode 2 huruf)
	CountryTopUp   int            // kandidat tambahan per siklus untuk negara yang belum mencapai target
	LowYieldCycles int            // siklus tanpa hasil sebelum sumber dimatikan (0 = tidak)
	LowYieldMin    int            // minimal entri agar sebuah sumber dinilai yield-nya

	// UpdateSourceHealth menyalakan pencatatan kesehatan sumber; SourceHealthLimit adalah
	// berapa kali kosong berturut-turut sebelum sumber dimatikan otomatis (0 = tidak dimatikan).
	UpdateSourceHealth bool
	SourceHealthLimit  int
}

// DefaultOptions memberi nilai wajar untuk mesin biasa. SourceList dibiarkan kosong:
// daftar sumber diambil dari database setiap siklus (lihat resolveSources).
func DefaultOptions() Options {
	return Options{
		Cap:          20000,
		Jobs:         150,
		FetchTimeout: 30 * time.Second,
		Checker:      checker.DefaultOptions(),
		RefreshLimit: 200,
		RefreshAfter: 30 * time.Minute,
	}
}

// resolveSources mengisi daftar sumber dari database (hanya yang aktif). Kalau database
// tidak tersedia atau kosong, katalog bawaan dipakai supaya scraping tetap jalan.
func resolveSources(st *store.Store, opts *Options) {
	if len(opts.SourceList) > 0 {
		return
	}
	if st != nil {
		rows, err := st.Sources(true)
		if err == nil && len(rows) > 0 {
			list := make([]sources.Source, 0, len(rows))
			for _, row := range rows {
				list = append(list, sources.Source{Name: row.Name, URL: row.URL, Kind: row.Kind, Optional: row.Optional})
			}
			opts.SourceList = list
			return
		}
	}
	opts.SourceList = sources.All
}

// SourceStat adalah hasil satu sumber.
type SourceStat struct {
	Name     string `json:"name"`
	Kind     string `json:"kind"`
	Count    int    `json:"count"`
	Bytes    int    `json:"bytes"`
	MS       int64  `json:"ms"`
	Status   int    `json:"status"`
	Error    string `json:"error,omitempty"`
	Optional bool   `json:"optional"`
}

// Progress adalah status siklus yang sedang/terakhir berjalan (dipakai API/UI).
type Progress struct {
	Running    bool         `json:"running"`
	Phase      string       `json:"phase"`
	Sources    []SourceStat `json:"sources,omitempty"`
	Candidates int          `json:"candidates"`
	Checked    int          `json:"checked"`
	Alive      int          `json:"alive"`
	Dead       int          `json:"dead"`
	Refreshed  int          `json:"refreshed"`
	StartedAt  time.Time    `json:"started_at"`
	EndedAt    time.Time    `json:"ended_at,omitempty"`
	Error      string       `json:"error,omitempty"`
}

// Runner menjaga status siklus supaya UI bisa memantau dari HTTP.
type Runner struct {
	store *store.Store

	// OnFinish dipanggil setiap siklus selesai (dipakai untuk sinkronisasi ke Supabase).
	OnFinish func(progress Progress, stats []SourceStat)

	mu       sync.Mutex
	progress Progress
	cancel   context.CancelFunc
}

// NewRunner membuat runner untuk store tertentu.
func NewRunner(st *store.Store) *Runner {
	return &Runner{store: st}
}

// Sink menerima pembaruan progres. Runner memakainya supaya UI bisa memantau dari HTTP,
// sedangkan subcommand `scrape` memakainya untuk mencetak progres ke terminal.
type Sink interface {
	Update(func(*Progress))
}

// Progress mengembalikan salinan status terakhir.
func (r *Runner) Progress() Progress {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.progress
}

// Update menerapkan perubahan ke status runner (implementasi Sink).
func (r *Runner) Update(fn func(*Progress)) { r.set(fn) }

func (r *Runner) set(fn func(p *Progress)) {
	r.mu.Lock()
	fn(&r.progress)
	r.mu.Unlock()
}

// Start menjalankan siklus di latar belakang. Mengembalikan error kalau sudah ada yang jalan.
func (r *Runner) Start(opts Options) error {
	r.mu.Lock()
	if r.progress.Running {
		r.mu.Unlock()
		return fmt.Errorf("siklus lain masih berjalan")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.progress = Progress{Running: true, Phase: "fetch", StartedAt: time.Now()}
	r.mu.Unlock()

	go func() {
		defer cancel()
		stats, err := Run(ctx, r.store, opts, r)
		r.set(func(p *Progress) {
			p.Running = false
			p.EndedAt = time.Now()
			p.Sources = stats
			p.Phase = "selesai"
			if err != nil {
				p.Error = err.Error()
			}
		})
		if r.OnFinish != nil {
			r.OnFinish(r.Progress(), stats)
		}
	}()
	return nil
}

// Stop membatalkan siklus yang sedang berjalan.
func (r *Runner) Stop() {
	r.mu.Lock()
	cancel := r.cancel
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// CheckSources mengunduh semua sumber dan mengembalikan statistik per sumber + jumlah
// kandidat unik. Dipakai subcommand `sources` sebagai alat uji katalog.
func CheckSources(ctx context.Context, st *store.Store, opts Options) ([]SourceStat, int) {
	resolveSources(st, &opts)
	stats, union, _, _, _ := fetchAll(ctx, opts)
	return stats, len(union)
}

// Run menjalankan satu siklus penuh (blocking).
func Run(ctx context.Context, st *store.Store, opts Options, sink Sink) ([]SourceStat, error) {
	if opts.Jobs <= 0 {
		opts.Jobs = 50
	}
	if opts.Cap <= 0 {
		opts.Cap = 10000
	}
	resolveSources(st, &opts)

	// 1. Ambil semua sumber paralel.
	stats, union, counts, countries, origin := fetchAll(ctx, opts)
	report(sink, func(p *Progress) {
		p.Sources = stats
		p.Phase = "verify"
		p.Candidates = len(union)
	})

	// 2. Prioritaskan negara yang stoknya tipis, lalu yang muncul di banyak sumber
	//    (sinyal kualitas), lalu port umum.
	thin := thinCountries(st, opts.CountryTargets)
	if len(thin) > 0 {
		codes := make([]string, 0, len(thin))
		for code := range thin {
			codes = append(codes, code)
		}
		sort.Strings(codes)
		log.Printf("stok tipis (didahulukan): %s", strings.Join(codes, ","))
	}
	// Bobot kandidat: 1 + hasil historis sumber yang mencantumkannya. Kandidat dari sumber
	// yang produktif diverifikasi lebih dulu, tanpa mengubah cakupan (semua tetap di-scrape).
	yields, _ := st.SourceYields()
	weights := make(map[string]int, len(origin))
	for addr, srcs := range origin {
		w := len(srcs)
		for _, name := range srcs {
			w += yields[name]
		}
		weights[addr] = w
	}
	ranked := rank(union, counts, weights, countries, thin, opts.MinSources)

	// 2b. Catat kesehatan tiap sumber: yang berkali-kali kosong dimatikan otomatis.
	if cfgHealth := opts.SourceHealthLimit; cfgHealth > 0 || opts.UpdateSourceHealth {
		autoDisabled := make([]string, 0, 2)
		for _, stat := range stats {
			if stat.Optional {
				continue
			}
			off, err := st.UpdateSourceHealth(stat.Name, stat.Count, stat.Error, opts.SourceHealthLimit, opts.LowYieldCycles, opts.LowYieldMin)
			if err == nil && off {
				autoDisabled = append(autoDisabled, stat.Name)
			}
		}
		if len(autoDisabled) > 0 {
			log.Printf("sumber dimatikan otomatis (kosong %d kali): %s", opts.SourceHealthLimit, strings.Join(autoDisabled, ", "))
		}
	}

	// 3. Buang yang sudah ada di database sebelum memotong slice: kalau tidak, siklus
	// berikutnya akan mengambil 400 kandidat teratas yang sama dan tidak mengerjakan apa pun.
	known, err := st.Known()
	if err != nil {
		return stats, err
	}
	unknown := make([]string, 0, len(ranked))
	for _, addr := range ranked {
		if !known[addr] {
			unknown = append(unknown, addr)
		}
	}

	end := opts.Offset + opts.Cap
	if opts.Offset > len(unknown) {
		opts.Offset = 0
	}
	if end > len(unknown) {
		end = len(unknown)
	}
	fresh := unknown[opts.Offset:end]
	report(sink, func(p *Progress) {
		p.Candidates = len(fresh)
		p.Phase = "verify"
	})

	if _, _, err := verifyAll(ctx, st, fresh, counts, countries, origin, opts, sink); err != nil {
		return stats, err
	}

	// 4. Segarkan baris yang sudah tersimpan tapi catatannya basi.
	if opts.RefreshLimit > 0 && opts.RefreshAfter > 0 {
		report(sink, func(p *Progress) { p.Phase = "refresh" })
		// Baris mati diuji ulang jauh lebih jarang (kelipatan deadRefreshFactor): cukup untuk
		// memastikan proxy yang hidup kembali terdeteksi, tanpa membakar jatah refresh untuk
		// ribuan kandidat mass-scan yang sudah jelas mati.
		stale, err := st.Stale(opts.RefreshAfter, opts.RefreshAfter*deadRefreshFactor, opts.RefreshLimit)
		if err != nil {
			return stats, err
		}
		addrs := make([]string, 0, len(stale))
		for _, s := range stale {
			addrs = append(addrs, s.Addr())
		}
		refreshed, _, err := verifyAll(ctx, st, addrs, counts, countries, nil, opts, sink)
		if err != nil {
			return stats, err
		}
		report(sink, func(p *Progress) { p.Refreshed = refreshed })
	}

	// 5. Pass tambahan untuk negara yang stok TLS-nya belum mencapai target: verifikasi ulang
	//    baris lokal berlabel negara itu (budget terpisah, jadi tidak memakan jatah utama).
	if opts.CountryTopUp > 0 {
		unmet := unmetCountries(st, opts.CountryTargets)
		if len(unmet) > 0 {
			report(sink, func(p *Progress) { p.Phase = "target negara" })
			log.Printf("target negara belum terpenuhi: %s — verifikasi tambahan maks %d", strings.Join(unmet, ","), opts.CountryTopUp)
			extra, err := st.ThinCountryCandidates(unmet, opts.CountryTopUp, opts.RefreshAfter)
			if err == nil && len(extra) > 0 {
				filled, _, verr := verifyAll(ctx, st, extra, counts, countries, origin, opts, sink)
				if verr != nil {
					return stats, verr
				}
				report(sink, func(p *Progress) { p.Refreshed += filled })
			}
		}
	}

	return stats, nil
}

// unmetCountries mengembalikan kode negara yang stok TLS-nya masih di bawah targetnya. Hanya
// negara yang targetnya eksplisit di pengaturan yang masuk sini (bagian murni bisa diuji
// tanpa database lewat unmetFromCounts).
func unmetCountries(st *store.Store, targets map[string]int) []string {
	list, err := st.Countries(true)
	if err != nil {
		return nil
	}
	return unmetFromCounts(list, targets)
}

func unmetFromCounts(list []store.CountryCount, targets map[string]int) []string {
	counts := make(map[string]int, len(list))
	for _, c := range list {
		counts[strings.ToUpper(strings.TrimSpace(c.Country))] = c.TLS
	}
	out := make([]string, 0, len(targets))
	for code, target := range targets {
		key := strings.ToUpper(strings.TrimSpace(code))
		if len(key) != 2 || target <= 0 {
			continue
		}
		if counts[key] < target {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

// fetchAll mengunduh semua sumber paralel dan menggabungkan hasilnya, termasuk kode negara
// dari sumber JSON yang menyertakan data lokasi.
func fetchAll(ctx context.Context, opts Options) ([]SourceStat, map[string]struct{}, map[string]int, map[string]string, map[string][]string) {
	client := &http.Client{Timeout: opts.FetchTimeout}
	union := make(map[string]struct{})
	counts := make(map[string]int)
	countries := make(map[string]string)
	// origin: sumber mana saja yang mencantumkan sebuah kandidat — dipakai untuk atribusi hasil
	// per sumber (yield) setelah verifikasi.
	origin := make(map[string][]string)
	stats := make([]SourceStat, len(opts.SourceList))

	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 12)
	for i, src := range opts.SourceList {
		wg.Add(1)
		go func(i int, src sources.Source) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			stat := SourceStat{Name: src.Name, Kind: src.Kind, Optional: src.Optional}
			started := time.Now()
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.URL, nil)
			if err == nil {
				req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) panproxyscanner/1.0")
				var resp *http.Response
				resp, err = client.Do(req)
				if err == nil {
					defer resp.Body.Close()
					stat.Status = resp.StatusCode
					if resp.StatusCode == http.StatusOK {
						body, readErr := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
						if readErr != nil {
							err = readErr
						} else {
							stat.Bytes = len(body)
							found := sources.Extract(string(body))
							stat.Count = len(found)
							// union/counts dibagi antar goroutine: wajib dikunci.
							mu.Lock()
							for addr := range found {
								union[addr] = struct{}{}
								counts[addr]++
								origin[addr] = append(origin[addr], src.Name)
							}
							mu.Unlock()

							// Sumber JSON (proxifly/geonode) membawa kode negara.
							if src.Kind == "json" || src.Kind == "api" {
								hints := sources.ExtractCountryHints(string(body))
								if len(hints) > 0 {
									mu.Lock()
									for addr, cc := range hints {
										if _, ok := countries[addr]; !ok {
											countries[addr] = cc
										}
									}
									mu.Unlock()
								}
							}
						}
					} else {
						err = fmt.Errorf("HTTP %d", resp.StatusCode)
					}
				}
			}
			if err != nil {
				stat.Error = err.Error()
			}
			stat.MS = time.Since(started).Milliseconds()
			stats[i] = stat
		}(i, src)
	}
	wg.Wait()
	return stats, union, counts, countries, origin
}

// rank menyusun urutan kandidat: terbanyak muncul di sumber dulu, lalu port umum.
// defaultCountryTarget: negara yang tidak punya target khusus tetap didahulukan selama stok
// TLS-nya di bawah angka ini. Jadi pencarian tetap menyeluruh (semua negara di-scrape), hanya
// urutan verifikasinya yang diatur supaya negara tipis tidak kalah oleh daftar mass-scan.
const defaultCountryTarget = 3

// thinFromCounts adalah bagian murni dari penentuan negara tipis (bisa diuji tanpa database).
func thinFromCounts(list []store.CountryCount, targets map[string]int) map[string]bool {
	out := make(map[string]bool, len(targets)+8)
	for code, target := range targets {
		key := strings.ToUpper(strings.TrimSpace(code))
		if len(key) == 2 && target > 0 {
			out[key] = true // negara bertarget khusus dianggap tipis sampai targetnya terpenuhi
		}
	}
	for _, c := range list {
		code := strings.ToUpper(strings.TrimSpace(c.Country))
		if code == "" {
			continue
		}
		target := defaultCountryTarget
		if t, ok := targets[code]; ok {
			target = t
		}
		switch {
		case target <= 0:
			delete(out, code) // target 0 = negara ini tidak diprioritaskan
		case c.TLS >= target:
			delete(out, code)
		default:
			out[code] = true
		}
	}
	return out
}

func thinCountries(st *store.Store, targets map[string]int) map[string]bool {
	list, err := st.Countries(true)
	if err != nil {
		return thinFromCounts(nil, targets)
	}
	return thinFromCounts(list, targets)
}

func rank(union map[string]struct{}, counts map[string]int, weights map[string]int, countries map[string]string, thin map[string]bool, minSources int) []string {
	if weights == nil {
		// Pemanggil yang belum punya data hasil per sumber: bobot = jumlah sumber (perilaku lama).
		weights = counts
	}
	list := make([]string, 0, len(union))
	for addr := range union {
		if minSources > 0 && counts[addr] < minSources {
			continue
		}
		list = append(list, addr)
	}
	sort.Slice(list, func(i, j int) bool {
		// 1. Negara yang stoknya tipis lebih dulu (kode negara dari sumber JSON kalau ada).
		ti, tj := thin[strings.ToUpper(countries[list[i]])], thin[strings.ToUpper(countries[list[j]])]
		if ti != tj {
			return ti
		}
		// 2. Bobot kandidat (jumlah sumber + hasil historis tiap sumber) = sinyal kualitas.
		wi, wj := weights[list[i]], weights[list[j]]
		if wi != wj {
			return wi > wj
		}
		pi, pj := preferredPort(list[i]), preferredPort(list[j])
		if pi != pj {
			return pi < pj
		}
		return list[i] < list[j]
	})
	return list
}

func preferredPort(addr string) int {
	_, portStr, err := splitAddr(addr)
	if err != nil {
		return 1
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 1
	}
	if preferredPorts[port] {
		return 0
	}
	return 1
}

func splitAddr(addr string) (string, string, error) {
	i := strings.LastIndex(addr, ":")
	if i <= 0 {
		return "", "", fmt.Errorf("alamat tidak valid: %s", addr)
	}
	return addr[:i], addr[i+1:], nil
}

// verifyAll memverifikasi daftar alamat dengan pool worker dan menyimpan hasilnya.
// Progres dilaporkan sebagai delta supaya UI bisa menampilkan total lintas tahap.
// creditSources mencatat hasil ke sumber asal kandidat. Hanya proxy HIDUP dan sanggup TLS yang
// dihitung: sumber dengan ratusan ribu entri tapi nol hasil akan terlihat dari yield_ok.
func creditSources(st *store.Store, origin map[string][]string, addr string, tlsOK bool) {
	if st == nil || !tlsOK {
		return
	}
	for _, name := range origin[addr] {
		_ = st.AddSourceYield(name, 1)
	}
}

func verifyAll(ctx context.Context, st *store.Store, addrs []string, counts map[string]int, countries map[string]string, origin map[string][]string, opts Options, sink Sink) (int, int, error) {
	if len(addrs) == 0 {
		return 0, 0, nil
	}
	type outcome struct {
		addr   string
		result checker.Result
	}

	jobs := make(chan string)
	results := make(chan outcome, opts.Jobs)
	var wg sync.WaitGroup
	for range opts.Jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range jobs {
				ip, portStr, err := splitAddr(addr)
				if err != nil {
					continue
				}
				port, _ := strconv.Atoi(portStr)
				res := checker.Check(ctx, addr, opts.Checker)
				results <- outcome{addr: fmt.Sprintf("%s:%d", ip, port), result: res}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, addr := range addrs {
			select {
			case <-ctx.Done():
				return
			case jobs <- addr:
			}
		}
	}()
	go func() {
		wg.Wait()
		close(results)
	}()

	alive, dead, checked := 0, 0, 0
	deltaChecked, deltaAlive, deltaDead := 0, 0, 0
	var firstErr error
	for out := range results {
		ip, portStr, err := splitAddr(out.addr)
		if err != nil {
			continue
		}
		port, _ := strconv.Atoi(portStr)
		srcCount := counts[out.addr]
		country := countries[out.addr]
		if country == "" {
			// negara dari trace proxy (loc=) lebih akurat daripada metadata sumber,
			// jadi dipakai kalau sumber tidak menyebutkannya.
			country = out.result.Country
		}
		// Kesalahan tulis tidak boleh menghentikan loop: kalau kita berhenti di tengah,
		// worker masih memblokir di kirim-hasil dan siklus berikutnya tidak pernah jalan.
		// Jadi error pertama disimpan, sisa hasil tetap dikuras sampai channel tertutup.
		if out.result.Alive {
			if err := st.RecordAlive(ip, port, out.result.Protocol, country, out.result.MS, srcCount, out.result.TLS, out.result.AltOK, out.result.ExitIP); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				alive++
				deltaAlive++
				// Atribusi hasil ke sumber asalnya: hanya proxy yang HIDUP dan sanggup TLS yang
				// dihitung, jadi sumber dengan banyak entri tapi nol hasil akan terlihat di yield.
				creditSources(st, origin, out.addr, out.result.TLS)
			}
		} else {
			if err := st.RecordDead(ip, port, country, srcCount); err != nil {
				if firstErr == nil {
					firstErr = err
				}
			} else {
				dead++
				deltaDead++
			}
		}
		checked++
		deltaChecked++
		if checked%50 == 0 {
			report(sink, func(p *Progress) {
				p.Checked += deltaChecked
				p.Alive += deltaAlive
				p.Dead += deltaDead
			})
			deltaChecked, deltaAlive, deltaDead = 0, 0, 0
		}
	}
	report(sink, func(p *Progress) {
		p.Checked += deltaChecked
		p.Alive += deltaAlive
		p.Dead += deltaDead
	})
	if firstErr != nil {
		return alive, dead, fmt.Errorf("tulis hasil verifikasi: %w", firstErr)
	}
	return alive, dead, nil
}

// RemoteRow adalah baris dari sumber eksternal (mis. tabel proxy_pool di Supabase) yang
// perlu diverifikasi ulang di sini.
type RemoteRow struct {
	ID      string
	IP      string
	Port    string
	Country string
}

// Verdict adalah hasil verifikasi satu baris eksternal.
type Verdict struct {
	Row    RemoteRow
	Alive  bool
	PingMS int
	TLS    bool   // sanggup tunnel TLS 443 (syarat VLESS)
	AltOK  bool   // sanggup tunnel port non-standar
	ExitIP string // alamat keluar sebenarnya
}

// RemoteSink menerima hasil verifikasi (dikirim per batch supaya hemat request).
type RemoteSink interface {
	UpdateVerdicts(ctx context.Context, verdicts []Verdict) error
}

// Summary adalah ringkasan hasil verifikasi baris eksternal.
type Summary struct {
	Checked int `json:"checked"`
	Alive   int `json:"alive"`
	Dead    int `json:"dead"`
	Errors  int `json:"errors"`
}

// VerifyRemote memverifikasi baris dari sumber eksternal satu per satu dengan handshake
// nyata, menulis hasilnya kembali ke sumber itu **per batch** (hemat request/bandwidth),
// dan sekaligus mencatatnya di database lokal.
func VerifyRemote(ctx context.Context, st *store.Store, remote RemoteSink, rows []RemoteRow, opts Options, sink Sink) (Summary, error) {
	summary := Summary{}
	if len(rows) == 0 {
		return summary, nil
	}
	if opts.Jobs <= 0 {
		opts.Jobs = 50
	}
	const batchSize = 100
	type outcome struct {
		row    RemoteRow
		result checker.Result
	}

	jobs := make(chan RemoteRow)
	outcomes := make(chan outcome, opts.Jobs)
	var wg sync.WaitGroup
	for range opts.Jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for row := range jobs {
				addr := row.IP + ":" + row.Port
				res := checker.Check(ctx, addr, opts.Checker)
				outcomes <- outcome{row: row, result: res}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, row := range rows {
			select {
			case <-ctx.Done():
				return
			case jobs <- row:
			}
		}
	}()
	go func() {
		wg.Wait()
		close(outcomes)
	}()

	pending := make([]Verdict, 0, batchSize)
	flush := func() error {
		if len(pending) == 0 || remote == nil {
			pending = pending[:0]
			return nil
		}
		if err := remote.UpdateVerdicts(ctx, pending); err != nil {
			summary.Errors += len(pending)
		}
		pending = pending[:0]
		return nil
	}

	for out := range outcomes {
		summary.Checked++
		if out.result.Alive {
			summary.Alive++
		} else {
			summary.Dead++
		}
		pending = append(pending, Verdict{Row: out.row, Alive: out.result.Alive, PingMS: out.result.MS, TLS: out.result.TLS, AltOK: out.result.AltOK, ExitIP: out.result.ExitIP})
		if len(pending) >= batchSize {
			_ = flush()
		}
		if st != nil {
			port, _ := strconv.Atoi(out.row.Port)
			country := out.row.Country
			if country == "" {
				country = out.result.Country
			}
			if out.result.Alive {
				_ = st.RecordAlive(out.row.IP, port, out.result.Protocol, country, out.result.MS, 0, out.result.TLS, out.result.AltOK, out.result.ExitIP)
			} else {
				_ = st.RecordDead(out.row.IP, port, country, 0)
			}
		}
		report(sink, func(p *Progress) {
			p.Phase = "verifikasi supabase"
			p.Checked++
			if out.result.Alive {
				p.Alive++
			} else {
				p.Dead++
			}
		})
	}
	_ = flush()
	return summary, nil
}

// OptionsFromSettings menyusun opsi scraper dari pengaturan yang bisa diubah di website.
func OptionsFromSettings(cfg settings.Settings) Options {
	opts := DefaultOptions()
	opts.Cap = cfg.Cap
	opts.Jobs = cfg.Jobs
	opts.MinSources = cfg.MinSources
	opts.RefreshLimit = cfg.RefreshLimit
	opts.RefreshAfter = cfg.RefreshAfter()
	opts.Checker.ProbeTLS = cfg.ProbeTLS
	opts.Checker.ProbeAlt = cfg.ProbeAlt
	opts.UpdateSourceHealth = cfg.AutoDisableSources
	opts.SourceHealthLimit = cfg.ZeroStreakLimit
	opts.CountryTargets = cfg.CountryTargets
	opts.CountryTopUp = cfg.CountryTopUp
	if cfg.AutoDisableLowYield {
		opts.LowYieldCycles = cfg.LowYieldCycles
		opts.LowYieldMin = cfg.LowYieldMinEntries
	}
	return opts
}

// Scheduler menjalankan siklus otomatis. Konfigurasinya bisa diubah saat berjalan (dari
// halaman Pengaturan di website) tanpa restart proses.
//
// Dua mode: "interval" (tiap N menit) dan "daily" (setiap hari pada jam tertentu). Kalau
// Pipeline diisi dan PipelineEnabled aktif, tiap jadwal menjalankan pipeline penuh
// (tarik dari Supabase → uji → scrape sumber → push kembali), bukan hanya siklus scraping.
type Scheduler struct {
	runner *Runner

	// Pipeline dijalankan sebagai satu jadwal kalau pengaturan PipelineEnabled aktif.
	// Pipeline menerima testOnly=true untuk jadwal uji berkala: menguji ulang yang tersimpan
	// tanpa scraping sumber baru (lebih hemat).
	Pipeline func(testOnly bool) error

	mu      sync.Mutex
	cfg     settings.Settings
	cancel  context.CancelFunc
	running bool
	offset  int
	lastRun time.Time
}

// NewScheduler membuat penjadwal untuk runner tertentu.
func NewScheduler(runner *Runner, cfg settings.Settings) *Scheduler {
	return &Scheduler{runner: runner, cfg: cfg}
}

// Config mengembalikan salinan pengaturan saat ini.
func (s *Scheduler) Config() settings.Settings {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg
}

// Running menandakan loop otomatis sedang aktif.
func (s *Scheduler) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// LastRun mengembalikan waktu siklus otomatis terakhir dipicu.
func (s *Scheduler) LastRun() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastRun
}

// Update mengganti konfigurasi; kalau loop sedang aktif, loop dijalankan ulang dengan
// interval baru. Kalau `auto` baru dinyalakan lewat pengaturan, penjadwal ikut dinyalakan.
func (s *Scheduler) Update(cfg settings.Settings) {
	s.mu.Lock()
	s.cfg = cfg
	cancel := s.cancel
	s.cancel = nil
	s.running = false
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if cfg.Auto {
		_ = s.Start()
	}
}

// Start menyalakan loop otomatis memakai konfigurasi saat ini.
func (s *Scheduler) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return nil
	}
	cfg := s.cfg
	cfg.Auto = true
	s.cfg = cfg
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.running = true
	s.mu.Unlock()

	go func() {
		// Jadwal 1: SCrape semua sumber — harian pada jam di zona waktu pengaturan, atau interval.
		go func() {
			if cfg.ScheduleMode == "daily" {
				for {
					cur := s.Config()
					next := settings.NextDailyIn(cur.DailyAt, time.Now(), cur.Location())
					log.Printf("jadwal scrape: berikutnya %s (zona %s)", next.Format("2006-01-02 15:04 MST"), cur.ScheduleTimezone)
					timer := time.NewTimer(time.Until(next))
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
						s.RunScheduled(false)
					}
				}
			}
			s.RunScheduled(false)
			ticker := time.NewTicker(cfg.Interval())
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					s.RunScheduled(false)
				}
			}
		}()

		// Jadwal 2: uji ulang berkala (bawaan 6 jam, kelipatan dari 00:00 UTC) supaya pool
		// tetap terjaga tanpa menyerap sumber baru setiap kali.
		hours := cfg.TestIntervalHours
		if hours > 0 {
			for {
				next := settings.NextTestRun(time.Now(), s.Config().TestIntervalHours)
				log.Printf("jadwal tes: berikutnya %s UTC (tiap %d jam)", next.Format("2006-01-02 15:04"), s.Config().TestIntervalHours)
				timer := time.NewTimer(time.Until(next))
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
					s.RunScheduled(true)
				}
			}
		}
		<-ctx.Done()
	}()
	return nil
}

// RunScheduled menjalankan pekerjaan sesuai jadwal: pipeline penuh kalau diaktifkan,
// atau satu siklus scraping biasa.
func (s *Scheduler) RunScheduled(testOnly bool) {
	cfg := s.Config()
	// Catat waktu jalan juga untuk jalur pipeline: tanpa ini `LastRun()` tetap nol dan UI
	// menampilkan "jadwal berikutnya" = sekarang (menyesatkan).
	s.mu.Lock()
	s.lastRun = time.Now()
	s.mu.Unlock()
	if testOnly {
		// Uji berkala: hanya menguji ulang yang sudah tersimpan + push bukti terbaru.
		if s.Pipeline == nil {
			log.Printf("jadwal tes: pipeline belum disiapkan, dilewati")
			return
		}
		if err := s.Pipeline(true); err != nil {
			log.Printf("tes berkala gagal: %v", err)
		}
		return
	}
	if cfg.PipelineEnabled && s.Pipeline != nil {
		if err := s.Pipeline(false); err != nil {
			log.Printf("pipeline gagal: %v", err)
		}
		return
	}
	s.trigger()
}

// Stop mematikan loop otomatis (siklus yang sedang jalan tetap diselesaikan/dibatalkan
// lewat Runner.Stop kalau dipanggil terpisah).
func (s *Scheduler) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.cancel = nil
	s.running = false
	cfg := s.cfg
	cfg.Auto = false
	s.cfg = cfg
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// TriggerNow menjalankan satu siklus sekarang juga (dipakai tombol "Sekarang").
func (s *Scheduler) TriggerNow() error {
	s.mu.Lock()
	cfg := s.cfg
	offset := s.offset
	if cfg.AutoRotate {
		s.offset += cfg.Cap
	}
	s.lastRun = time.Now()
	s.mu.Unlock()

	opts := OptionsFromSettings(cfg)
	opts.Offset = offset
	return s.runner.Start(opts)
}

func (s *Scheduler) trigger() {
	if err := s.TriggerNow(); err != nil {
		// siklus lain masih jalan; biarkan lewat
		return
	}
}

func report(sink Sink, fn func(*Progress)) {
	if sink != nil {
		sink.Update(fn)
	}
}
