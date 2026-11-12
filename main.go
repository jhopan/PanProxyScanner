// Pan Proxy Scanner — pengumpul + verifikator proxy publik, dengan website dan API.
//
//	go run . serve            # jalankan website + API (default 0.0.0.0:8787)
//	go run . scrape           # satu siklus scraping, lalu keluar
//	go run . sources          # uji semua sumber daftar proxy (status + jumlah)
//
// Fokus: proxy yang bisa dipakai tunnel VLESS (handler TCP lewat SOCKS5 / HTTP CONNECT),
// jadi verifikasinya bukan sekadar "port terbuka" tetapi handshake sungguhan.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jhopan/pan-proxy-scanner/internal/api"
	"github.com/jhopan/pan-proxy-scanner/internal/checker"
	"github.com/jhopan/pan-proxy-scanner/internal/envfile"
	"github.com/jhopan/pan-proxy-scanner/internal/scraper"
	"github.com/jhopan/pan-proxy-scanner/internal/settings"
	"github.com/jhopan/pan-proxy-scanner/internal/sources"
	"github.com/jhopan/pan-proxy-scanner/internal/store"
)

var (
	flagAddr         = flag.String("addr", "0.0.0.0:8787", "alamat listen website + API (0.0.0.0 = bisa diakses dari LAN)")
	flagDB           = flag.String("db", "proxies.db", "path database SQLite")
	flagConfig       = flag.String("config", "", "path settings.json (default ~/.pan-proxy-scanner/settings.json)")
	flagJobs         = flag.Int("jobs", 150, "jumlah verifikasi paralel (scrape/check/verify)")
	flagCap          = flag.Int("cap", 20000, "batas kandidat baru per siklus")
	flagOffset       = flag.Int("offset", 0, "geser slice kandidat (rotasi antar siklus)")
	flagMinSources   = flag.Int("min-sources", 0, "hanya verifikasi kandidat yang muncul di >= N sumber")
	flagInterval     = flag.Duration("interval", 0, "jarak antar siklus otomatis saat start (0 = ikut settings.json)")
	flagScrapeNow    = flag.Bool("scrape-now", false, "jalankan satu siklus begitu server hidup")
	flagFresh        = flag.Duration("fresh", 6*time.Hour, "jendela kesegaran untuk statistik & API")
	flagRefreshLimit = flag.Int("refresh-limit", 300, "berapa baris tersimpan yang diverifikasi ulang per siklus")
	flagRefreshAfter = flag.Duration("refresh-after", 30*time.Minute, "umur catatan sebelum diverifikasi ulang")
	flagFetchTimeout = flag.Duration("fetch-timeout", 30*time.Second, "timeout pengambilan satu sumber")
	flagDialTimeout  = flag.Duration("dial-timeout", 4*time.Second, "timeout koneksi ke proxy")
	flagStageTimeout = flag.Duration("stage-timeout", 4*time.Second, "timeout handshake proxy")
	flagProbeTimeout = flag.Duration("probe-timeout", 6*time.Second, "timeout permintaan HTTP lewat tunnel")
	flagPurgeDead    = flag.Duration("purge-dead", 0, "hapus catatan mati lebih tua dari durasi ini saat start (0 = tidak)")
	flagEnv          = flag.String("env", ".env", "berkas .env untuk konfigurasi port/LAN/akun (opsional)")
	flagTLSProbe     = flag.Bool("tls", true, "uji CONNECT 443 + TLS handshake saat verifikasi (subcommand check/scrape)")
	flagAltProbe     = flag.Bool("alt", true, "uji tunnel ke port non-standar 8443/2053 (subcommand check/scrape)")
)

// Nilai dari .env; env selalu menang atas default flag, tapi nilainya bisa juga dipakai
// untuk menimpa pengaturan yang tersimpan (lihat applyEnvToSettings).
var (
	envValues = map[string]string{}
	effAddr   = ""
	effDB     = ""
	effConfig = ""
)

// initEnv membaca .env dan menghitung alamat listen, path database, dan path pengaturan.
//
//	PS_ADDR           alamat lengkap, mis. "0.0.0.0:9000" (menang atas semuanya)
//	PS_BIND_LAN       true/false — false berarti hanya 127.0.0.1
//	PS_HOST           host listen (default 0.0.0.0 kalau PS_BIND_LAN true)
//	PS_PORT           port listen (default 8787)
//	PS_DB, PS_CONFIG  path database & settings.json
func initEnv() {
	envValues, _ = envfile.Load(*flagEnv)
	effDB = envfile.Get(envValues, "PS_DB", *flagDB)
	effConfig = envfile.Get(envValues, "PS_CONFIG", configPathFromFlag())

	host := envfile.Get(envValues, "PS_HOST", "")
	if host == "" {
		if envfile.GetBool(envValues, "PS_BIND_LAN", true) {
			host = "0.0.0.0"
		} else {
			host = "127.0.0.1"
		}
	}
	port := envfile.GetInt(envValues, "PS_PORT", 8787)
	effAddr = envfile.Get(envValues, "PS_ADDR", net.JoinHostPort(host, strconv.Itoa(port)))
}

// applyEnvToSettings menimpa pengaturan tersimpan dengan nilai dari .env (kalau diisi).
func applyEnvToSettings(cfg *settings.Settings) {
	if v := envfile.Get(envValues, "PS_ADMIN_USER", ""); v != "" {
		cfg.AdminUser = v
	}
	if v := envfile.Get(envValues, "PS_ADMIN_PASSWORD", ""); v != "" {
		cfg.AdminPasswordHash = settings.HashPassword(v)
	}
	if v := envfile.Get(envValues, "PS_API_TOKEN", ""); v != "" {
		cfg.Token = v
	}
	if v := envfile.Get(envValues, "PS_PUBLIC_URL", ""); v != "" {
		cfg.PublicURL = v
	}
	if v := envfile.Get(envValues, "PS_TUNNEL_NAME", ""); v != "" {
		cfg.TunnelName = v
	}
	if v := envfile.Get(envValues, "PS_TUNNEL_DOMAIN", ""); v != "" {
		cfg.TunnelDomain = v
	}
	cfg.Auto = envfile.GetBool(envValues, "PS_AUTO", cfg.Auto)
	cfg.IntervalMinutes = envfile.GetInt(envValues, "PS_INTERVAL_MINUTES", cfg.IntervalMinutes)
	cfg.Cap = envfile.GetInt(envValues, "PS_CAP", cfg.Cap)
	cfg.Jobs = envfile.GetInt(envValues, "PS_JOBS", cfg.Jobs)
	cfg.MinSources = envfile.GetInt(envValues, "PS_MIN_SOURCES", cfg.MinSources)
	cfg.FreshMinutes = envfile.GetInt(envValues, "PS_FRESH_MINUTES", cfg.FreshMinutes)
	cfg.Normalize()
}

// version diisi saat build rilis:
//   go build -ldflags "-X main.version=v1.2.3" .
// Dipakai oleh subcommand `version` dan dicatat di log saat serve.
var version = "dev"

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "pemakaian: %s [serve|scrape|sources|check|verify|version] [opsi]\n\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()

	command := "serve"
	args := flag.Args()
	if len(args) > 0 {
		command = args[0]
		// flag.Parse bawaan berhenti di argumen non-flag, jadi `serve -addr X` tidak akan
		// terbaca. Parse ulang sisa argumen supaya flag boleh ditulis setelah subcommand.
		if err := flag.CommandLine.Parse(args[1:]); err != nil {
			log.Fatalf("parse flag: %v", err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	initEnv()
	if len(envValues) > 0 {
		log.Printf("konfigurasi .env dibaca: %d nilai (listen %s)", len(envValues), effAddr)
	}

	switch command {
	case "version":
		fmt.Println("pan-proxy-scanner " + version)
	case "sources":
		if err := runSources(ctx); err != nil {
			log.Fatalf("sources: %v", err)
		}
	case "check":
		if err := runCheck(ctx); err != nil {
			log.Fatalf("check: %v", err)
		}
	case "verify":
		if err := runVerify(ctx); err != nil {
			log.Fatalf("verify: %v", err)
		}
	case "scrape":
		if err := runScrape(ctx); err != nil {
			log.Fatalf("scrape: %v", err)
		}
	case "serve":
		if err := runServe(ctx); err != nil {
			log.Fatalf("serve: %v", err)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
}

func configPathFromFlag() string {
	if *flagConfig != "" {
		return *flagConfig
	}
	return filepath.Join(settings.ConfigDir(), "settings.json")
}

func newStore() (*store.Store, error) {
	st, err := store.Open(effDB)
	if err != nil {
		return nil, fmt.Errorf("buka db %s: %w", effDB, err)
	}
	return st, nil
}

// seedSources mengisi tabel sources dengan katalog bawaan (sekali; sumber yang sudah
// ditambah/diubah pengguna tidak disentuh).
func seedSources(st *store.Store) error {
	seeds := make([]store.SourceSeed, 0, len(sources.All))
	for _, src := range sources.All {
		seeds = append(seeds, store.SourceSeed{Name: src.Name, URL: src.URL, Kind: src.Kind, Optional: src.Optional})
	}
	return st.EnsureSources(seeds)
}

// loadSettings membaca settings.json; kalau belum ada, dibuat dari nilai default + flag,
// lalu ditimpa nilai apa pun yang ada di .env.
func loadSettings() (settings.Settings, string, error) {
	path := effConfig
	cfg, err := settings.Load(path)
	if err != nil {
		return cfg, path, err
	}
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		if *flagInterval > 0 {
			cfg.IntervalMinutes = int(flagInterval.Minutes())
		}
		cfg.Cap = *flagCap
		cfg.Jobs = *flagJobs
		cfg.MinSources = *flagMinSources
		cfg.RefreshLimit = *flagRefreshLimit
		cfg.RefreshAfterMinutes = int(flagRefreshAfter.Minutes())
		cfg.FreshMinutes = int(flagFresh.Minutes())
		cfg.Auto = *flagInterval > 0
		if err := cfg.Save(path); err != nil {
			return cfg, path, err
		}
		log.Printf("pengaturan awal dibuat: %s", path)
	}
	applyEnvToSettings(&cfg)
	return cfg, path, nil
}

func checkerOptions() checker.Options {
	return checker.Options{DialTimeout: *flagDialTimeout, StageTimeout: *flagStageTimeout, ProbeTimeout: *flagProbeTimeout}
}

// runServe menjalankan website + API dan penjadwal siklus otomatis.
func runServe(ctx context.Context) error {
	st, err := newStore()
	if err != nil {
		return err
	}
	defer st.Close()

	if err := seedSources(st); err != nil {
		log.Printf("gagal menyiapkan katalog sumber: %v", err)
	}

	cfg, cfgPath, err := loadSettings()
	if err != nil {
		return err
	}

	if *flagPurgeDead > 0 {
		if n, err := st.DeleteDead(*flagPurgeDead); err == nil && n > 0 {
			log.Printf("housekeeping: %d catatan mati dihapus", n)
		}
		if n, err := st.DeleteHopeless(2, 5000); err == nil && n > 0 {
			log.Printf("housekeeping: %d kandidat gagal berulang dihapus", n)
		}
	}

	runner := scraper.NewRunner(st)
	sched := scraper.NewScheduler(runner, cfg)
	apiServer := api.New(st, runner, sched, &cfg, cfgPath, effAddr)
	apiServer.SetEnvPath(*flagEnv)
	// Setiap siklus selesai: dorong hasil ke Supabase dan/atau verifikasi baris lama.
	runner.OnFinish = func(scraper.Progress, []scraper.SourceStat) { apiServer.AfterCycle() }
	// Jadwal (interval/harian) menjalankan pipeline penuh kalau PipelineEnabled aktif.
	sched.Pipeline = apiServer.RunPipeline
	srv := &http.Server{Addr: effAddr, Handler: apiServer.Handler()}

	if cfg.Auto {
		if err := sched.Start(); err != nil {
			log.Printf("gagal menyalakan siklus otomatis: %v", err)
		} else {
			log.Printf("siklus otomatis aktif setiap %d menit (cap %d, jobs %d)", cfg.IntervalMinutes, cfg.Cap, cfg.Jobs)
		}
	} else if *flagScrapeNow {
		if err := sched.TriggerNow(); err != nil {
			log.Printf("siklus awal dilewati: %v", err)
		}
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	host, port, _ := strings.Cut(effAddr, ":")
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	log.Printf("pan-proxy-scanner %s — website + API: http://%s (bind %s)", version, net.JoinHostPort(host, port), effAddr)
	log.Printf("database: %s | pengaturan: %s | sumber: %d", effDB, cfgPath, countSources(st))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// countSources menghitung sumber aktif di database (untuk log startup).
func countSources(st *store.Store) int {
	list, err := st.Sources(true)
	if err != nil {
		return 0
	}
	return len(list)
}

// runScrape menjalankan satu siklus di terminal dengan progres ringkas.
func runScrape(ctx context.Context) error {
	st, err := newStore()
	if err != nil {
		return err
	}
	defer st.Close()

	if err := seedSources(st); err != nil {
		log.Printf("gagal menyiapkan katalog sumber: %v", err)
	}

	cfg, _, err := loadSettings()
	if err != nil {
		return err
	}
	opts := scraper.OptionsFromSettings(cfg)
	opts.Offset = *flagOffset
	opts.FetchTimeout = *flagFetchTimeout
	opts.Checker = checkerOptions()

	sink := &termSink{}
	stats, err := scraper.Run(ctx, st, opts, sink)
	if err != nil {
		return err
	}

	ok, bad, optional := 0, 0, 0
	for _, s := range stats {
		switch {
		case s.Error == "" && s.Count > 0:
			ok++
		case s.Optional:
			optional++
		default:
			bad++
		}
	}
	log.Printf("sumber: %d OK, %d gagal, %d opsional gagal", ok, bad, optional)

	summary, err := st.Stats(cfg.Fresh())
	if err != nil {
		return err
	}
	log.Printf("database: %d baris · %d hidup · %d hidup & segar · rata-rata %d ms", summary.Total, summary.Alive, summary.AliveFresh, summary.AvgPingMS)
	return nil
}

// termSink mencetak progres siklus ke terminal (satu baris per perubahan).
type termSink struct {
	mu   sync.Mutex
	p    scraper.Progress
	last string
}

func (t *termSink) Update(fn func(*scraper.Progress)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	fn(&t.p)
	line := fmt.Sprintf("[%s] kandidat %d · dicek %d · hidup %d · mati %d", t.p.Phase, t.p.Candidates, t.p.Checked, t.p.Alive, t.p.Dead)
	if line != t.last {
		log.Print(line)
		t.last = line
	}
}

// checkPool memverifikasi banyak alamat dengan paralelisme `jobs` dan memanggil onResult
// untuk setiap hasil (urutan tidak dijamin).
func checkPool(ctx context.Context, addrs []string, opts checker.Options, jobs int, onResult func(addr string, res checker.Result)) {
	if jobs < 1 {
		jobs = 1
	}
	work := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range work {
				onResult(addr, checker.Check(ctx, addr, opts))
			}
		}()
	}
	for _, addr := range addrs {
		select {
		case <-ctx.Done():
			close(work)
			wg.Wait()
			return
		case work <- addr:
		}
	}
	close(work)
	wg.Wait()
}

// splitHostPort memisah "ip:port" (port wajib angka) dan menormalkan IPv6.
func splitHostPort(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		i := strings.LastIndex(addr, ":")
		if i <= 0 {
			return "", 0, fmt.Errorf("alamat tidak valid: %s", addr)
		}
		host, portStr = addr[:i], addr[i+1:]
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("port tidak valid: %s", addr)
	}
	return host, port, nil
}

// checkOptions menyusun opsi checker dari flag CLI.
func checkOptions() checker.Options {
	return checker.Options{
		DialTimeout:  *flagDialTimeout,
		StageTimeout: *flagStageTimeout,
		ProbeTimeout: *flagProbeTimeout,
		ProbeTLS:     *flagTLSProbe,
		ProbeAlt:     *flagAltProbe,
	}
}

// formatResult menyusun satu baris ringkas hasil verifikasi untuk terminal.
func formatResult(addr string, res checker.Result) string {
	status := "MATI "
	if res.Alive {
		status = "HIDUP"
	}
	detail := res.Detail
	if res.Alive {
		tlsState, altState := "tls=tidak", "alt=tidak"
		if res.TLS {
			tlsState = "tls=ya"
		}
		if res.AltOK {
			altState = "alt=ya"
		}
		exit := ""
		if res.ExitIP != "" {
			exit = " exit=" + res.ExitIP
		}
		if res.Country != "" {
			exit += " " + res.Country
		}
		detail = fmt.Sprintf("%s %dms (total %dms) %s %s%s | %s", res.Protocol, res.MS, res.TotalMS, tlsState, altState, exit, res.Detail)
	}
	return fmt.Sprintf("%-6s %-24s %s", status, addr, detail)
}

// runCheck menguji satu atau beberapa proxy langsung dari terminal (tanpa database),
// termasuk uji TLS 443. Berguna untuk memastikan logika checker di mesin ini.
func runCheck(ctx context.Context) error {
	addrs := flag.Args() // setelah parse dua tahap, Args berisi alamat saja
	if len(addrs) == 0 {
		return fmt.Errorf("pakai: check [-tls=false] [-jobs N] ip:port [ip:port ...]")
	}
	opts := checkOptions()
	var mu sync.Mutex
	alive := 0
	checkPool(ctx, addrs, opts, *flagJobs, func(addr string, res checker.Result) {
		if res.Alive {
			mu.Lock()
			alive++
			mu.Unlock()
		}
		mu.Lock()
		fmt.Println(formatResult(addr, res))
		mu.Unlock()
	})
	fmt.Printf("\n%d dari %d hidup\n", alive, len(addrs))
	return nil
}

// runVerify memverifikasi alamat yang diberikan lalu MENYIMPAN hasilnya ke database
// (RecordAlive/RecordDead). Dipakai operator untuk memperbarui verdict tertentu tanpa
// menunggu siklus penuh — mis. setelah menambah kandidat manual.
func runVerify(ctx context.Context) error {
	addrs := flag.Args()
	if len(addrs) == 0 {
		return fmt.Errorf("pakai: verify [-tls=false] [-jobs N] ip:port [ip:port ...]")
	}
	st, err := newStore()
	if err != nil {
		return err
	}
	defer st.Close()

	opts := checkOptions()
	var mu sync.Mutex
	alive, dead, withTLS := 0, 0, 0
	var firstErr error
	checkPool(ctx, addrs, opts, *flagJobs, func(addr string, res checker.Result) {
		host, port, splitErr := splitHostPort(addr)
		if splitErr != nil {
			mu.Lock()
			fmt.Printf("LEWATI %-24s %v\n", addr, splitErr)
			mu.Unlock()
			return
		}
		var writeErr error
		if res.Alive {
			writeErr = st.RecordAlive(host, port, res.Protocol, res.Country, res.MS, 0, res.TLS, res.AltOK, res.ExitIP)
		} else {
			writeErr = st.RecordDead(host, port, res.Country, 0)
		}
		mu.Lock()
		defer mu.Unlock()
		if writeErr != nil && firstErr == nil {
			firstErr = writeErr
		}
		if res.Alive {
			alive++
			if res.TLS {
				withTLS++
			}
		} else {
			dead++
		}
		fmt.Println(formatResult(addr, res))
	})
	fmt.Printf("\nverify: %d hidup (%d sanggup TLS 443), %d mati — tersimpan di database\n", alive, withTLS, dead)
	return firstErr
}
func runSources(ctx context.Context) error {
	opts := scraper.DefaultOptions()
	opts.FetchTimeout = *flagFetchTimeout
	// Pakai daftar sumber dari database kalau ada (termasuk yang ditambah lewat website).
	var st *store.Store
	if opened, err := newStore(); err == nil {
		st = opened
		defer st.Close()
		if err := seedSources(st); err != nil {
			log.Printf("gagal menyiapkan katalog sumber: %v", err)
		}
	} else {
		log.Printf("database tidak bisa dibuka (%v) — memakai katalog bawaan", err)
	}
	started := time.Now()
	stats, unique := scraper.CheckSources(ctx, st, opts)
	sort.Slice(stats, func(i, j int) bool { return stats[i].Count > stats[j].Count })

	fmt.Printf("%-26s%-7s%-8s%-9s%-11s%-7s%s\n", "nama", "jenis", "status", "proxy", "ukuran", "ms", "catatan")
	ok, empty, failed, optionalFailed := 0, 0, 0, 0
	for _, s := range stats {
		note := ""
		switch {
		case s.Error == "" && s.Count > 0:
			ok++
		case s.Optional:
			optionalFailed++
			note = "opsional: " + s.Error
		case s.Error == "":
			empty++
			note = "KOSONG"
		default:
			failed++
			note = s.Error
		}
		size := "-"
		if s.Bytes > 0 {
			size = fmt.Sprintf("%.1f KB", float64(s.Bytes)/1024)
		}
		fmt.Printf("%-26s%-7s%-8d%-9d%-11s%-7d%s\n", s.Name, s.Kind, s.Status, s.Count, size, s.MS, note)
	}
	fmt.Printf("\nOK: %d | KOSONG: %d | GAGAL: %d | opsional gagal: %d\n", ok, empty, failed, optionalFailed)
	fmt.Printf("kandidat unik: %d (dalam %s)\n", unique, time.Since(started).Round(time.Millisecond))
	if empty > 0 || failed > 0 {
		return fmt.Errorf("%d sumber bermasalah", empty+failed)
	}
	return nil
}
