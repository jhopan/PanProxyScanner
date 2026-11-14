package api

import (
	"context"
	"encoding/json"
	"github.com/jhopan/Pan-ProxyScanner/internal/scraper"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jhopan/Pan-ProxyScanner/internal/settings"
	"github.com/jhopan/Pan-ProxyScanner/internal/store"
	"github.com/jhopan/Pan-ProxyScanner/internal/supabase"
)

// exitKey dipakai untuk dedup alamat keluar di pool: IPv4 dinormalkan per /24 supaya
// beberapa port dari subnet yang sama tidak memenuhi pool, IPv6 dibiarkan utuh.
func TestExitKey(t *testing.T) {
	cases := map[string]string{
		"103.18.78.157":        "103.18.78.0/24",
		" 103.18.78.9 ":        "103.18.78.0/24",
		"2400:6180:0:d0::e5:1": "2400:6180:0:d0::e5:1",
		"":                     "",
		"bukan-ip":             "",
	}
	for in, want := range cases {
		if got := exitKey(in); got != want {
			t.Fatalf("exitKey(%q) = %q, mau %q", in, got, want)
		}
	}
	if exitKey("103.18.78.157") != exitKey("103.18.78.200") {
		t.Fatal("dua alamat di /24 yang sama harus menghasilkan kunci yang sama")
	}
	if exitKey("103.18.78.157") == exitKey("103.18.79.1") {
		t.Fatal("/24 berbeda tidak boleh dianggap duplikat")
	}
}

// pushFreshMinutes membatasi umur verdict yang boleh dipakai saat push (≤ 90 menit),
// karena fresh_minutes bawaan (360) terlalu longgar untuk panel.
func TestPushFreshMinutes(t *testing.T) {
	cases := []struct{ in, want int }{
		{in: 360, want: 90},
		{in: 0, want: 90},
		{in: 45, want: 45},
		{in: 90, want: 90},
		{in: 120, want: 90},
	}
	for _, c := range cases {
		got := pushFreshMinutes(settings.Settings{FreshMinutes: c.in})
		if got != c.want {
			t.Fatalf("fresh_minutes=%d → %d, mau %d", c.in, got, c.want)
		}
	}
}

// Push delta: dengan penanda , hanya baris yang dicek setelah titik itu yang dikirim.
// Stub HTTP di sini menggantikan Supabase, jadi tes tidak menyentuh jaringan nyata.
func TestPushSinceSendsOnlyChanged(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "p.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	// Dua kali berhasil per baris: pool menahan proxy yang baru sekali lolos (minOK=2).
	for i, addr := range []string{"1.1.1.1", "8.8.8.8", "9.9.9.9"} {
		exit := []string{"203.0.113.1", "203.0.114.1", "203.0.115.1"}[i]
		for n := 0; n < 2; n++ {
			if err := st.RecordAlive(addr, 1080, "socks5", "ID", 900, 2, true, false, exit); err != nil {
				t.Fatal(err)
			}
		}
	}

	var mu sync.Mutex
	rowsSeen := []int{}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var rows []map[string]any
		_ = json.Unmarshal(body, &rows)
		mu.Lock()
		rowsSeen = append(rowsSeen, len(rows))
		mu.Unlock()
		w.Header().Set("Content-Range", "0-0/0")
		w.WriteHeader(http.StatusCreated)
	}))
	defer ts.Close()

	client := supabase.New(ts.URL, "kunci-uji", "proxy_pool")
	srv := &Server{store: st}
	cfg := settings.Default()

	// Penanda di masa depan: belum ada baris yang berubah, jadi tidak ada yang dikirim.
	sentNew, _, err := srv.pushToSupabase(context.Background(), client, cfg, 100, nil, time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatalf("push delta tanpa perubahan bukan kegagalan: %v", err)
	}
	if sentNew != 0 {
		t.Fatalf("penanda masa depan harus mengirim 0 baris, dapat %d", sentNew)
	}

	// Penanda di masa lalu: ketiga baris ikut terkirim.
	sentOld, _, err := srv.pushToSupabase(context.Background(), client, cfg, 100, nil, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if sentOld == 0 {
		t.Fatalf("penanda masa lalu harus mengirim baris, dapat %d", sentOld)
	}

	mu.Lock()
	defer mu.Unlock()
	total := 0
	for _, n := range rowsSeen {
		total += n
	}
	if total != 3 {
		t.Fatalf("payload ke Supabase harus berisi 3 baris, dapat %d", total)
	}
	if _, err := st.Meta("last_push_at"); err != nil {
		t.Fatalf("penanda push harus tersimpan: %v", err)
	}
}

// pushTestStore membuka database sementara berisi satu proxy hidup yang sudah dua kali
// berhasil (pool menahan proxy yang baru sekali lolos, minOK=2 di pushToSupabase).
func pushTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "p.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for range 2 {
		if err := st.RecordAlive("1.1.1.1", 1080, "socks5", "ID", 900, 2, true, false, "203.0.113.1"); err != nil {
			t.Fatal(err)
		}
	}
	return st
}

// hostEvidenceKeys adalah kolom bukti vantage host scanner yang harus ikut dikirim push.
var hostEvidenceKeys = []string{"tunnel_host_ok", "tunnel_host_ms", "tunnel_host_checked"}

// edgeOnlyKeys adalah kolom verdict edge (diisi cron panel CFVPN): scanner hanya membacanya
// (PullEdgeVerdicts), jadi tidak boleh pernah muncul di payload push.
var edgeOnlyKeys = []string{"tunnel_ok", "tunnel_ms", "tunnel_checked"}

// Payload push harus memuat bukti vantage host: tunnel nyata lewat proxy itu sudah lulus
// dari mesin scanner, jadi baris yang naik membawa ok/ms/waktu cek — penanda bukti untuk
// baris non-TLS yang tidak punya tls_ok/alt_ok.
func TestPushSendsHostTunnelEvidence(t *testing.T) {
	st := pushTestStore(t)

	var mu sync.Mutex
	var payload []map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var rows []map[string]any
		_ = json.Unmarshal(body, &rows)
		mu.Lock()
		payload = append(payload, rows...)
		mu.Unlock()
		w.Header().Set("Content-Range", "0-0/0")
		w.WriteHeader(http.StatusCreated)
	}))
	defer ts.Close()

	client := supabase.New(ts.URL, "kunci-uji", "proxy_pool")
	srv := &Server{store: st}
	sent, _, err := srv.pushToSupabase(context.Background(), client, settings.Default(), 100, nil, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("push ke stub harus sukses: %v", err)
	}
	if sent == 0 {
		t.Fatal("push harus mengirim baris")
	}
	if !client.HostEvidenceColumns() {
		t.Fatal("stub menerima kolom tunnel_host_*: HostEvidenceColumns harus true")
	}

	mu.Lock()
	recorded := append([]map[string]any(nil), payload...)
	mu.Unlock()
	if len(recorded) == 0 {
		t.Fatal("tidak ada payload tercatat")
	}
	for _, row := range recorded {
		ok, isBool := row["tunnel_host_ok"].(bool)
		if !isBool || !ok {
			t.Fatalf("tunnel_host_ok harus true (tunnel nyata lulus dari host scanner), dapat %v", row["tunnel_host_ok"])
		}
		ms, isNum := row["tunnel_host_ms"].(float64)
		if !isNum || ms <= 0 {
			t.Fatalf("tunnel_host_ms harus latensi milidetik, dapat %v", row["tunnel_host_ms"])
		}
		checked, isStr := row["tunnel_host_checked"].(string)
		if !isStr {
			t.Fatalf("tunnel_host_checked harus ada, dapat %v", row["tunnel_host_checked"])
		}
		if _, err := time.Parse(time.RFC3339, checked); err != nil {
			t.Fatalf("tunnel_host_checked harus RFC3339, dapat %q: %v", checked, err)
		}
		for _, key := range edgeOnlyKeys {
			if _, ok := row[key]; ok {
				t.Fatalf("%s milik edge (cron panel) — scanner tidak boleh menulisnya", key)
			}
		}
	}
}

// Kalau tabel belum punya kolom tunnel_host_* (SQL panel belum dijalankan), PostgREST
// menolak payload dengan 400 + PGRST204 atau SQLSTATE 42703: push harus tetap berhasil tanpa
// kolom itu, statusnya diingat, dan kolom bukti lama (tls_ok/alt_ok/exit_ip) tidak ikut hilang.
func TestPushToleratesMissingHostEvidenceColumns(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{
			name: "PGRST204 kolom tidak ada di schema cache",
			body: `{"code":"PGRST204","message":"Could not find the 'tunnel_host_ok' column of 'proxy_pool' in the schema cache"}`,
		},
		{
			name: "SQLSTATE 42703 undefined column",
			body: `{"code":"42703","message":"column \"tunnel_host_ok\" does not exist","details":null}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := pushTestStore(t)

			var mu sync.Mutex
			var accepted []map[string]any
			rejected := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				var rows []map[string]any
				_ = json.Unmarshal(body, &rows)
				offending := false
				for _, row := range rows {
					for _, key := range hostEvidenceKeys {
						if _, ok := row[key]; ok {
							offending = true
						}
					}
				}
				mu.Lock()
				if offending {
					rejected++
				} else {
					accepted = append(accepted, rows...)
				}
				mu.Unlock()
				if offending {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte(tc.body))
					return
				}
				w.Header().Set("Content-Range", "0-0/0")
				w.WriteHeader(http.StatusCreated)
			}))
			defer ts.Close()

			client := supabase.New(ts.URL, "kunci-uji", "proxy_pool")
			srv := &Server{store: st}
			sent, _, err := srv.pushToSupabase(context.Background(), client, settings.Default(), 100, nil, time.Now().Add(-24*time.Hour))
			if err != nil {
				t.Fatalf("penolakan kolom tunnel_host_* bukan kegagalan push: %v", err)
			}
			if sent == 0 {
				t.Fatal("push harus tetap mengirim baris (tanpa kolom tunnel_host_*)")
			}
			if client.HostEvidenceColumns() {
				t.Fatal("kolom tunnel_host_* pernah ditolak: statusnya harus diingat sebagai tidak ada")
			}
			mu.Lock()
			defer mu.Unlock()
			if rejected == 0 {
				t.Fatal("stub harus menolak percobaan pertama yang memuat kolom tunnel_host_*")
			}
			if len(accepted) == 0 {
				t.Fatal("push ulang tanpa kolom tunnel_host_* harus sampai ke tabel")
			}
			for _, row := range accepted {
				for _, key := range hostEvidenceKeys {
					if _, ok := row[key]; ok {
						t.Fatalf("payload yang diterima tidak boleh memuat %s", key)
					}
				}
				if _, ok := row["tls_ok"]; !ok {
					t.Fatal("kolom bukti tls_ok harus tetap terkirim walau tunnel_host_* ditolak")
				}
			}
		})
	}
}

// Kegagalan yang BUKAN soal kolom (5xx, atau 400 dengan kode lain) tidak boleh membuat kolom
// bukti dilepas dan state-nya diingat: satu gangguan sesaat jangan sampai mematikan kolom
// tunnel_host_* / tls_ok sampai proses scanner restart. Push berikutnya ke stub yang sama
// harus tetap mengirim kolom host.
func TestPushKeepsEvidenceColumnsOnNonColumnError(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{name: "500 internal error", status: http.StatusInternalServerError, body: `{"code":"XX000","message":"internal error"}`},
		{name: "400 kode lain (PGRST102)", status: http.StatusBadRequest, body: `{"code":"PGRST102","message":"All object keys must match"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := pushTestStore(t)

			var mu sync.Mutex
			fail := true
			posts, hostSeen := 0, 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				var rows []map[string]any
				_ = json.Unmarshal(body, &rows)
				if r.Method != http.MethodPost {
					w.Header().Set("Content-Range", "0-0/0")
					w.WriteHeader(http.StatusOK)
					return
				}
				mu.Lock()
				failing := fail
				posts++
				if !failing {
					for _, row := range rows {
						if _, ok := row["tunnel_host_ok"]; ok {
							hostSeen++
						}
					}
				}
				mu.Unlock()
				if failing {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
					return
				}
				w.Header().Set("Content-Range", "0-0/0")
				w.WriteHeader(http.StatusCreated)
			}))
			defer ts.Close()

			client := supabase.New(ts.URL, "kunci-uji", "proxy_pool")
			srv := &Server{store: st}
			if _, _, err := srv.pushToSupabase(context.Background(), client, settings.Default(), 100, nil, time.Now().Add(-24*time.Hour)); err == nil {
				t.Fatalf("stub membalas %d: push harus melaporkan error", tc.status)
			}
			mu.Lock()
			tries := posts
			mu.Unlock()
			if tries != 1 {
				t.Fatalf("error non-kolom tidak boleh memicu push ulang, dapat %d percobaan", tries)
			}

			mu.Lock()
			fail = false
			mu.Unlock()
			sent, _, err := srv.pushToSupabase(context.Background(), client, settings.Default(), 100, nil, time.Now().Add(-24*time.Hour))
			if err != nil {
				t.Fatalf("push berikutnya harus sukses: %v", err)
			}
			if sent == 0 {
				t.Fatal("push berikutnya harus mengirim baris")
			}
			mu.Lock()
			seen := hostSeen
			mu.Unlock()
			if seen == 0 {
				t.Fatal("setelah error non-kolom, push berikutnya harus tetap mengirim tunnel_host_ok")
			}
			if !client.HostEvidenceColumns() {
				t.Fatal("kolom tunnel_host_* masih dipakai: HostEvidenceColumns harus true")
			}
		})
	}
}

// Pipeline sinkron dipakai penjadwal: hasilnya harus mencerminkan langkah yang benar-benar jalan
// (cek lokal saat testOnly) supaya penanda scrape harian tidak ditulis untuk run yang gagal.
func TestRunPipelineSyncRunsLocalCheck(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pipe1.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := settings.Default()
	srv := &Server{store: st, runner: scraper.NewRunner(st), settings: &cfg}
	if err := srv.RunPipelineSync(true); err != nil {
		t.Fatalf("cek lokal tanpa Supabase harus sukses: %v", err)
	}
	srv.pipelineMu.Lock()
	steps := append([]PipelineStep(nil), srv.pipeline.Steps...)
	running := srv.pipeline.Running
	srv.pipelineMu.Unlock()
	if running {
		t.Fatal("status harus berhenti berjalan setelah pipeline sinkron selesai")
	}
	var local, pull bool
	for _, step := range steps {
		switch {
		case strings.HasPrefix(step.Name, "cek lokal"):
			local = step.OK
		case strings.HasPrefix(step.Name, "tarik & verifikasi"):
			pull = strings.Contains(step.Note, "dilewati")
		}
	}
	if !local {
		t.Fatalf("langkah cek lokal harus tercatat sukses, dapat %+v", steps)
	}
	if !pull {
		t.Fatalf("mode tes tidak boleh menarik dari Supabase, dapat %+v", steps)
	}
}

// Langkah yang gagal harus muncul sebagai error dari pipeline sinkron (dipakai penjadwal untuk
// memutuskan perlu percobaan ulang, bukan menulis penanda harian).
func TestRunPipelineSyncReportsFailedStep(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pipe2.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	stub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"stub gagal"}`))
	}))
	defer stub.Close()
	cfg := settings.Default()
	cfg.SupabaseURL = stub.URL
	cfg.SupabaseKey = "kunci-uji"
	cfg.PipelinePushLimit = 10
	srv := &Server{store: st, runner: scraper.NewRunner(st), settings: &cfg}
	err = srv.RunPipelineSync(true)
	if err == nil {
		t.Fatal("push yang gagal harus membuat pipeline sinkron mengembalikan error")
	}
	if !strings.Contains(err.Error(), "push ke Supabase") {
		t.Fatalf("error harus menyebut langkah gagal, dapat %q", err.Error())
	}
	srv.pipelineMu.Lock()
	statusErr := srv.pipeline.Error
	srv.pipelineMu.Unlock()
	if statusErr == "" {
		t.Fatal("status pipeline harus menyimpan pesan error")
	}
}

// Prune tanpa service key bukan kegagalan 5xx: balas deleted 0 + alasan, dan jangan
// menembak Supabase sama sekali — anon key sudah tidak boleh DELETE, jadi request apa pun
// hanya menambah latensi dan error.
func TestPruneWithoutServiceKeyExplainsSkip(t *testing.T) {
	var mu sync.Mutex
	var methods []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		methods = append(methods, r.Method)
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	cfg := settings.Default()
	cfg.SupabaseURL = ts.URL
	cfg.SupabaseKey = "kunci-anon"
	srv := &Server{settings: &cfg}
	rec := httptest.NewRecorder()
	srv.handleSupabasePrune(rec, httptest.NewRequest(http.MethodPost, "/api/supabase/prune?days=7&limit=10", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("prune tanpa service key harus 200, dapat %d: %s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Deleted int    `json:"deleted"`
		Note    string `json:"note"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("balasan prune bukan JSON: %v", err)
	}
	if payload.Deleted != 0 {
		t.Fatalf("tanpa service key deleted harus 0, dapat %d", payload.Deleted)
	}
	if !strings.Contains(payload.Note, "supabase_service_key") {
		t.Fatalf("note harus menyebut supabase_service_key, dapat %q", payload.Note)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(methods) != 0 {
		t.Fatalf("tanpa service key tidak boleh ada request ke Supabase, dapat %v", methods)
	}
}

// Prune dengan service key: hitung kandidat (HEAD) tetap memakai anon key, sedangkan
// operasi hapus (GET id + DELETE) harus membawa `Authorization: Bearer <service key>` —
// itulah satu-satunya yang diizinkan panel.
func TestPruneUsesServiceKeyForDelete(t *testing.T) {
	var mu sync.Mutex
	var deleteAuth, headAuth, listAuth []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			mu.Lock()
			headAuth = append(headAuth, r.Header.Get("Authorization"))
			mu.Unlock()
			w.Header().Set("Content-Range", "0-0/1")
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			mu.Lock()
			listAuth = append(listAuth, r.Header.Get("Authorization"))
			mu.Unlock()
			_, _ = w.Write([]byte(`[{"id":"baris-1"}]`))
		case http.MethodDelete:
			mu.Lock()
			deleteAuth = append(deleteAuth, r.Header.Get("Authorization"))
			mu.Unlock()
			_, _ = w.Write([]byte(`[{"id":"baris-1"}]`))
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer ts.Close()

	cfg := settings.Default()
	cfg.SupabaseURL = ts.URL
	cfg.SupabaseKey = "kunci-anon"
	cfg.SupabaseServiceKey = "kunci-service"
	srv := &Server{settings: &cfg}
	rec := httptest.NewRecorder()
	srv.handleSupabasePrune(rec, httptest.NewRequest(http.MethodPost, "/api/supabase/prune?days=7&limit=10", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("prune harus 200, dapat %d: %s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Deleted int `json:"deleted"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("balasan prune bukan JSON: %v", err)
	}
	if payload.Deleted != 1 {
		t.Fatalf("stub menghapus 1 baris, deleted harus 1, dapat %d", payload.Deleted)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(deleteAuth) != 1 || deleteAuth[0] != "Bearer kunci-service" {
		t.Fatalf("DELETE harus memakai service key, dapat %v", deleteAuth)
	}
	if len(listAuth) != 1 || listAuth[0] != "Bearer kunci-service" {
		t.Fatalf("ambil id yang akan dihapus harus memakai service key, dapat %v", listAuth)
	}
	if len(headAuth) == 0 || headAuth[0] != "Bearer kunci-anon" {
		t.Fatalf("hitung kandidat tetap memakai anon key, dapat %v", headAuth)
	}
}
