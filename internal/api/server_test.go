package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jhopan/Pan-ProxyScanner/internal/scraper"
	"io"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jhopan/Pan-ProxyScanner/internal/auth"
	"github.com/jhopan/Pan-ProxyScanner/internal/notify"
	"github.com/jhopan/Pan-ProxyScanner/internal/settings"
	"github.com/jhopan/Pan-ProxyScanner/internal/store"
	"github.com/jhopan/Pan-ProxyScanner/internal/supabase"
)

// testClients membungkus satu klien stub sebagai pasangan baca/tulis untuk memanggil
// pushToSupabase/pullVerify langsung dari tes (tanpa service key, jadi tanpa catatan fallback).
func testClients(client *supabase.Client) supabaseClients {
	return supabaseClients{read: client, write: client}
}

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
	sentNew, _, err := srv.pushToSupabase(context.Background(), testClients(client), cfg, 100, nil, time.Now().Add(24*time.Hour))
	if err != nil {
		t.Fatalf("push delta tanpa perubahan bukan kegagalan: %v", err)
	}
	if sentNew != 0 {
		t.Fatalf("penanda masa depan harus mengirim 0 baris, dapat %d", sentNew)
	}

	// Penanda di masa lalu: ketiga baris ikut terkirim.
	sentOld, _, err := srv.pushToSupabase(context.Background(), testClients(client), cfg, 100, nil, time.Now().Add(-24*time.Hour))
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
	sent, _, err := srv.pushToSupabase(context.Background(), testClients(client), settings.Default(), 100, nil, time.Now().Add(-24*time.Hour))
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
			sent, _, err := srv.pushToSupabase(context.Background(), testClients(client), settings.Default(), 100, nil, time.Now().Add(-24*time.Hour))
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
			if _, _, err := srv.pushToSupabase(context.Background(), testClients(client), settings.Default(), 100, nil, time.Now().Add(-24*time.Hour)); err == nil {
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
			sent, _, err := srv.pushToSupabase(context.Background(), testClients(client), settings.Default(), 100, nil, time.Now().Add(-24*time.Hour))
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

// purgeStub menggantikan Supabase untuk uji POST /api/pool-purge: setiap request dicatat
// supaya tes bisa memastikan anon key hanya dipakai untuk whoami dan service key hanya untuk
// DELETE. Tidak ada jaringan nyata yang disentuh.
type purgeStub struct {
	mu           sync.Mutex
	requests     []purgeCall
	whoamiStatus int
	whoamiBody   string
	deleteStatus int
	deleteRange  string
}

type purgeCall struct {
	method string
	path   string
	query  string
	auth   string
	apikey string
}

func (st *purgeStub) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		st.mu.Lock()
		st.requests = append(st.requests, purgeCall{
			method: r.Method, path: r.URL.Path, query: r.URL.RawQuery,
			auth: r.Header.Get("Authorization"), apikey: r.Header.Get("apikey"),
		})
		st.mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/rest/v1/rpc/whoami":
			status := st.whoamiStatus
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			_, _ = io.WriteString(w, st.whoamiBody)
		case r.Method == http.MethodDelete:
			status := st.deleteStatus
			if status == 0 {
				status = http.StatusNoContent
			}
			if st.deleteRange != "" {
				w.Header().Set("Content-Range", st.deleteRange)
			}
			w.WriteHeader(status)
		default:
			w.WriteHeader(http.StatusNotImplemented)
		}
	}
}

// calls mengembalikan salinan request yang sudah diterima stub.
func (st *purgeStub) calls() []purgeCall {
	st.mu.Lock()
	defer st.mu.Unlock()
	return append([]purgeCall(nil), st.requests...)
}

// deletes hanya mengembalikan request DELETE (operasi yang mengubah data).
func (st *purgeStub) deletes() []purgeCall {
	var out []purgeCall
	for _, call := range st.calls() {
		if call.method == http.MethodDelete {
			out = append(out, call)
		}
	}
	return out
}

// purgeTestServer menyiapkan Server dengan stub Supabase sebagai `supabase_url`.
func purgeTestServer(t *testing.T, stub *purgeStub, serviceKey string) *Server {
	t.Helper()
	ts := httptest.NewServer(stub.handler())
	t.Cleanup(ts.Close)
	cfg := settings.Default()
	cfg.SupabaseURL = ts.URL
	cfg.SupabaseKey = "kunci-anon"
	cfg.SupabaseServiceKey = serviceKey
	return &Server{settings: &cfg}
}

// doPoolPurge menjalankan satu POST /api/pool-purge langsung ke handler (tanpa router).
func doPoolPurge(srv *Server, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	srv.handlePoolPurge(rec, httptest.NewRequest(http.MethodPost, "/api/pool-purge", strings.NewReader(body)))
	return rec
}

// Tanpa service key endpoint tidak boleh menyentuh Supabase sama sekali: anon key tidak bisa
// DELETE, jadi request apa pun hanya menambah latensi dan kesempatan fail-open.
func TestPoolPurgeWithoutServiceKeyIs503(t *testing.T) {
	stub := &purgeStub{}
	srv := purgeTestServer(t, stub, "")

	rec := doPoolPurge(srv, `{"username":"budi","hash":"hash-sesi"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("tanpa service key harus 503, dapat %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "supabase_service_key") {
		t.Fatalf("pesan 503 harus menyebut supabase_service_key, dapat %s", rec.Body.String())
	}
	if calls := stub.calls(); len(calls) != 0 {
		t.Fatalf("tanpa service key tidak boleh ada request ke Supabase, dapat %v", calls)
	}
}

// Hash salah (whoami membalas string kosong) = 401 dan tidak pernah ada DELETE.
func TestPoolPurgeWrongHashIs401(t *testing.T) {
	stub := &purgeStub{whoamiBody: `""`}
	srv := purgeTestServer(t, stub, "kunci-service")

	rec := doPoolPurge(srv, `{"username":"budi","hash":"hash-salah"}`)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("hash salah harus 401, dapat %d: %s", rec.Code, rec.Body.String())
	}
	if deletes := stub.deletes(); len(deletes) != 0 {
		t.Fatalf("hash salah tidak boleh menghapus apa pun, dapat %v", deletes)
	}
}

// Role selain admin = 403; DELETE tetap tidak dipanggil.
func TestPoolPurgeNonAdminIs403(t *testing.T) {
	stub := &purgeStub{whoamiBody: `"user"`}
	srv := purgeTestServer(t, stub, "kunci-service")

	rec := doPoolPurge(srv, `{"username":"budi","hash":"hash-sesi"}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("role non-admin harus 403, dapat %d: %s", rec.Code, rec.Body.String())
	}
	if deletes := stub.deletes(); len(deletes) != 0 {
		t.Fatalf("non-admin tidak boleh menghapus apa pun, dapat %v", deletes)
	}
}

// RPC whoami belum ada (404/PGRST202) = 409 yang menyebut migrasi SQL, bukan 401 dan bukan
// fail-open ke anon key.
func TestPoolPurgeMissingWhoamiIs409(t *testing.T) {
	stub := &purgeStub{
		whoamiStatus: http.StatusNotFound,
		whoamiBody:   `{"code":"PGRST202","message":"Could not find the function public.whoami in the schema cache"}`,
	}
	srv := purgeTestServer(t, stub, "kunci-service")

	rec := doPoolPurge(srv, `{"username":"budi","hash":"hash-sesi"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("whoami belum ada harus 409, dapat %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "2026-11-14-login-lockout.sql") {
		t.Fatalf("pesan 409 harus menyebut migrasi yang harus dijalankan, dapat %s", rec.Body.String())
	}
	if deletes := stub.deletes(); len(deletes) != 0 {
		t.Fatalf("tanpa whoami tidak boleh menghapus apa pun, dapat %v", deletes)
	}
}

// Admin: whoami memakai anon key, DELETE memakai service key, dan deleted dibaca dari
// Content-Range (`*/N`).
func TestPoolPurgeDeletesWithServiceKey(t *testing.T) {
	stub := &purgeStub{whoamiBody: `"admin"`, deleteStatus: http.StatusOK, deleteRange: "*/3"}
	srv := purgeTestServer(t, stub, "kunci-service")

	rec := doPoolPurge(srv, `{"username":" Budi ","hash":"hash-sesi"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("purge admin harus 200, dapat %d: %s", rec.Code, rec.Body.String())
	}
	var payload struct {
		Success bool `json:"success"`
		Deleted int  `json:"deleted"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("balasan purge bukan JSON: %v", err)
	}
	if !payload.Success || payload.Deleted != 3 {
		t.Fatalf("balasan harus success true + deleted 3, dapat %+v", payload)
	}

	deletes := stub.deletes()
	if len(deletes) != 1 {
		t.Fatalf("harus ada tepat satu DELETE, dapat %v", deletes)
	}
	if deletes[0].auth != "Bearer kunci-service" || deletes[0].apikey != "kunci-service" {
		t.Fatalf("DELETE harus memakai service key, dapat %+v", deletes[0])
	}
	if deletes[0].query != "is_active=eq.false" {
		t.Fatalf("DELETE harus memfilter is_active=eq.false, dapat %q", deletes[0].query)
	}

	var whoamiCalls int
	for _, call := range stub.calls() {
		if call.path != "/rest/v1/rpc/whoami" {
			continue
		}
		whoamiCalls++
		if call.auth != "Bearer kunci-anon" || call.apikey != "kunci-anon" {
			t.Fatalf("whoami harus memakai anon key, dapat %+v", call)
		}
	}
	if whoamiCalls != 1 {
		t.Fatalf("harus ada tepat satu panggilan whoami, dapat %d", whoamiCalls)
	}
}

// Limiter per IP: jatah habis = 429 + Retry-After, bukan percobaan hash tanpa batas.
func TestPoolPurgeRateLimited(t *testing.T) {
	stub := &purgeStub{whoamiBody: `"admin"`, deleteStatus: http.StatusOK, deleteRange: "*/0"}
	srv := purgeTestServer(t, stub, "kunci-service")

	for i := range poolPurgeRateLimit {
		if rec := doPoolPurge(srv, `{"username":"budi","hash":"hash-sesi"}`); rec.Code != http.StatusOK {
			t.Fatalf("permintaan ke-%d harus 200, dapat %d: %s", i+1, rec.Code, rec.Body.String())
		}
	}
	rec := doPoolPurge(srv, `{"username":"budi","hash":"hash-sesi"}`)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("permintaan berlebih harus 429, dapat %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("429 harus menyertakan Retry-After")
	}
}

// Rute /api/pool-purge harus publik di router (panel tidak punya sesi/token scanner) dan
// preflight CORS-nya dilayani, karena panel memanggilnya lintas origin dari Pages.
func TestPoolPurgeRouteIsPublicWithCORS(t *testing.T) {
	stub := &purgeStub{whoamiBody: `"admin"`, deleteStatus: http.StatusOK, deleteRange: "*/1"}
	srv := purgeTestServer(t, stub, "kunci-service")
	handler := srv.Handler()

	preflight := httptest.NewRecorder()
	handler.ServeHTTP(preflight, httptest.NewRequest(http.MethodOptions, "/api/pool-purge", nil))
	if preflight.Code != http.StatusNoContent {
		t.Fatalf("preflight harus 204, dapat %d", preflight.Code)
	}
	if origin := preflight.Header().Get("Access-Control-Allow-Origin"); origin != "*" {
		t.Fatalf("preflight harus mengizinkan origin panel, dapat %q", origin)
	}

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/pool-purge",
		strings.NewReader(`{"username":"budi","hash":"hash-sesi"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("POST tanpa sesi scanner harus diteruskan ke handler (200), dapat %d: %s", rec.Code, rec.Body.String())
	}
}

// pushRow adalah satu baris database uji untuk pengujian batas push.
type pushRow struct {
	ip      string
	country string
	org     string
	ping    int // skor push = ok_count*100 - fail_count*25 - ping/10: makin kecil ping, makin tinggi
	ok      int // berapa kali berhasil (0 = pakai 2, ambang minOK di pushToSupabase)
	exit    string
}

// capTestStore menyiapkan database uji berisi baris hidup & segar. Exit IP default diberi
// subnet /24 yang berbeda per baris supaya tidak saling mendedup (kecuali `exit` diisi).
func capTestStore(t *testing.T, rows []pushRow) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "caps.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	refs := make([]store.OrgRef, 0, len(rows))
	for i, row := range rows {
		ok := row.ok
		if ok == 0 {
			ok = 2
		}
		exit := row.exit
		if exit == "" {
			exit = fmt.Sprintf("203.0.%d.1", i+1)
		}
		for range ok {
			if err := st.RecordAlive(row.ip, 1080, "socks5", row.country, row.ping, 2, true, false, exit); err != nil {
				t.Fatal(err)
			}
		}
		if row.org != "" {
			refs = append(refs, store.OrgRef{IP: row.ip, Port: 1080, Org: row.org})
		}
	}
	if err := st.SetOrgs(refs); err != nil {
		t.Fatal(err)
	}
	return st
}

// capPush menjalankan pushToSupabase ke stub Supabase dan mengembalikan baris yang benar-benar
// terkirim plus catatan sinkron yang tercatat.
func capPush(t *testing.T, st *store.Store, cfg settings.Settings) ([]map[string]any, string) {
	t.Helper()
	var mu sync.Mutex
	var got []map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			body, _ := io.ReadAll(r.Body)
			var rows []map[string]any
			_ = json.Unmarshal(body, &rows)
			mu.Lock()
			got = append(got, rows...)
			mu.Unlock()
		}
		w.Header().Set("Content-Range", "0-0/0")
		w.WriteHeader(http.StatusCreated)
	}))
	defer ts.Close()

	client := supabase.New(ts.URL, "kunci-uji", "proxy_pool")
	srv := &Server{store: st}
	if _, _, err := srv.pushToSupabase(context.Background(), testClients(client), cfg, 100, nil, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("push ke stub harus sukses: %v", err)
	}
	srv.stateMu.Lock()
	note, _ := srv.lastSync["note"].(string)
	srv.stateMu.Unlock()
	return got, note
}

// rowsWith menjumlahkan baris payload yang nilai `key`-nya sama dengan `value`.
func rowsWith(rows []map[string]any, key, value string) int {
	n := 0
	for _, row := range rows {
		if got, _ := row[key].(string); got == value {
			n++
		}
	}
	return n
}

// ips mengambil kolom ip dari payload supaya urutan pilihan (skor tertinggi dulu) bisa diuji.
func ips(rows []map[string]any) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		ip, _ := row["ip"].(string)
		out = append(out, ip)
	}
	return out
}

// push_org_cap: satu penyedia (org) tidak boleh melebihi jatahnya, dan yang dipertahankan
// adalah baris dengan skor tertinggi (ping terkecil), karena urutan seleksi push = urut skor.
func TestPushOrgCapKeepsTopScores(t *testing.T) {
	st := capTestStore(t, []pushRow{
		{ip: "1.1.1.1", country: "ID", org: "Hetzner", ping: 100},
		{ip: "2.2.2.2", country: "SG", org: "Hetzner", ping: 900},
		{ip: "3.3.3.3", country: "MY", org: "Hetzner", ping: 1900},
		{ip: "4.4.4.4", country: "JP", org: "OVH", ping: 500},
	})
	cfg := settings.Default()
	cfg.PushOrgCap = 2
	cfg.PushCountryCap = 0 // isolasi: yang diuji hanya batas org

	got, note := capPush(t, st, cfg)
	if len(got) != 3 {
		t.Fatalf("2 baris Hetzner + 1 baris OVH harus terkirim, dapat %d (%v)", len(got), ips(got))
	}
	if n := rowsWith(got, "org", "Hetzner"); n != 2 {
		t.Fatalf("org Hetzner harus dibatasi 2 baris, dapat %d", n)
	}
	if n := rowsWith(got, "org", "OVH"); n != 1 {
		t.Fatalf("org OVH harus tetap terkirim, dapat %d", n)
	}
	kept := ips(got)
	for _, ip := range kept {
		if ip == "3.3.3.3" {
			t.Fatalf("baris Hetzner berskor terendah (ping terbesar) yang harus ditahan, dapat %v", kept)
		}
	}
	if note == "" || !strings.Contains(note, "1 ditahan (batas org/negara)") {
		t.Fatalf("catatan sinkron harus menyebut baris yang ditahan batas org/negara, dapat %q", note)
	}
}

// push_country_cap: batas per negara bekerja sama seperti batas org, dan kolom org ikut
// dikirim apa adanya (tidak lagi dikosongkan saat upsert).
func TestPushCountryCapKeepsTopScores(t *testing.T) {
	st := capTestStore(t, []pushRow{
		{ip: "1.1.1.1", country: "id", org: "Hetzner", ping: 100},
		{ip: "2.2.2.2", country: "ID", org: "OVH", ping: 900},
		{ip: "3.3.3.3", country: "ID", org: "DigitalOcean", ping: 1900},
		{ip: "4.4.4.4", country: "SG", org: "Vultr", ping: 500},
	})
	cfg := settings.Default()
	cfg.PushOrgCap = 0 // isolasi: yang diuji hanya batas negara
	cfg.PushCountryCap = 2

	got, note := capPush(t, st, cfg)
	if len(got) != 3 {
		t.Fatalf("2 baris ID + 1 baris SG harus terkirim, dapat %d (%v)", len(got), ips(got))
	}
	if n := rowsWith(got, "country", "ID"); n != 2 {
		t.Fatalf("negara ID harus dibatasi 2 baris, dapat %d", n)
	}
	if n := rowsWith(got, "country", "SG"); n != 1 {
		t.Fatalf("negara SG harus tetap terkirim, dapat %d", n)
	}
	if n := rowsWith(got, "ip", "3.3.3.3"); n != 0 {
		t.Fatal("baris ID berskor terendah yang harus ditahan")
	}
	if !strings.Contains(note, "1 ditahan (batas org/negara)") {
		t.Fatalf("catatan sinkron harus menyebut baris yang ditahan batas org/negara, dapat %q", note)
	}
	if n := rowsWith(got, "org", "Hetzner"); n != 1 {
		t.Fatal("kolom org harus ikut dikirim, bukan dikosongkan")
	}
}

// Batas 0 = tanpa batas (bawaan bisa dimatikan operator).
func TestPushCapsZeroMeansUnlimited(t *testing.T) {
	st := capTestStore(t, []pushRow{
		{ip: "1.1.1.1", country: "ID", org: "Hetzner", ping: 100},
		{ip: "2.2.2.2", country: "ID", org: "Hetzner", ping: 900},
		{ip: "3.3.3.3", country: "ID", org: "Hetzner", ping: 1900},
		{ip: "4.4.4.4", country: "ID", org: "Hetzner", ping: 2500},
	})
	cfg := settings.Default()
	cfg.PushOrgCap = 0
	cfg.PushCountryCap = 0

	got, note := capPush(t, st, cfg)
	if len(got) != 4 {
		t.Fatalf("cap 0 harus mengirim semua baris, dapat %d", len(got))
	}
	if strings.Contains(note, "batas org/negara") {
		t.Fatalf("tanpa batas tidak boleh ada baris yang ditahan, dapat %q", note)
	}
}

// Baris tanpa org (belum pernah terlihat di tabel Supabase) bukan "satu org yang sama":
// cap org tidak boleh menahannya.
func TestPushOrgCapSkipsRowsWithoutOrg(t *testing.T) {
	st := capTestStore(t, []pushRow{
		{ip: "1.1.1.1", country: "ID", ping: 100},
		{ip: "2.2.2.2", country: "SG", ping: 900},
		{ip: "3.3.3.3", country: "MY", ping: 1900},
		{ip: "4.4.4.4", country: "JP", org: "Hetzner", ping: 500},
		{ip: "5.5.5.5", country: "KR", org: "Hetzner", ping: 700},
	})
	cfg := settings.Default()
	cfg.PushOrgCap = 1
	cfg.PushCountryCap = 0

	got, note := capPush(t, st, cfg)
	if n := rowsWith(got, "org", ""); n != 3 {
		t.Fatalf("3 baris tanpa org harus tetap terkirim, dapat %d (%v)", n, ips(got))
	}
	if n := rowsWith(got, "org", "Hetzner"); n != 1 {
		t.Fatalf("org Hetzner harus dibatasi 1 baris, dapat %d", n)
	}
	if !strings.Contains(note, "1 ditahan (batas org/negara)") {
		t.Fatalf("catatan sinkron harus menyebut 1 baris yang ditahan, dapat %q", note)
	}
}

// Batas baru tidak boleh melemahkan saringan lama: dedup exit IP per /24 dan syarat
// ok_count ≥ 2 tetap berlaku.
func TestPushCapsKeepDedupAndOKCountRules(t *testing.T) {
	st := capTestStore(t, []pushRow{
		{ip: "1.1.1.1", country: "ID", org: "Hetzner", ping: 100, exit: "198.51.100.7"},
		{ip: "2.2.2.2", country: "ID", org: "OVH", ping: 200, exit: "198.51.100.99"}, // /24 sama
		{ip: "3.3.3.3", country: "ID", org: "Vultr", ping: 300, ok: 1},               // baru 1× berhasil
	})
	cfg := settings.Default() // bawaan 25 org / 60 negara: tidak ikut membatasi kasus ini

	got, note := capPush(t, st, cfg)
	if len(got) != 1 || ips(got)[0] != "1.1.1.1" {
		t.Fatalf("hanya baris pertama yang boleh terkirim (exit /24 sama, ok_count 1), dapat %v", ips(got))
	}
	if !strings.Contains(note, "1 ditahan (baru 1× berhasil)") {
		t.Fatalf("baris ok_count 1 harus tetap ditahan, dapat %q", note)
	}
	if !strings.Contains(note, "1 dilewati (exit IP sama)") {
		t.Fatalf("dedup exit IP per /24 harus tetap berlaku, dapat %q", note)
	}
}

// org yang dibaca dari tabel Supabase disimpan di database lokal: itu satu-satunya sumber
// nilai org, dan tanpanya batas org tidak pernah bisa bekerja.
func TestRemoteOrgsKeepsOnlyUsableRows(t *testing.T) {
	refs := remoteOrgs([]supabase.Row{
		{IP: "1.1.1.1", Port: "1080", Org: "Hetzner"},
		{IP: "2.2.2.2", Port: "1080", Org: "  "},
		{IP: "3.3.3.3", Port: "bukan-angka", Org: "OVH"},
		{IP: "4.4.4.4", Port: "8080", Org: "OVH"},
	})
	if len(refs) != 2 {
		t.Fatalf("hanya baris ber-org & ber-port angka yang boleh disimpan, dapat %+v", refs)
	}
	if refs[0].Org != "Hetzner" || refs[1].Port != 8080 {
		t.Fatalf("isi refs tidak sesuai: %+v", refs)
	}
}

// --------------------------------------------------- service key: pilih klien tulis + fallback

// Operasi tulis memilih klien ber-service key saat key itu terisi, dan jatuh ke anon (dengan
// catatan) saat kosong; operasi baca tetap anon.
func TestSupabaseWriteClientPrefersServiceKey(t *testing.T) {
	cfg := settings.Default()
	cfg.SupabaseURL = "https://contoh.test"
	cfg.SupabaseKey = "kunci-anon"
	cfg.SupabaseServiceKey = "kunci-service"
	srv := &Server{settings: &cfg}

	clients := srv.supabaseClients()
	if clients.write.Key != "kunci-service" {
		t.Fatalf("tulis harus memakai service key, dapat %q", clients.write.Key)
	}
	if clients.read.Key != "kunci-anon" {
		t.Fatalf("baca tetap anon, dapat %q", clients.read.Key)
	}
	if clients.note != "" {
		t.Fatalf("service key terisi: tidak boleh ada catatan fallback, dapat %q", clients.note)
	}

	cfg.SupabaseServiceKey = ""
	fallback := (&Server{settings: &cfg}).supabaseClients()
	if fallback.write.Key != "kunci-anon" {
		t.Fatalf("tanpa service key tulis harus jatuh ke anon, dapat %q", fallback.write.Key)
	}
	if !strings.Contains(fallback.note, "supabase_service_key") {
		t.Fatalf("catatan fallback harus menyebut supabase_service_key, dapat %q", fallback.note)
	}
}

// Push (upsert) benar-benar membawa service key saat terisi; tanpa key itu requestnya memakai
// anon dan catatannya muncul di ringkasan sinkron (bukan hanya di log).
func TestPushWritesWithServiceKeyAndNotesAnonFallback(t *testing.T) {
	st := capTestStore(t, []pushRow{{ip: "1.1.1.1", country: "ID", org: "Hetzner", ping: 100}})
	var mu sync.Mutex
	var postAuth []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			mu.Lock()
			postAuth = append(postAuth, r.Header.Get("Authorization"))
			mu.Unlock()
		}
		w.Header().Set("Content-Range", "0-0/1")
		w.WriteHeader(http.StatusCreated)
	}))
	defer ts.Close()

	cfg := settings.Default()
	cfg.SupabaseURL = ts.URL
	cfg.SupabaseKey = "kunci-anon"
	cfg.SupabaseServiceKey = "kunci-service"
	srv := &Server{store: st, settings: &cfg}
	if _, _, err := srv.pushToSupabase(context.Background(), srv.supabaseClients(), cfg, 100, nil, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("push dengan service key harus sukses: %v", err)
	}

	cfg.SupabaseServiceKey = ""
	srvAnon := &Server{store: st, settings: &cfg}
	if _, _, err := srvAnon.pushToSupabase(context.Background(), srvAnon.supabaseClients(), cfg, 100, nil, time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatalf("push dengan anon key harus tetap jalan (Supabase yang menolak, bukan scanner): %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(postAuth) != 2 {
		t.Fatalf("harus ada 2 upsert (service lalu anon), dapat %v", postAuth)
	}
	if postAuth[0] != "Bearer kunci-service" {
		t.Fatalf("upsert pertama harus memakai service key, dapat %q", postAuth[0])
	}
	if postAuth[1] != "Bearer kunci-anon" {
		t.Fatalf("tanpa service key upsert harus memakai anon, dapat %q", postAuth[1])
	}

	srv.stateMu.Lock()
	noteService := srv.lastSync["note"].(string)
	srv.stateMu.Unlock()
	if strings.Contains(noteService, "supabase_service_key") {
		t.Fatalf("service key terisi: ringkasan sinkron tidak boleh menyebut fallback anon, dapat %q", noteService)
	}
	srvAnon.stateMu.Lock()
	noteAnon := srvAnon.lastSync["note"].(string)
	srvAnon.stateMu.Unlock()
	if !strings.Contains(noteAnon, "supabase_service_key") {
		t.Fatalf("tanpa service key ringkasan sinkron harus memuat catatan fallback, dapat %q", noteAnon)
	}
}

// ---------------------------------------------------------------- retensi deploy_log (trim RPC)

// RPC retensi dipanggil dengan parameter & key yang benar: POST /rest/v1/rpc/deploy_log_trim
// berisi {"p_keep_days":30} dan Authorization memakai service key.
func TestDeployLogTrimStepUsesServiceKeyAndKeepDays(t *testing.T) {
	var mu sync.Mutex
	var path, auth string
	var body map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		path, auth = r.URL.Path, r.Header.Get("Authorization")
		_ = json.Unmarshal(raw, &body)
		mu.Unlock()
		_, _ = w.Write([]byte(`7`))
	}))
	defer ts.Close()

	cfg := settings.Default()
	cfg.SupabaseURL = ts.URL
	cfg.SupabaseKey = "kunci-anon"
	cfg.SupabaseServiceKey = "kunci-service"
	srv := &Server{settings: &cfg}

	step := srv.trimDeployLogStep(context.Background(), false)
	if !step.OK {
		t.Fatalf("trim selalu best-effort: OK harus true, dapat %+v", step)
	}
	mu.Lock()
	defer mu.Unlock()
	if path != "/rest/v1/rpc/deploy_log_trim" {
		t.Fatalf("RPC harus dipanggil di /rest/v1/rpc/deploy_log_trim, dapat %q", path)
	}
	if auth != "Bearer kunci-service" {
		t.Fatalf("RPC hanya boleh dipanggil dengan service key, dapat %q", auth)
	}
	if got, _ := body["p_keep_days"].(float64); got != 30 {
		t.Fatalf("p_keep_days harus 30, dapat %v", body["p_keep_days"])
	}
	if step.Counts["dihapus"] != 7 || !strings.Contains(step.Note, "7 baris") {
		t.Fatalf("balasan RPC (7) harus muncul di langkah, dapat %+v", step)
	}
}

// Kegagalan RPC (fungsi belum dijalankan, 5xx, key salah) hanya jadi catatan langkah: OK tetap
// true dan status pipeline tidak pernah diisi error. Tanpa service key, RPC-nya tidak dipanggil
// sama sekali.
func TestDeployLogTrimFailureDoesNotFailPipeline(t *testing.T) {
	var mu sync.Mutex
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"PGRST202","message":"Could not find the function public.deploy_log_trim"}`))
	}))
	defer ts.Close()

	cfg := settings.Default()
	cfg.SupabaseURL = ts.URL
	cfg.SupabaseKey = "kunci-anon"
	cfg.SupabaseServiceKey = "kunci-service"
	srv := &Server{settings: &cfg}

	step := srv.trimDeployLogStep(context.Background(), false)
	if !step.OK {
		t.Fatalf("kegagalan trim tidak boleh menandai langkah gagal, dapat %+v", step)
	}
	if !strings.Contains(step.Note, "best-effort") || !strings.Contains(step.Note, "PGRST202") {
		t.Fatalf("catatan langkah harus memuat sebab kegagalan, dapat %q", step.Note)
	}
	srv.addPipelineStep(step)
	srv.pipelineMu.Lock()
	pipelineErr := srv.pipeline.Error
	srv.pipelineMu.Unlock()
	if pipelineErr != "" {
		t.Fatalf("kegagalan trim tidak boleh mengisi error pipeline, dapat %q", pipelineErr)
	}

	mu.Lock()
	calls := requests
	mu.Unlock()
	if calls != 1 {
		t.Fatalf("stub harus dipanggil sekali, dapat %d", calls)
	}

	cfg.SupabaseServiceKey = ""
	srvNoKey := &Server{settings: &cfg}
	skip := srvNoKey.trimDeployLogStep(context.Background(), false)
	if !skip.OK || !strings.Contains(skip.Note, "supabase_service_key") {
		t.Fatalf("tanpa service key langkah harus dilewati dengan catatan, dapat %+v", skip)
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 1 {
		t.Fatalf("tanpa service key RPC tidak boleh dipanggil, dapat %d request", requests)
	}
}

// ------------------------------------------------------------------- backup state (user_states)

// Backup menulis berkas dengan bentuk yang benar, tidak pernah memuat kolom sandi/hash (walau
// Supabase mengirimnya), dan menyisakan 14 berkas terbaru.
func TestStateBackupWritesSanitizedFileAndRotates(t *testing.T) {
	dir := t.TempDir()
	backupDir := filepath.Join(dir, "backups")
	if err := os.MkdirAll(backupDir, 0o700); err != nil {
		t.Fatal(err)
	}
	// 15 berkas lama: dengan berkas hari ini, rotasi harus menyisakan 14 dan membuang 2 tertua.
	for day := 1; day <= 15; day++ {
		name := fmt.Sprintf("user_states-2026-01-%02d.json", day)
		if err := os.WriteFile(filepath.Join(backupDir, name), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var auths, paths []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		auths = append(auths, r.Header.Get("Authorization"))
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		switch {
		case strings.HasPrefix(r.URL.Path, "/rest/v1/user_states"):
			// Stub sengaja mengirim kolom sensitif yang TIDAK boleh ikut ke berkas backup.
			_, _ = w.Write([]byte(`[
				{"username":"budi","state_data":{"workers":[{"id":"w1","sub_token":"tok-lb-123"}]},"updated_at":"2026-11-15T00:00:00Z","password_hash":"hash-rahasia","api_key":"kunci-rahasia"},
				{"username":"siti","state_data":{"workers":[]},"updated_at":"2026-11-15T00:00:00Z"}
			]`))
		case r.Method == http.MethodHead:
			w.Header().Set("Content-Range", "0-0/9")
			w.WriteHeader(http.StatusOK)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	cfg := settings.Default()
	cfg.SupabaseURL = ts.URL
	cfg.SupabaseKey = "kunci-anon"
	cfg.SupabaseServiceKey = "kunci-service"
	srv := &Server{settings: &cfg, dataDir: dir}

	rec := httptest.NewRecorder()
	srv.handleStateBackup(rec, httptest.NewRequest(http.MethodPost, "/api/state-backup", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("backup harus 200, dapat %d: %s", rec.Code, rec.Body.String())
	}
	var result stateBackupResult
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatalf("balasan backup bukan JSON: %v", err)
	}
	wantPath := filepath.Join(backupDir, "user_states-"+time.Now().UTC().Format("2006-01-02")+".json")
	if result.Path != wantPath {
		t.Fatalf("path balasan harus %q, dapat %q", wantPath, result.Path)
	}
	if result.UserStates != 2 || result.ProxyPool != 9 {
		t.Fatalf("jumlah baris balasan salah: user_states=%d proxy_pool=%d", result.UserStates, result.ProxyPool)
	}
	if result.GeneratedAt == "" {
		t.Fatal("balasan harus memuat waktu backup")
	}

	raw, err := os.ReadFile(result.Path)
	if err != nil {
		t.Fatalf("berkas backup harus ada: %v", err)
	}
	if int64(len(raw)) != result.Bytes {
		t.Fatalf("ukuran balasan (%d) harus sama dengan ukuran berkas (%d)", result.Bytes, len(raw))
	}
	for _, banned := range []string{"password", "hash-rahasia", "api_key", "kunci-rahasia", "password_hash"} {
		if strings.Contains(string(raw), banned) {
			t.Fatalf("berkas backup tidak boleh memuat %q: %s", banned, raw)
		}
	}
	var file struct {
		GeneratedAt     string                       `json:"generated_at"`
		UserStatesCount int                          `json:"user_states_count"`
		ProxyPoolCount  int                          `json:"proxy_pool_count"`
		Truncated       bool                         `json:"truncated"`
		UserStates      []map[string]json.RawMessage `json:"user_states"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("berkas backup bukan JSON yang diharapkan: %v", err)
	}
	if file.GeneratedAt != result.GeneratedAt || file.UserStatesCount != 2 || file.ProxyPoolCount != 9 {
		t.Fatalf("ringkasan di berkas tidak cocok: %+v", file)
	}
	if file.Truncated {
		t.Fatal("2 baris jauh di bawah batas, berkas tidak boleh ditandai terpotong")
	}
	if len(file.UserStates) != 2 {
		t.Fatalf("harus ada 2 baris user_states, dapat %d", len(file.UserStates))
	}
	first := file.UserStates[0]
	for _, column := range []string{"username", "state_data", "updated_at"} {
		if _, ok := first[column]; !ok {
			t.Fatalf("kolom %q harus ikut tersalin, dapat %v", column, first)
		}
	}
	// Muatan state pengguna disalin utuh (token langganan LB ada di dalam state_data, bukan
	// kolom tabel), karena itu justru data yang dipulihkan dari backup.
	if !strings.Contains(string(first["state_data"]), "sub_token") {
		t.Fatalf("state_data harus disalin utuh, dapat %s", first["state_data"])
	}

	// Semua request backup (baca user_states + hitung proxy_pool) memakai service key.
	mu.Lock()
	for _, auth := range auths {
		if auth != "Bearer kunci-service" {
			mu.Unlock()
			t.Fatalf("backup harus memakai service key, dapat %q (paths %v)", auth, paths)
		}
	}
	mu.Unlock()

	// Rotasi: hanya 14 berkas terbaru yang tinggal, dan yang dibuang adalah 2 tertua.
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	if len(names) != stateBackupKeep {
		t.Fatalf("harus tersisa %d berkas, dapat %d (%v)", stateBackupKeep, len(names), names)
	}
	for _, gone := range []string{"user_states-2026-01-01.json", "user_states-2026-01-02.json"} {
		if _, err := os.Stat(filepath.Join(backupDir, gone)); !os.IsNotExist(err) {
			t.Fatalf("berkas tertua %q harus dihapus, err=%v", gone, err)
		}
	}
	if len(result.Removed) != 2 {
		t.Fatalf("balasan harus menyebut 2 berkas terhapus, dapat %v", result.Removed)
	}
}

// Tanpa service key backup tidak jalan sama sekali (tabel user_states sudah dicabut dari anon),
// dan balasannya 503 dengan sebab yang jelas — bukan berkas kosong yang tampak sukses.
func TestStateBackupWithoutServiceKeyIs503(t *testing.T) {
	requests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	dir := t.TempDir()
	cfg := settings.Default()
	cfg.SupabaseURL = ts.URL
	cfg.SupabaseKey = "kunci-anon"
	srv := &Server{settings: &cfg, dataDir: dir}

	rec := httptest.NewRecorder()
	srv.handleStateBackup(rec, httptest.NewRequest(http.MethodPost, "/api/state-backup", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("tanpa service key harus 503, dapat %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "supabase_service_key") {
		t.Fatalf("pesan 503 harus menyebut supabase_service_key, dapat %s", rec.Body.String())
	}
	if requests != 0 {
		t.Fatalf("tanpa service key tidak boleh ada request ke Supabase, dapat %d", requests)
	}
	if _, err := os.Stat(filepath.Join(dir, "backups")); !os.IsNotExist(err) {
		t.Fatalf("folder backup tidak boleh dibuat saat gagal, err=%v", err)
	}
}

// Hanya POST yang dilayani: endpoint ini menulis berkas di host scanner.
func TestStateBackupRejectsNonPost(t *testing.T) {
	cfg := settings.Default()
	srv := &Server{settings: &cfg, dataDir: t.TempDir()}
	rec := httptest.NewRecorder()
	srv.handleStateBackup(rec, httptest.NewRequest(http.MethodGet, "/api/state-backup", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET harus 405, dapat %d", rec.Code)
	}
}

// Rute backup tidak boleh publik: tanpa sesi/token gate menjawab 401, dan dengan
// X-Api-Token yang benar request diteruskan ke handler (yang lalu menjawab 503 karena service
// key kosong — bukan 401).
func TestStateBackupRequiresSessionOrToken(t *testing.T) {
	cfg := settings.Default()
	cfg.Token = "token-uji"
	srv := &Server{settings: &cfg, sessions: auth.New(), dataDir: t.TempDir()}
	handler := srv.Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/state-backup", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("tanpa sesi/token harus 401, dapat %d: %s", rec.Code, rec.Body.String())
	}

	req := httptest.NewRequest(http.MethodPost, "/api/state-backup", nil)
	req.Header.Set("X-Api-Token", "token-uji")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code == http.StatusUnauthorized {
		t.Fatalf("token API yang benar harus lolos gate, dapat %d: %s", rec.Code, rec.Body.String())
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("tanpa service key handler harus menjawab 503, dapat %d: %s", rec.Code, rec.Body.String())
	}
}

// telegramStub menggantikan api.telegram.org untuk tes notifikasi: request dicatat, balasan
// diatur tes. Dengan ini tidak pernah ada pesan sungguhan yang dikirim.
type telegramStub struct {
	*httptest.Server
	mu       sync.Mutex
	requests []telegramRequest
	status   int
	body     string
}

type telegramRequest struct {
	method      string
	path        string
	contentType string
	form        neturl.Values
}

func newTelegramStub(t *testing.T) *telegramStub {
	t.Helper()
	stub := &telegramStub{status: http.StatusOK, body: `{"ok":true,"result":{"message_id":1}}`}
	stub.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("body form tidak valid: %v", err)
		}
		stub.mu.Lock()
		stub.requests = append(stub.requests, telegramRequest{
			method: r.Method, path: r.URL.Path, contentType: r.Header.Get("Content-Type"), form: r.Form,
		})
		status, body := stub.status, stub.body
		stub.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(stub.Close)
	return stub
}

func (s *telegramStub) reply(status int, body string) {
	s.mu.Lock()
	s.status, s.body = status, body
	s.mu.Unlock()
}

func (s *telegramStub) sent() []telegramRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]telegramRequest(nil), s.requests...)
}

// stubTelegramClient mengarahkan klien notifikasi server ke stub (bukan Telegram sungguhan).
func stubTelegramClient(stub *telegramStub) func(token, chatID string) *notify.Client {
	return func(token, chatID string) *notify.Client {
		return &notify.Client{Token: token, ChatID: chatID, Base: stub.URL, HTTP: stub.Client()}
	}
}

// publicSettings tidak boleh membocorkan token bot: klien hanya menerima penanda sudah-diisi.
func TestPublicSettingsHidesTelegramToken(t *testing.T) {
	pub := publicSettings(settings.Settings{TelegramBotToken: "123:RAHASIA", TelegramChatID: "-1001"})
	if _, exists := pub["telegram_bot_token"]; exists {
		t.Fatal("publicSettings tidak boleh mengirim telegram_bot_token")
	}
	if pub["telegram_bot_token_set"] != true {
		t.Fatalf("telegram_bot_token_set harus true, dapat %v", pub["telegram_bot_token_set"])
	}
	if pub["telegram_chat_id"] != "-1001" {
		t.Fatalf("chat id boleh dibaca klien, dapat %v", pub["telegram_chat_id"])
	}
}

// Patch setelan memangkas spasi token/chat id (Normalize) dan mengembalikannya tanpa token.
func TestSettingsPatchTrimsTelegram(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "tg-set.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	cfg := settings.Default()
	cfgPath := filepath.Join(t.TempDir(), "settings.json")
	srv := &Server{store: st, settings: &cfg, sessions: auth.New(), cfgPath: cfgPath}
	srv.sched = scraper.NewScheduler(scraper.NewRunner(st), cfg)

	body := strings.NewReader(`{"telegram_bot_token":"  123:ABC \n","telegram_chat_id":"  -1001 "}`)
	rec := httptest.NewRecorder()
	srv.handleSettings(rec, httptest.NewRequest(http.MethodPost, "/api/settings", body))
	if rec.Code != http.StatusOK {
		t.Fatalf("simpan setelan harus 200, dapat %d: %s", rec.Code, rec.Body.String())
	}
	if got := srv.currentSettings().TelegramBotToken; got != "123:ABC" {
		t.Fatalf("token harus dipangkas, dapat %q", got)
	}
	if got := srv.currentSettings().TelegramChatID; got != "-1001" {
		t.Fatalf("chat id harus dipangkas, dapat %q", got)
	}
	var payload struct {
		Settings map[string]any `json:"settings"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if _, exists := payload.Settings["telegram_bot_token"]; exists {
		t.Fatal("balasan POST /api/settings tidak boleh memuat token bot")
	}
	if payload.Settings["telegram_bot_token_set"] != true {
		t.Fatalf("balasan harus menandai token sudah diisi, dapat %v", payload.Settings["telegram_bot_token_set"])
	}
}

// Endpoint uji: tanpa setelan lengkap balasannya jelas (503/400), bukan 500 dan bukan request
// ke Telegram.
func TestNotifyTestEndpointNeedsFullSettings(t *testing.T) {
	cfg := settings.Default()
	srv := &Server{settings: &cfg, sessions: auth.New()}
	rec := httptest.NewRecorder()
	srv.handleNotifyTest(rec, httptest.NewRequest(http.MethodPost, "/api/notify-test", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("tanpa setelan Telegram harus 503, dapat %d: %s", rec.Code, rec.Body.String())
	}
	var empty struct {
		OK      bool     `json:"ok"`
		Error   string   `json:"error"`
		Missing []string `json:"missing"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &empty); err != nil {
		t.Fatal(err)
	}
	if empty.OK || !strings.Contains(empty.Error, "telegram_bot_token") || !strings.Contains(empty.Error, "telegram_chat_id") {
		t.Fatalf("pesan 503 harus jelas, dapat %+v", empty)
	}
	if len(empty.Missing) != 2 {
		t.Fatalf("kedua field harus dilaporkan kurang, dapat %v", empty.Missing)
	}

	// Token ada tetapi chat id kosong: kesalahan setelan (400), bukan 503.
	cfgTokenOnly := settings.Default()
	cfgTokenOnly.TelegramBotToken = "123:TOKEN"
	srvTokenOnly := &Server{settings: &cfgTokenOnly, sessions: auth.New()}
	rec = httptest.NewRecorder()
	srvTokenOnly.handleNotifyTest(rec, httptest.NewRequest(http.MethodPost, "/api/notify-test", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("chat id kosong harus 400, dapat %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "telegram_chat_id") {
		t.Fatalf("pesan 400 harus menyebut field yang kurang: %s", rec.Body.String())
	}

	// Hanya POST.
	rec = httptest.NewRecorder()
	srvTokenOnly.handleNotifyTest(rec, httptest.NewRequest(http.MethodGet, "/api/notify-test", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET harus 405, dapat %d", rec.Code)
	}
}

// Hasil balasan Telegram diteruskan apa adanya: sukses → ok + telegram_status 200; ditolak →
// ok false + status balasan Telegram + deskripsinya.
func TestNotifyTestForwardsTelegramResult(t *testing.T) {
	stub := newTelegramStub(t)
	cfg := settings.Default()
	cfg.TelegramBotToken = "123:TOKEN"
	cfg.TelegramChatID = "-1001"
	srv := &Server{settings: &cfg, sessions: auth.New(), notifyClient: stubTelegramClient(stub)}

	rec := httptest.NewRecorder()
	srv.handleNotifyTest(rec, httptest.NewRequest(http.MethodPost, "/api/notify-test", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("kirim uji sukses harus 200, dapat %d: %s", rec.Code, rec.Body.String())
	}
	var ok struct {
		OK     bool `json:"ok"`
		Status int  `json:"telegram_status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &ok); err != nil {
		t.Fatal(err)
	}
	if !ok.OK || ok.Status != http.StatusOK {
		t.Fatalf("balasan sukses salah: %s", rec.Body.String())
	}
	sent := stub.sent()
	if len(sent) != 1 {
		t.Fatalf("harus satu pesan uji terkirim, dapat %d", len(sent))
	}
	if sent[0].path != "/bot123:TOKEN/sendMessage" || sent[0].method != http.MethodPost {
		t.Fatalf("request uji salah: %+v", sent[0])
	}
	if sent[0].form.Get("chat_id") != "-1001" || !strings.Contains(sent[0].form.Get("text"), "uji notifikasi") {
		t.Fatalf("isi pesan uji salah: %+v", sent[0].form)
	}

	// Telegram menolak: statusnya diteruskan dan payload menyebut alasannya.
	stub.reply(http.StatusBadRequest, `{"ok":false,"error_code":400,"description":"Bad Request: chat not found"}`)
	rec = httptest.NewRecorder()
	srv.handleNotifyTest(rec, httptest.NewRequest(http.MethodPost, "/api/notify-test", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("penolakan Telegram harus 502, dapat %d: %s", rec.Code, rec.Body.String())
	}
	var bad struct {
		OK             bool   `json:"ok"`
		Error          string `json:"error"`
		TelegramStatus int    `json:"telegram_status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &bad); err != nil {
		t.Fatal(err)
	}
	if bad.OK || bad.TelegramStatus != http.StatusBadRequest || !strings.Contains(bad.Error, "chat not found") {
		t.Fatalf("balasan penolakan harus meneruskan hasil Telegram, dapat %s", rec.Body.String())
	}
}

// Rute uji ikut gate yang sama dengan rute /api/* lain: tanpa sesi/token → 401.
func TestNotifyTestRequiresSessionOrToken(t *testing.T) {
	cfg := settings.Default()
	cfg.Token = "token-uji"
	srv := &Server{settings: &cfg, sessions: auth.New()}
	handler := srv.Handler()

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/notify-test", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("tanpa sesi/token harus 401, dapat %d: %s", rec.Code, rec.Body.String())
	}

	req := httptest.NewRequest(http.MethodPost, "/api/notify-test", nil)
	req.Header.Set("X-Api-Token", "token-uji")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("token benar harus lolos gate lalu 503 (setelan kosong), dapat %d: %s", rec.Code, rec.Body.String())
	}
}

// Kegagalan kirim notifikasi tidak boleh menggagalkan pipeline: langkahnya tetap OK, hanya ada
// catatan "notifikasi gagal" (tidak ada pipeline.error).
func TestPipelineNotifyFailureKeepsStepOK(t *testing.T) {
	stub := newTelegramStub(t)
	stub.reply(http.StatusInternalServerError, `{"ok":false,"error_code":500,"description":"stub rusak"}`)

	cfg := settings.Default()
	cfg.TelegramBotToken = "123:TOKEN"
	cfg.TelegramChatID = "-1001"
	srv := &Server{settings: &cfg, sessions: auth.New(), notifyClient: stubTelegramClient(stub)}
	srv.addPipelineStep(PipelineStep{Name: "scrape sumber & verifikasi", OK: true, Note: "140 kandidat diperiksa, 14 hidup"})

	srv.notifyPipeline(context.Background(), cfg, "scrape sumber & verifikasi",
		notify.DailyMessage(map[string]int{"kandidat": 1200, "hidup": 14, "mati": 1186}, 97, 34*time.Minute))

	srv.pipelineMu.Lock()
	steps := append([]PipelineStep(nil), srv.pipeline.Steps...)
	statusErr := srv.pipeline.Error
	running := srv.pipeline.Running
	srv.pipelineMu.Unlock()
	if len(steps) != 1 {
		t.Fatalf("langkah pipeline tidak boleh bertambah/hilang, dapat %d", len(steps))
	}
	if !steps[0].OK || !strings.Contains(steps[0].Note, "notifikasi gagal") {
		t.Fatalf("langkah harus tetap OK + bercatatan, dapat %+v", steps[0])
	}
	if !strings.Contains(steps[0].Note, "140 kandidat diperiksa") {
		t.Fatalf("catatan asli langkah tidak boleh hilang, dapat %q", steps[0].Note)
	}
	if statusErr != "" || running {
		t.Fatalf("kegagalan notifikasi tidak boleh menjadikan pipeline gagal: err=%q running=%v", statusErr, running)
	}
	if len(stub.sent()) != 1 {
		t.Fatalf("notifikasi tetap dicoba sekali, dapat %d", len(stub.sent()))
	}
}

// Tanpa setelan lengkap tidak ada request sama sekali dan tidak ada catatan palsu di langkah.
func TestPipelineNotifySkippedWithoutSettings(t *testing.T) {
	stub := newTelegramStub(t)
	cfg := settings.Default()
	srv := &Server{settings: &cfg, sessions: auth.New(), notifyClient: stubTelegramClient(stub)}
	srv.addPipelineStep(PipelineStep{Name: "cek lokal (refresh + target negara)", OK: true, Note: "300 baris diverifikasi ulang"})

	srv.notifyPipeline(context.Background(), cfg, "cek lokal (refresh + target negara)",
		notify.LocalMessage(map[string]int{"diperiksa": 300, "hidup": 88}))

	if got := stub.sent(); len(got) != 0 {
		t.Fatalf("tanpa setelan tidak boleh ada request, dapat %d", len(got))
	}
	srv.pipelineMu.Lock()
	note := srv.pipeline.Steps[0].Note
	srv.pipelineMu.Unlock()
	if note != "300 baris diverifikasi ulang" {
		t.Fatalf("catatan langkah tidak boleh berubah, dapat %q", note)
	}
}

// Cek lokal (jadwal 6 jam) mengirim pesan "cek 6 jam" tepat satu kali dan tidak menggagalkan
// pipeline; alamat/path/body-nya sama seperti pengiriman biasa.
func TestRunPipelineSyncSendsLocalCheckNotification(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "pipe-tg.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	stub := newTelegramStub(t)
	cfg := settings.Default()
	cfg.TelegramBotToken = "123:TOKEN"
	cfg.TelegramChatID = "-1001"
	srv := &Server{store: st, runner: scraper.NewRunner(st), settings: &cfg, notifyClient: stubTelegramClient(stub)}

	if err := srv.RunPipelineSync(true); err != nil {
		t.Fatalf("cek lokal tanpa Supabase harus sukses: %v", err)
	}
	sent := stub.sent()
	if len(sent) != 1 {
		t.Fatalf("cek lokal harus mengirim tepat satu notifikasi, dapat %d", len(sent))
	}
	if sent[0].path != "/bot123:TOKEN/sendMessage" {
		t.Fatalf("path notifikasi salah: %s", sent[0].path)
	}
	if got := sent[0].form.Get("text"); !strings.HasPrefix(got, "Pan-VlessCloudflare - cek 6 jam") || !strings.Contains(got, "Diperiksa ulang: ") {
		t.Fatalf("isi pesan cek 6 jam salah:\n%s", got)
	}

	srv.pipelineMu.Lock()
	steps := append([]PipelineStep(nil), srv.pipeline.Steps...)
	statusErr := srv.pipeline.Error
	srv.pipelineMu.Unlock()
	if statusErr != "" {
		t.Fatalf("notifikasi sukses tidak boleh mengisi error pipeline: %q", statusErr)
	}
	for _, step := range steps {
		if strings.HasPrefix(step.Name, "cek lokal") {
			if !step.OK {
				t.Fatalf("langkah cek lokal harus sukses, dapat %+v", step)
			}
			if strings.Contains(step.Note, "notifikasi gagal") {
				t.Fatalf("kirim sukses tidak boleh menulis catatan gagal, dapat %q", step.Note)
			}
		}
	}
}
