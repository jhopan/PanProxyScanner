package supabase

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// Operasi tulis memakai service key bila terisi, dan jatuh ke anon (nilai kedua false) bila
// kosong; pemanggil yang mencatat fallback itu.
func TestNewForWritePrefersServiceKey(t *testing.T) {
	client, usedService := NewForWrite("https://contoh.test", "kunci-anon", "kunci-service", "proxy_pool")
	if !usedService || client.Key != "kunci-service" {
		t.Fatalf("service key terisi → klien tulis harus memakainya, dapat %q (used=%v)", client.Key, usedService)
	}

	fallback, usedService := NewForWrite("https://contoh.test", "kunci-anon", "  ", "proxy_pool")
	if usedService || fallback.Key != "kunci-anon" {
		t.Fatalf("service key kosong → klien tulis harus jatuh ke anon, dapat %q (used=%v)", fallback.Key, usedService)
	}
	if !fallback.Configured() {
		t.Fatal("klien hasil fallback tetap terkonfigurasi kalau anon key terisi")
	}
}

// TrimDeployLog memanggil RPC dengan nama parameter yang dipakai migrasi cfvpn-hub
// (`p_keep_days`) dan membaca balasan angka apa adanya.
func TestTrimDeployLogSendsKeepDays(t *testing.T) {
	var mu sync.Mutex
	var path, body string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		path, body = r.URL.Path, string(raw)
		mu.Unlock()
		_, _ = w.Write([]byte(`12`))
	}))
	defer ts.Close()

	deleted, err := New(ts.URL, "kunci-service", "proxy_pool").TrimDeployLog(context.Background(), 30)
	if err != nil {
		t.Fatalf("trim harus sukses: %v", err)
	}
	if deleted != 12 {
		t.Fatalf("jumlah baris terhapus harus dibaca dari balasan, dapat %d", deleted)
	}
	mu.Lock()
	defer mu.Unlock()
	if path != "/rest/v1/rpc/"+DeployLogTrimRPC {
		t.Fatalf("RPC harus dipanggil di /rest/v1/rpc/%s, dapat %q", DeployLogTrimRPC, path)
	}
	var sent map[string]any
	if err := json.Unmarshal([]byte(body), &sent); err != nil {
		t.Fatalf("body RPC bukan JSON: %v", err)
	}
	if got, _ := sent["p_keep_days"].(float64); got != 30 {
		t.Fatalf("p_keep_days harus 30, dapat %v", sent["p_keep_days"])
	}
}

// Tanpa keepDays eksplisit dipakai retensi bawaan (30 hari), dan balasan `null` bukan error.
func TestTrimDeployLogDefaultsAndNullReply(t *testing.T) {
	var mu sync.Mutex
	var sent map[string]any
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		_ = json.Unmarshal(raw, &sent)
		mu.Unlock()
		_, _ = w.Write([]byte(`null`))
	}))
	defer ts.Close()

	deleted, err := New(ts.URL, "kunci-service", "proxy_pool").TrimDeployLog(context.Background(), 0)
	if err != nil {
		t.Fatalf("balasan null bukan kegagalan: %v", err)
	}
	if deleted != 0 {
		t.Fatalf("null berarti 0 baris, dapat %d", deleted)
	}
	mu.Lock()
	defer mu.Unlock()
	if got, _ := sent["p_keep_days"].(float64); got != DeployLogKeepDays {
		t.Fatalf("p_keep_days bawaan harus %d, dapat %v", DeployLogKeepDays, sent["p_keep_days"])
	}
}

// Kegagalan RPC (fungsi belum dijalankan) dilaporkan sebagai error berisi kode PostgREST —
// pemanggil yang memutuskan itu hanya catatan langkah.
func TestTrimDeployLogReportsMissingFunction(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"PGRST202","message":"Could not find the function public.deploy_log_trim"}`))
	}))
	defer ts.Close()

	if _, err := New(ts.URL, "kunci-service", "proxy_pool").TrimDeployLog(context.Background(), 30); err == nil {
		t.Fatal("RPC yang tidak ada harus jadi error, bukan diam-diam sukses")
	} else if !strings.Contains(err.Error(), "PGRST202") {
		t.Fatalf("error harus memuat kode PostgREST, dapat %q", err.Error())
	}
}

// FetchUserStates meminta kolom backup secara eksplisit (tidak pernah `*`) dan membuang kolom
// sensitif walau server mengirimkannya; muatan `state_data` disalin utuh.
func TestFetchUserStatesSelectsBackupColumnsAndStripsSensitive(t *testing.T) {
	var mu sync.Mutex
	var got url.Values
	var path string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		got, path = r.URL.Query(), r.URL.Path
		mu.Unlock()
		_, _ = w.Write([]byte(`[{"username":"budi","state_data":{"workers":[{"sub_token":"tok-1"}]},"updated_at":"2026-11-15T00:00:00Z","password_hash":"rahasia","api_key":"rahasia","session_secret":"rahasia"}]`))
	}))
	defer ts.Close()

	rows, err := New(ts.URL, "kunci-service", "proxy_pool").FetchUserStates(context.Background(), 10)
	if err != nil {
		t.Fatalf("fetch harus sukses: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if path != "/rest/v1/"+UserStateTable {
		t.Fatalf("harus membaca tabel %s, dapat %q", UserStateTable, path)
	}
	if selectCols := got.Get("select"); selectCols != UserStateBackupSelect {
		t.Fatalf("select harus %q, dapat %q", UserStateBackupSelect, selectCols)
	}
	for key, values := range got {
		for _, value := range values {
			if strings.Contains(value, "*") {
				t.Fatalf("permintaan backup tidak boleh memakai wildcard (%s=%s)", key, value)
			}
		}
	}
	if len(rows) != 1 {
		t.Fatalf("harus ada 1 baris, dapat %d", len(rows))
	}
	for _, banned := range []string{"password_hash", "api_key", "session_secret"} {
		if _, ok := rows[0][banned]; ok {
			t.Fatalf("kolom %q tidak boleh ikut ke backup, dapat %v", banned, rows[0])
		}
	}
	if _, ok := rows[0]["state_data"]; !ok {
		t.Fatalf("state_data harus ikut (itu muatan yang dipulihkan), dapat %v", rows[0])
	}
	if !strings.Contains(string(rows[0]["state_data"]), "sub_token") {
		t.Fatalf("state_data harus utuh, dapat %s", rows[0]["state_data"])
	}
	if len(rows[0]) != 3 {
		t.Fatalf("hanya 3 kolom allowlist yang boleh tersisa, dapat %v", rows[0])
	}
}

// sanitizeUserState adalah pertahanan berlapis: kolom allowlist disalin, nama kolom sensitif
// apa pun dibuang, dan baris tanpa kolom yang berguna tidak menghasilkan entri kosong.
func TestSanitizeUserState(t *testing.T) {
	clean := sanitizeUserState(map[string]json.RawMessage{
		"username":      json.RawMessage(`"budi"`),
		"state_data":    json.RawMessage(`{"workers":[]}`),
		"updated_at":    json.RawMessage(`"2026-11-15T00:00:00Z"`),
		"password_hash": json.RawMessage(`"rahasia"`),
		"user_password": json.RawMessage(`"rahasia"`),
		"kolom_lain":    json.RawMessage(`"abaikan"`),
	})
	if len(clean) != 3 {
		t.Fatalf("hanya kolom allowlist non-sensitif yang boleh tersisa, dapat %v", clean)
	}
	if _, ok := clean["password_hash"]; ok {
		t.Fatal("password_hash tidak boleh lolos")
	}
	if _, ok := clean["kolom_lain"]; ok {
		t.Fatal("kolom di luar allowlist tidak boleh ikut")
	}

	for _, name := range []string{"Password_Hash", " user_password", "api_key", "service_key", "session_secret", "pwd"} {
		if !sensitiveUserStateColumn(name) {
			t.Fatalf("%q harus dianggap kolom sensitif", name)
		}
	}
	for _, name := range []string{"username", "state_data", "updated_at"} {
		if sensitiveUserStateColumn(name) {
			t.Fatalf("%q bukan kolom sensitif", name)
		}
	}

	if got := sanitizeUserState(map[string]json.RawMessage{"password_hash": json.RawMessage(`"x"`)}); len(got) != 0 {
		t.Fatalf("baris tanpa kolom berguna harus kosong, dapat %v", got)
	}
}
