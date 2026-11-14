package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
