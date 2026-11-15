package notify

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// Pesan harian memakai angka Counts langkah scrape; jumlah baris yang dikirim ke pool datang
// dari langkah push dan lama langkah scrape ditulis dalam menit.
func TestDailyMessageFullCounts(t *testing.T) {
	got := DailyMessage(map[string]int{"kandidat": 1200, "dicek": 1200, "hidup": 140, "mati": 1060}, 97, 34*time.Minute)
	want := "Pan-VlessCloudflare - scrape harian\n" +
		"Kandidat ditarik: 1200\n" +
		"Hidup: 140\n" +
		"Mati: 1060\n" +
		"Dikirim ke pool: 97\n" +
		"Waktu: selesai dalam 34 menit"
	if got != want {
		t.Fatalf("pesan harian tidak sesuai:\ndapat:\n%s\nmau:\n%s", got, want)
	}
}

// Counts yang tidak lengkap (langkah scrape tidak melaporkan angka) harus ditulis "-", bukan 0 —
// dan langkah push yang dilewati juga "-".
func TestDailyMessageMissingCounts(t *testing.T) {
	got := DailyMessage(nil, -1, 30*time.Second)
	for _, line := range []string{
		"Kandidat ditarik: -",
		"Hidup: -",
		"Mati: -",
		"Dikirim ke pool: -",
		"Waktu: selesai dalam kurang dari 1 menit",
	} {
		if !strings.Contains(got, line) {
			t.Fatalf("pesan harian harus memuat %q, dapat:\n%s", line, got)
		}
	}
	if strings.Contains(got, ": 0") {
		t.Fatalf("angka yang tidak dilaporkan tidak boleh jadi 0:\n%s", got)
	}

	// Counts hanya memuat sebagian: yang ada tetap ditampilkan.
	partial := DailyMessage(map[string]int{"hidup": 3}, 0, 90*time.Minute)
	if !strings.Contains(partial, "Hidup: 3") || !strings.Contains(partial, "Mati: -") {
		t.Fatalf("counts sebagian harus ditampilkan apa adanya:\n%s", partial)
	}
	if !strings.Contains(partial, "Dikirim ke pool: 0") || !strings.Contains(partial, "selesai dalam 90 menit") {
		t.Fatalf("push 0 baris dan 90 menit harus dilaporkan:\n%s", partial)
	}
}

// Pesan cek 6 jam: angka mati dihitung dari diperiksa - hidup kalau Counts tidak memuatnya.
func TestLocalMessageComputesDeadFromChecked(t *testing.T) {
	got := LocalMessage(map[string]int{"diperiksa": 300, "hidup": 88})
	want := "Pan-VlessCloudflare - cek 6 jam\nDiperiksa ulang: 300\nMasih hidup: 88\nSudah mati: 212"
	if got != want {
		t.Fatalf("pesan cek lokal tidak sesuai:\ndapat:\n%s\nmau:\n%s", got, want)
	}
}

// Counts yang sudah memuat angka mati dipakai apa adanya, dan yang benar-benar kosong
// menghasilkan "-" (bukan angka karangan).
func TestLocalMessagePrefersDeadCount(t *testing.T) {
	if got := LocalMessage(map[string]int{"diperiksa": 10, "hidup": 4, "mati": 6}); !strings.Contains(got, "Sudah mati: 6") {
		t.Fatalf("angka mati dari Counts harus dipakai, dapat:\n%s", got)
	}
	if got := LocalMessage(nil); !strings.Contains(got, "Sudah mati: -") || !strings.Contains(got, "Diperiksa ulang: -") {
		t.Fatalf("tanpa Counts semua angka harus -, dapat:\n%s", got)
	}
	// Hidup lebih besar dari diperiksa (angka tidak konsisten) tidak boleh jadi negatif.
	if got := LocalMessage(map[string]int{"diperiksa": 5, "hidup": 9}); !strings.Contains(got, "Sudah mati: 0") {
		t.Fatalf("selisih negatif harus dijepit ke 0, dapat:\n%s", got)
	}
}

// configuredClient membuat klien yang menunjuk server stub (bukan api.telegram.org).
func configuredClient(base, token, chatID string, client *http.Client) *Client {
	return &Client{Token: token, ChatID: chatID, Base: base, HTTP: client}
}

// Pengiriman harus memakai POST form-urlencoded ke /bot<token>/sendMessage dengan chat_id,
// text, dan disable_web_page_preview=true.
func TestSendUsesTelegramSendMessageEndpoint(t *testing.T) {
	type seen struct {
		method      string
		path        string
		contentType string
		form        map[string]string
	}
	var mu sync.Mutex
	var captured []seen
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Errorf("body form tidak valid: %v", err)
		}
		s := seen{method: r.Method, path: r.URL.Path, contentType: r.Header.Get("Content-Type")}
		s.form = map[string]string{
			"chat_id":                  r.Form.Get("chat_id"),
			"text":                     r.Form.Get("text"),
			"disable_web_page_preview": r.Form.Get("disable_web_page_preview"),
		}
		mu.Lock()
		captured = append(captured, s)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":7}}`))
	}))
	defer ts.Close()

	client := configuredClient(ts.URL, "123:TOKEN", "-1001234567890", ts.Client())
	status, err := client.Send(context.Background(), "halo telegram")
	if err != nil {
		t.Fatalf("kirim ke stub harus sukses: %v", err)
	}
	if status != http.StatusOK {
		t.Fatalf("status balasan harus 200, dapat %d", status)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(captured) != 1 {
		t.Fatalf("harus tepat satu request, dapat %d", len(captured))
	}
	got := captured[0]
	if got.method != http.MethodPost {
		t.Fatalf("harus POST, dapat %s", got.method)
	}
	if got.path != "/bot123:TOKEN/sendMessage" {
		t.Fatalf("path salah: %s", got.path)
	}
	if !strings.HasPrefix(got.contentType, "application/x-www-form-urlencoded") {
		t.Fatalf("Content-Type salah: %s", got.contentType)
	}
	if got.form["chat_id"] != "-1001234567890" || got.form["text"] != "halo telegram" || got.form["disable_web_page_preview"] != "true" {
		t.Fatalf("body form salah: %+v", got.form)
	}
}

// Tanpa token atau chat id tidak ada request sama sekali (dan Send mengembalikan error).
func TestSendSkippedWithoutSettings(t *testing.T) {
	var mu sync.Mutex
	hits := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hits++
		mu.Unlock()
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer ts.Close()

	cases := []*Client{
		configuredClient(ts.URL, "", "42", ts.Client()),
		configuredClient(ts.URL, "123:TOKEN", "", ts.Client()),
		configuredClient(ts.URL, "   ", "  ", ts.Client()),
		nil,
	}
	for _, client := range cases {
		if client.Configured() {
			t.Fatalf("client tanpa token+chat id tidak boleh dianggap aktif: %+v", client)
		}
		status, err := client.Send(context.Background(), "tidak boleh terkirim")
		if err == nil {
			t.Fatal("Send tanpa setelan lengkap harus mengembalikan error")
		}
		if status != 0 {
			t.Fatalf("tanpa request status harus 0, dapat %d", status)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if hits != 0 {
		t.Fatalf("tidak boleh ada request ke Telegram, dapat %d", hits)
	}
}

// Penolakan Telegram diteruskan: status balasan apa adanya + deskripsi dari body JSON.
func TestSendReportsTelegramRejection(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":400,"description":"Bad Request: chat not found"}`))
	}))
	defer ts.Close()

	status, err := configuredClient(ts.URL, "123:TOKEN", "42", ts.Client()).Send(context.Background(), "halo")
	if status != http.StatusBadRequest {
		t.Fatalf("status balasan Telegram harus diteruskan, dapat %d", status)
	}
	if err == nil || !strings.Contains(err.Error(), "chat not found") {
		t.Fatalf("error harus memuat deskripsi Telegram, dapat %v", err)
	}
}

// Klien bawaan (tanpa HTTP/Base) memakai alamat resmi Telegram dan punya timeout sendiri.
func TestNewUsesTelegramBaseAndTrims(t *testing.T) {
	client := New("  123:TOKEN\n", " -1001 ")
	if client.Token != "123:TOKEN" || client.ChatID != "-1001" {
		t.Fatalf("token/chat id harus dipangkas: %+v", client)
	}
	if client.baseURL() != DefaultBaseURL {
		t.Fatalf("base bawaan harus %s, dapat %s", DefaultBaseURL, client.baseURL())
	}
	if client.httpClient().Timeout != sendTimeout {
		t.Fatalf("timeout bawaan harus %s, dapat %s", sendTimeout, client.httpClient().Timeout)
	}
	if (&Client{Token: "x", ChatID: "y", Base: "https://contoh.test/"}).baseURL() != "https://contoh.test" {
		t.Fatal("base pengganti harus dipangkas garis miring di ujung")
	}
}
