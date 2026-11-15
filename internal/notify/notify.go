// Package notify mengirim notifikasi singkat ke Telegram lewat HTTP API resmi
// (POST /bot<token>/sendMessage). Sengaja memakai net/http biasa supaya tidak ada dependensi
// baru, dan seluruh pengiriman bersifat best-effort: kegagalan kirim tidak pernah dianggap
// kegagalan pekerjaan pemanggil (pipeline harian / cek lokal 6 jam).
package notify

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DefaultBaseURL adalah alamat API Telegram. Dipisah jadi field Client supaya tes bisa
// menunjuk server stub dan tidak pernah mengirim pesan sungguhan.
const DefaultBaseURL = "https://api.telegram.org"

// sendTimeout membatasi satu pengiriman: Telegram yang lambat tidak boleh menahan pipeline
// (langkah pipeline punya batas waktu sendiri, tapi notifikasi harus selesai jauh lebih cepat).
const sendTimeout = 15 * time.Second

// maxReplyBytes adalah batas pembacaan balasan Telegram (deskripsi error saja yang dipakai).
const maxReplyBytes = 4096

// Client mengirim pesan teks ke satu chat Telegram. Nilai nol (atau token/chat id kosong)
// berarti notifikasi tidak aktif: Configured() false dan Send() menolak tanpa menembak jaringan.
type Client struct {
	Token  string
	ChatID string
	// Base adalah alamat API pengganti (kosong = DefaultBaseURL); hanya diisi tes.
	Base string
	// HTTP adalah klien pengganti (kosong = klien dengan timeout sendTimeout); hanya diisi tes.
	HTTP *http.Client
}

// New membuat client dari setelan pengguna; spasi dibuang supaya token hasil salin-tempel tidak
// gagal hanya karena spasi/newline di ujung.
func New(token, chatID string) *Client {
	return &Client{Token: strings.TrimSpace(token), ChatID: strings.TrimSpace(chatID)}
}

// Configured melaporkan apakah notifikasi boleh dijalankan: butuh token DAN chat id.
func (c *Client) Configured() bool {
	return c != nil && strings.TrimSpace(c.Token) != "" && strings.TrimSpace(c.ChatID) != ""
}

func (c *Client) baseURL() string {
	if c != nil && strings.TrimSpace(c.Base) != "" {
		return strings.TrimRight(strings.TrimSpace(c.Base), "/")
	}
	return DefaultBaseURL
}

func (c *Client) httpClient() *http.Client {
	if c != nil && c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: sendTimeout}
}

// Send mengirim `text` ke chat yang dikonfigurasi. Balikan pertama adalah status HTTP balasan
// Telegram (0 kalau request tidak sampai sama sekali); error berisi deskripsi Telegram atau
// kegagalan jaringan. Body dikirim form-urlencoded: chat_id, text, disable_web_page_preview.
func (c *Client) Send(ctx context.Context, text string) (int, error) {
	if !c.Configured() {
		return 0, errors.New("token bot dan chat id Telegram belum diisi")
	}
	form := url.Values{}
	form.Set("chat_id", strings.TrimSpace(c.ChatID))
	form.Set("text", text)
	form.Set("disable_web_page_preview", "true")

	endpoint := c.baseURL() + "/bot" + strings.TrimSpace(c.Token) + "/sendMessage"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxReplyBytes))
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, fmt.Errorf("Telegram menolak (%d): %s", resp.StatusCode, replyDescription(body))
	}
	return resp.StatusCode, nil
}

// replyDescription mengambil field "description" dari balasan JSON Telegram; kalau tidak ada,
// potongan body dipakai apa adanya supaya penyebabnya tetap terlihat di log/langkah pipeline.
func replyDescription(body []byte) string {
	var parsed struct {
		Description string `json:"description"`
	}
	if err := json.Unmarshal(body, &parsed); err == nil && strings.TrimSpace(parsed.Description) != "" {
		return strings.TrimSpace(parsed.Description)
	}
	if text := strings.TrimSpace(string(body)); text != "" {
		return text
	}
	return "(tanpa keterangan)"
}

// DailyMessage menyusun pesan setelah langkah "scrape sumber & verifikasi" pada pipeline penuh
// selesai. `counts` adalah Counts langkah itu; `pushed` jumlah baris yang dikirim langkah push
// (< 0 = langkah push dilewati, ditulis "-"); `dur` lama langkah scrape.
func DailyMessage(counts map[string]int, pushed int, dur time.Duration) string {
	pool := "-"
	if pushed >= 0 {
		pool = strconv.Itoa(pushed)
	}
	var b strings.Builder
	b.WriteString("Pan-VlessCloudflare - scrape harian\n")
	fmt.Fprintf(&b, "Kandidat ditarik: %s\n", countText(counts, "kandidat"))
	fmt.Fprintf(&b, "Hidup: %s\n", countText(counts, "hidup"))
	fmt.Fprintf(&b, "Mati: %s\n", countText(counts, "mati"))
	fmt.Fprintf(&b, "Dikirim ke pool: %s\n", pool)
	fmt.Fprintf(&b, "Waktu: selesai dalam %s", durationText(dur))
	return b.String()
}

// LocalMessage menyusun pesan setelah langkah "cek lokal (refresh + target negara)" pada jadwal
// 6 jam selesai. Angka mati dihitung dari selisih diperiksa - hidup kalau Counts langkah tidak
// memuatnya.
func LocalMessage(counts map[string]int) string {
	var b strings.Builder
	b.WriteString("Pan-VlessCloudflare - cek 6 jam\n")
	fmt.Fprintf(&b, "Diperiksa ulang: %s\n", countText(counts, "diperiksa"))
	fmt.Fprintf(&b, "Masih hidup: %s\n", countText(counts, "hidup"))
	fmt.Fprintf(&b, "Sudah mati: %s\n", deadText(counts))
	return strings.TrimRight(b.String(), "\n")
}

// countText menampilkan angka Counts; kunci yang tidak ada ditulis "-" (bukan 0) supaya
// pembaca tidak menyimpulkan "nol" padahal langkahnya tidak melaporkan angka itu.
func countText(counts map[string]int, key string) string {
	value, ok := counts[key]
	if !ok {
		return "-"
	}
	return strconv.Itoa(value)
}

// deadText memakai angka "mati" bila ada; kalau tidak, dihitung dari diperiksa - hidup.
func deadText(counts map[string]int) string {
	if value, ok := counts["mati"]; ok {
		return strconv.Itoa(value)
	}
	checked, okChecked := counts["diperiksa"]
	alive, okAlive := counts["hidup"]
	if !okChecked || !okAlive {
		return "-"
	}
	if checked < alive {
		return "0"
	}
	return strconv.Itoa(checked - alive)
}

// durationText menulis lama langkah dalam menit; di bawah satu menit dibulatkan ke bawah supaya
// "0 menit" tidak muncul untuk langkah yang jelas berjalan.
func durationText(d time.Duration) string {
	if d < time.Minute {
		return "kurang dari 1 menit"
	}
	return strconv.Itoa(int(d.Minutes()+0.5)) + " menit"
}
