// Package supabase berisi klien tipis untuk Supabase REST (PostgREST) dengan net/http
// saja — dipakai untuk menyinkronkan hasil scraping ke tabel yang dibaca panel CFVPN Hub,
// dan untuk menarik baris lama supaya bisa diverifikasi ulang di sini (bukan di browser).
package supabase

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Row adalah satu baris tabel proxy. Field selalu dikirim lengkap (tanpa omitempty) supaya
// semua objek dalam satu batch upsert punya kunci yang sama — PostgREST menolak batch yang
// objeknya tidak seragam (PGRST102 "All object keys must match").
type Row struct {
	ID          string `json:"id,omitempty"`
	IP          string `json:"ip"`
	Port        string `json:"port"`
	Country     string `json:"country"`
	Org         string `json:"org"`
	IsActive    bool   `json:"is_active"`
	LastChecked string `json:"last_checked"`
	Ping        int    `json:"ping"`
	// Bukti verifikasi (kolom opsional; lihat docs/supabase-policies.sql). Tidak dikirim
	// selama kolomnya belum ada di tabel — kalau dikirim, PostgREST membalas 400 dan
	// seluruh push gagal. Karena itu pointer: "tidak dikirim" dan "false" berbeda.
	TunnelOK *bool   `json:"tunnel_ok,omitempty"` // verdict edge dari cron panel (hanya dibaca)
	TLSOK    *bool   `json:"tls_ok,omitempty"`
	AltOK    *bool   `json:"alt_ok,omitempty"`
	ExitIP   *string `json:"exit_ip,omitempty"`
}

// EvidenceRow melengkapi baris dengan bukti verifikasi (dipakai kalau kolomnya sudah ada).
func (r Row) EvidenceRow(tlsOK, altOK bool, exitIP string) Row {
	r.TLSOK, r.AltOK = &tlsOK, &altOK
	if exitIP != "" {
		r.ExitIP = &exitIP
	}
	return r
}

// Client mengakses satu tabel di Supabase.
type Client struct {
	BaseURL string
	Key     string
	Table   string
	http    *http.Client

	mu           sync.Mutex
	evidenceCols int // 0 = belum diketahui, 1 = ada, -1 = tidak ada
}

// New membuat klien. `table` default "proxy_pool" (tabel yang dipakai panel).
func New(baseURL, key, table string) *Client {
	if strings.TrimSpace(table) == "" {
		table = "proxy_pool"
	}
	return &Client{
		BaseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		Key:     strings.TrimSpace(key),
		Table:   strings.TrimSpace(table),
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// Configured menandakan kredensial sudah diisi.
func (c *Client) Configured() bool { return c != nil && c.BaseURL != "" && c.Key != "" }

func (c *Client) endpoint(query string) string {
	return fmt.Sprintf("%s/rest/v1/%s%s", c.BaseURL, c.Table, query)
}

func (c *Client) headers(extra map[string]string) http.Header {
	h := http.Header{}
	h.Set("apikey", c.Key)
	h.Set("Authorization", "Bearer "+c.Key)
	h.Set("Content-Type", "application/json")
	for k, v := range extra {
		h.Set(k, v)
	}
	return h
}

func (c *Client) do(ctx context.Context, method, url string, body any, extra map[string]string) (*http.Response, []byte, error) {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return nil, nil, err
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, nil, err
	}
	req.Header = c.headers(extra)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return resp, nil, err
	}
	return resp, data, nil
}

func errorFrom(resp *http.Response, data []byte) error {
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	snippet := strings.TrimSpace(string(data))
	if len(snippet) > 200 {
		snippet = snippet[:200]
	}
	return fmt.Errorf("supabase HTTP %d: %s", resp.StatusCode, snippet)
}

// Test memeriksa koneksi dan mengembalikan jumlah baris (total & aktif).
func (c *Client) Test(ctx context.Context) (total, alive int, err error) {
	if !c.Configured() {
		return 0, 0, fmt.Errorf("url atau key Supabase belum diisi")
	}
	resp, _, err := c.do(ctx, http.MethodHead, c.endpoint("?select=id"), nil, map[string]string{"Prefer": "count=exact", "Range": "0-1"})
	if err != nil {
		return 0, 0, err
	}
	total = parseCount(resp.Header.Get("Content-Range"))

	resp, _, err = c.do(ctx, http.MethodHead, c.endpoint("?select=id&is_active=eq.true"), nil, map[string]string{"Prefer": "count=exact", "Range": "0-1"})
	if err != nil {
		return total, 0, err
	}
	alive = parseCount(resp.Header.Get("Content-Range"))
	return total, alive, nil
}

func parseCount(contentRange string) int {
	if contentRange == "" {
		return 0
	}
	_, totalStr, found := strings.Cut(contentRange, "/")
	if !found {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(totalStr))
	if err != nil {
		return 0
	}
	return n
}

// Pull mengambil baris dari tabel. `staleBefore` (opsional, unix detik) hanya mengambil
// yang terakhir dicek sebelum waktu itu, supaya verifikasi berputar dan tidak mengulang
// baris yang baru saja diperiksa.
func (c *Client) Pull(ctx context.Context, limit, offset int, staleBefore int64, onlyAlive bool) ([]Row, error) {
	if !c.Configured() {
		return nil, fmt.Errorf("url atau key Supabase belum diisi")
	}
	query := url.Values{}
	query.Set("select", "id,ip,port,country,is_active,last_checked,ping")
	if onlyAlive {
		query.Set("is_active", "eq.true")
	}
	if staleBefore > 0 {
		query.Set("last_checked", "lt."+time.Unix(staleBefore, 0).UTC().Format(time.RFC3339))
	}
	query.Set("order", "last_checked.asc.nullsfirst")
	query.Set("limit", strconv.Itoa(limit))
	if offset > 0 {
		query.Set("offset", strconv.Itoa(offset))
	}
	resp, data, err := c.do(ctx, http.MethodGet, c.endpoint("?"+query.Encode()), nil, nil)
	if err != nil {
		return nil, err
	}
	if err := errorFrom(resp, data); err != nil {
		return nil, err
	}
	var rows []Row
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, fmt.Errorf("balasan bukan JSON: %w", err)
	}
	return rows, nil
}

// Upsert menyisipkan atau memperbarui baris berdasarkan pasangan (ip, port) — kolom itu
// unik di tabel proxy_pool, jadi aman dipakai berulang.
//
// Kalau baris membawa bukti verifikasi (`tls_ok`/`alt_ok`/`exit_ip`) tetapi kolomnya belum
// ada di tabel, PostgREST membalas 400 dan seluruh push gagal. Karena itu kegagalan seperti
// itu ditangani: kolom bukti dilepas, push diulang, dan hasilnya dicatat supaya percobaan
// berikutnya tidak membuang request. Setelah `docs/supabase-policies.sql` dijalankan,
// kolom bukti langsung ikut terkirim tanpa perlu restart.
func (c *Client) Upsert(ctx context.Context, rows []Row) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	if !c.Configured() {
		return 0, fmt.Errorf("url atau key Supabase belum diisi")
	}
	if c.evidenceState() < 0 {
		rows = stripEvidence(rows)
	}
	sent, err := c.upsertChunks(ctx, rows)
	if err == nil || !hasEvidence(rows) {
		if err == nil {
			c.setEvidenceState(1)
		}
		return sent, err
	}
	c.setEvidenceState(-1)
	sent, err = c.upsertChunks(ctx, stripEvidence(rows))
	return sent, err
}

func (c *Client) upsertChunks(ctx context.Context, rows []Row) (int, error) {
	sent := 0
	for start := 0; start < len(rows); start += 200 {
		end := min(start+200, len(rows))
		chunk := rows[start:end]
		resp, data, err := c.do(ctx, http.MethodPost,
			c.endpoint("?on_conflict=ip,port"), chunk,
			map[string]string{"Prefer": "resolution=merge-duplicates,return=minimal"})
		if err != nil {
			return sent, err
		}
		if err := errorFrom(resp, data); err != nil {
			return sent, err
		}
		sent += len(chunk)
	}
	return sent, nil
}

// EvidenceColumns menandakan kolom bukti (tls_ok/alt_ok/exit_ip) dipakai oleh push terakhir.
func (c *Client) EvidenceColumns() bool { return c.evidenceState() == 1 }

func (c *Client) evidenceState() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evidenceCols
}

func (c *Client) setEvidenceState(state int) {
	c.mu.Lock()
	c.evidenceCols = state
	c.mu.Unlock()
}

func hasEvidence(rows []Row) bool {
	for _, r := range rows {
		if r.TLSOK != nil || r.AltOK != nil || r.ExitIP != nil {
			return true
		}
	}
	return false
}

func stripEvidence(rows []Row) []Row {
	out := make([]Row, len(rows))
	for i, r := range rows {
		r.TLSOK, r.AltOK, r.ExitIP = nil, nil, nil
		out[i] = r
	}
	return out
}

// CountDead menghitung baris mati yang lebih tua dari `before`.
func (c *Client) CountDead(ctx context.Context, before time.Time) (int, error) {
	if !c.Configured() {
		return 0, fmt.Errorf("url atau key Supabase belum diisi")
	}
	query := url.Values{}
	query.Set("select", "id")
	query.Set("is_active", "eq.false")
	query.Set("last_checked", "lt."+before.UTC().Format(time.RFC3339))
	resp, _, err := c.do(ctx, http.MethodHead, c.endpoint("?"+query.Encode()), nil,
		map[string]string{"Prefer": "count=exact", "Range": "0-1"})
	if err != nil {
		return 0, err
	}
	return parseCount(resp.Header.Get("Content-Range")), nil
}

// DeleteDead menghapus baris mati yang terakhir dicek sebelum `before` (housekeeping).
// PostgREST butuh filter, jadi parameter wajib ada supaya tidak menghapus seluruh tabel.
func (c *Client) DeleteDead(ctx context.Context, before time.Time, limit int) (int, error) {
	if !c.Configured() {
		return 0, fmt.Errorf("url atau key Supabase belum diisi")
	}
	if limit <= 0 {
		limit = 2000
	}
	query := url.Values{}
	query.Set("is_active", "eq.false")
	query.Set("last_checked", "lt."+before.UTC().Format(time.RFC3339))
	query.Set("limit", strconv.Itoa(limit))
	ids, err := c.deadIDs(ctx, query)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	return c.deleteByIDs(ctx, ids)
}

func (c *Client) deadIDs(ctx context.Context, query url.Values) ([]string, error) {
	q := url.Values{}
	for k, v := range query {
		q[k] = v
	}
	q.Set("select", "id")
	resp, data, err := c.do(ctx, http.MethodGet, c.endpoint("?"+q.Encode()), nil, nil)
	if err != nil {
		return nil, err
	}
	if err := errorFrom(resp, data); err != nil {
		return nil, err
	}
	var rows []Row
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.ID != "" {
			ids = append(ids, row.ID)
		}
	}
	return ids, nil
}

func (c *Client) deleteByIDs(ctx context.Context, ids []string) (int, error) {
	deleted := 0
	for start := 0; start < len(ids); start += 200 {
		end := min(start+200, len(ids))
		chunk := ids[start:end]
		query := url.Values{}
		query.Set("id", "in.("+strings.Join(chunk, ",")+")")
		// return=representation: PostgREST mengembalikan baris yang BENAR-BENAR terhapus.
		// Kalau RLS menolak DELETE, balasannya array kosong — jadi hitungannya tidak bohong.
		resp, data, err := c.do(ctx, http.MethodDelete, c.endpoint("?"+query.Encode()), nil,
			map[string]string{"Prefer": "return=representation"})
		if err != nil {
			return deleted, err
		}
		if err := errorFrom(resp, data); err != nil {
			return deleted, err
		}
		var rows []Row
		if err := json.Unmarshal(data, &rows); err != nil {
			return deleted, fmt.Errorf("balasan hapus bukan JSON: %w", err)
		}
		deleted += len(rows)
	}
	return deleted, nil
}

// MarkDead menandai banyak baris sebagai mati dalam satu request per potongan
// (`id=in.(...)`) — jauh lebih hemat daripada PATCH per baris.
func (c *Client) MarkDead(ctx context.Context, ids []string, when time.Time) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	if !c.Configured() {
		return 0, fmt.Errorf("url atau key Supabase belum diisi")
	}
	body := map[string]any{"is_active": false, "ping": nil, "last_checked": when.UTC().Format(time.RFC3339)}
	updated := 0
	for start := 0; start < len(ids); start += 200 {
		end := min(start+200, len(ids))
		chunk := ids[start:end]
		query := url.Values{}
		query.Set("id", "in.("+strings.Join(chunk, ",")+")")
		resp, data, err := c.do(ctx, http.MethodPatch, c.endpoint("?"+query.Encode()), body,
			map[string]string{"Prefer": "return=minimal"})
		if err != nil {
			return updated, err
		}
		if err := errorFrom(resp, data); err != nil {
			return updated, err
		}
		updated += len(chunk)
	}
	return updated, nil
}

// UpdateVerdict menulis hasil verifikasi satu baris (jalur lambat; diutamakan MarkDead +
// Upsert supaya hemat request).
func (c *Client) UpdateVerdict(ctx context.Context, id string, alive bool, pingMS int, when time.Time) error {
	if !c.Configured() {
		return fmt.Errorf("url atau key Supabase belum diisi")
	}
	body := map[string]any{"is_active": alive, "last_checked": when.UTC().Format(time.RFC3339), "ping": nil}
	if alive {
		body["ping"] = pingMS
	}
	query := url.Values{}
	query.Set("id", "eq."+id)
	resp, data, err := c.do(ctx, http.MethodPatch, c.endpoint("?"+query.Encode()), body,
		map[string]string{"Prefer": "return=minimal"})
	if err != nil {
		return err
	}
	return errorFrom(resp, data)
}

// PullEdgeVerdicts mengambil verdict probe edge (kolom tunnel_ok) yang sudah tercatat.
// Dipakai push: baris yang TERBUKTI MATI dari edge tidak boleh dinaikkan lagi ke is_active=true
// hanya karena verifikasi lokal menganggapnya hidup (vantage lokal != vantage Cloudflare).
func (c *Client) PullEdgeVerdicts(ctx context.Context, limit int) (map[string]bool, error) {
	if !c.Configured() {
		return nil, fmt.Errorf("url atau key Supabase belum diisi")
	}
	if limit <= 0 {
		limit = 5000
	}
	query := url.Values{}
	query.Set("select", "ip,port,tunnel_ok")
	query.Set("tunnel_ok", "not.is.null")
	query.Set("limit", strconv.Itoa(limit))
	resp, body, err := c.do(ctx, http.MethodGet, c.endpoint("?"+query.Encode()), nil, nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pull verdict HTTP %d", resp.StatusCode)
	}
	var rows []Row
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(rows))
	for _, row := range rows {
		if row.TunnelOK != nil {
			out[row.IP+":"+row.Port] = *row.TunnelOK
		}
	}
	return out, nil
}
