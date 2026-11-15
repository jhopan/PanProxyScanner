// Package api menyediakan website + API HTTP untuk daftar proxy hasil scraping.
package api

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	neturl "net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jhopan/Pan-ProxyScanner/internal/auth"
	"github.com/jhopan/Pan-ProxyScanner/internal/notify"
	"github.com/jhopan/Pan-ProxyScanner/internal/scraper"
	"github.com/jhopan/Pan-ProxyScanner/internal/settings"
	"github.com/jhopan/Pan-ProxyScanner/internal/store"
	"github.com/jhopan/Pan-ProxyScanner/internal/supabase"
)

//go:embed web/index.html
var indexHTML []byte

//go:embed web/login.html
var loginHTML []byte

// Server membungkus store + runner + penjadwal di balik HTTP.
type Server struct {
	store    *store.Store
	runner   *scraper.Runner
	sched    *scraper.Scheduler
	settings *settings.Settings
	sessions *auth.Manager
	cfgPath  string
	addr     string
	envPath  string
	mu       sync.Mutex // melindungi settings

	// stateMu melindungi lastSync/remote; syncRun memastikan hanya satu operasi
	// sinkronisasi Supabase berjalan (jangan digabung: goroutine sinkronisasi memanggil
	// recordSync yang butuh stateMu, kalau satu mutex keduanya deadlock).
	stateMu  sync.Mutex
	syncRun  sync.Mutex
	lastSync map[string]any
	remote   remoteCount

	// pipelineMu melindungi status pipeline; pipelineRun memastikan satu pipeline berjalan.
	pipelineMu  sync.Mutex
	pipelineRun sync.Mutex
	pipeline    PipelineStatus

	// tunnelMu melindungi kondisi tunnel (tunnelstate.go): hasil uji jalur publik disegarkan
	// di latar belakang supaya handler /api/tunnel tidak ikut menunggu jaringan.
	tunnelMu      sync.Mutex
	tunnelState   tunnelState
	tunnelStateAt time.Time
	tunnelProbing bool

	// purgeMu melindungi purgeHits: jatah POST /api/pool-purge per IP. Endpoint itu
	// memverifikasi hash lewat RPC whoami, jadi tanpa pagar ia menjadi oracle hash tanpa batas.
	purgeMu   sync.Mutex
	purgeHits map[string]purgeHit

	// dataDir adalah folder data scanner (folder berkas DB SQLite); backup state ditulis ke
	// <dataDir>/backups. Kosong berarti folder kerja proses.
	dataDir string
	// writeKeyOnce memastikan catatan "operasi tulis jatuh ke anon key" hanya masuk log sekali
	// per proses — catatan yang sama muncul lagi di ringkasan sinkron tiap operasi tulis.
	writeKeyOnce sync.Once
	// notifyClient membuat klien Telegram; kosong = notify.New. Hanya diisi tes supaya endpoint
	// /api/notify-test dan notifikasi pipeline bisa diarahkan ke server stub (tidak pernah
	// mengirim pesan sungguhan).
	notifyClient func(token, chatID string) *notify.Client
}

// New membuat server. `cfgPath` adalah lokasi settings.json (dipakai saat menyimpan).
func New(st *store.Store, runner *scraper.Runner, sched *scraper.Scheduler, cfg *settings.Settings, cfgPath, addr string) *Server {
	return &Server{store: st, runner: runner, sched: sched, settings: cfg, sessions: auth.New(), cfgPath: cfgPath, addr: addr}
}

// SetEnvPath mencatat berkas .env yang dibaca saat startup (ditampilkan di halaman Tunnel).
func (s *Server) SetEnvPath(path string) { s.envPath = path }

// SetDataDir mencatat folder data scanner (folder berkas DB SQLite). Backup state ditulis ke
// <dir>/backups; tanpa ini berkasnya jatuh ke folder kerja proses.
func (s *Server) SetDataDir(dir string) { s.dataDir = dir }

// Handler mengembalikan router lengkap.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/login", s.handleLoginPage)
	mux.HandleFunc("/api/login", s.handleLogin)
	mux.HandleFunc("/api/logout", s.handleLogout)
	mux.HandleFunc("/api/me", s.handleMe)
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/proxies", s.handleProxies)
	mux.HandleFunc("/api/proxies.txt", s.handleProxiesText)
	mux.HandleFunc("/api/countries", s.handleCountries)
	mux.HandleFunc("/api/sources", s.handleSources)
	mux.HandleFunc("/api/sources/toggle", s.handleSourceToggle)
	mux.HandleFunc("/api/settings", s.handleSettings)
	mux.HandleFunc("/api/auto/start", s.handleAutoStart)
	mux.HandleFunc("/api/auto/stop", s.handleAutoStop)
	mux.HandleFunc("/api/now", s.handleNow)
	mux.HandleFunc("/api/purge", s.handlePurge)
	mux.HandleFunc("/api/scrape", s.handleScrape)
	mux.HandleFunc("/api/stop", s.handleStop)
	mux.HandleFunc("/api/tunnel", s.handleTunnel)
	mux.HandleFunc("/api/pipeline", s.handlePipeline)
	mux.HandleFunc("/api/pipeline/run", s.handlePipelineRun)
	mux.HandleFunc("/api/notify-test", s.handleNotifyTest)
	mux.HandleFunc("/api/supabase/status", s.handleSupabaseStatus)
	mux.HandleFunc("/api/supabase/test", s.handleSupabaseTest)
	mux.HandleFunc("/api/supabase/push", s.handleSupabasePush)
	mux.HandleFunc("/api/supabase/pull-verify", s.handleSupabasePullVerify)
	mux.HandleFunc("/api/supabase/prune", s.handleSupabasePrune)
	mux.HandleFunc("/api/state-backup", s.handleStateBackup)
	mux.HandleFunc("/api/pool-purge", s.handlePoolPurge)
	mux.HandleFunc("/api/vless/clash", s.handleClash)
	mux.HandleFunc("/api/vless/raw", s.handleRaw)
	return s.withCommon(mux)
}

// withCommon menambahkan CORS, lalu mewajibkan login (cookie sesi) atau token API.
//
//   - /login dan /api/login terbuka.
//   - /api/health terbuka supaya bisa dipakai monitoring/tunnel check.
//   - /api/pool-purge terbuka: panel CFVPN Hub memverifikasi dirinya sendiri lewat body
//     (username + hash → RPC whoami) dan DELETE-nya memakai service key di sisi scanner.
//   - rute lain: sesi valid ATAU token API (kalau token diatur di pengaturan) — token
//     dipakai klien mesin seperti panel CFVPN Hub yang tidak bisa login lewat browser.
func (s *Server) withCommon(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,X-Api-Token")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		path := r.URL.Path
		public := path == "/login" || path == "/api/login" || path == "/api/health" || path == "/api/pool-purge"
		if public {
			next.ServeHTTP(w, r)
			return
		}

		if _, ok := s.sessions.UserFromRequest(r); ok {
			next.ServeHTTP(w, r)
			return
		}

		if token := s.currentToken(); token != "" {
			given := r.Header.Get("X-Api-Token")
			if given == "" {
				given = r.URL.Query().Get("token")
			}
			if given == token {
				next.ServeHTTP(w, r)
				return
			}
		}

		if strings.HasPrefix(path, "/api/") {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "belum login (POST /api/login) atau token API salah"})
			return
		}
		http.Redirect(w, r, "/login", http.StatusFound)
	})
}

// handleLoginPage menyajikan halaman login; pengguna yang sudah login langsung diarahkan.
func (s *Server) handleLoginPage(w http.ResponseWriter, r *http.Request) {
	if _, ok := s.sessions.UserFromRequest(r); ok {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(loginHTML)
}

// handleLogin memeriksa kredensial dan menulis cookie sesi.
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	var body struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body JSON tidak valid"})
		return
	}
	cfg := s.currentSettings()
	token, ok := s.sessions.Login(body.Username, body.Password, cfg.AdminUser, cfg.AdminPasswordHash, settings.HashPassword)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "username atau password salah"})
		return
	}
	auth.SetCookie(w, r, token)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "user": cfg.AdminUser})
}

// handleLogout menghapus sesi.
func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	if cookie, err := r.Cookie(auth.CookieName); err == nil {
		s.sessions.Logout(cookie.Value)
	}
	auth.ClearCookie(w, r)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleMe memberi tahu status login (dipakai shell website).
func (s *Server) handleMe(w http.ResponseWriter, r *http.Request) {
	user, ok := s.sessions.UserFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"logged_in": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"logged_in": true, "user": user})
}

// ---------------------------------------------------------------- Supabase (sinkron proxy_pool)

func (s *Server) supabaseClient() *supabase.Client {
	cfg := s.currentSettings()
	return supabase.New(cfg.SupabaseURL, cfg.SupabaseKey, cfg.SupabaseTable)
}

// writeAnonNote adalah catatan tunggal saat `supabase_service_key` kosong: operasi tulis
// jatuh ke anon key, yang setelah cfvpn-hub/sql/2026-11-15-pool-write-lockdown.sql tidak lagi
// punya hak tulis pada proxy_pool (Supabase akan menolaknya). Baca tetap lewat anon seperti biasa.
const writeAnonNote = "supabase_service_key belum diset: operasi tulis jatuh ke anon key, sementara anon sudah tidak punya hak tulis di proxy_pool"

// supabaseClients membungkus pilihan klien Supabase untuk jalur sinkronisasi: baca selalu
// memakai anon key (pool memang boleh dibaca anon), tulis memakai `supabase_service_key` bila
// terisi. Struktur ini dipakai supaya tidak ada jalur tulis yang diam-diam memakai anon.
type supabaseClients struct {
	read  *supabase.Client
	write *supabase.Client
	// note berisi catatan pemilihan key saat tulis jatuh ke anon (kosong kalau service key
	// terisi); nilainya ikut ke ringkasan sinkron dan balasan endpoint.
	note string
}

// supabaseClients merakit klien baca + tulis dari pengaturan saat ini. Catatan fallback ditulis
// ke log hanya sekali per proses supaya log tidak banjir saat pipeline memanggil berkali-kali.
func (s *Server) supabaseClients() supabaseClients {
	cfg := s.currentSettings()
	write, usedService := supabase.NewForWrite(cfg.SupabaseURL, cfg.SupabaseKey, cfg.SupabaseServiceKey, cfg.SupabaseTable)
	out := supabaseClients{
		read:  supabase.New(cfg.SupabaseURL, cfg.SupabaseKey, cfg.SupabaseTable),
		write: write,
	}
	// Catatan fallback hanya berguna kalau Supabase memang dikonfigurasi: tanpa URL/anon key
	// tidak ada request yang dikirim, jadi jangan menakuti log dengan peringatan key kosong.
	if !usedService && out.read.Configured() {
		out.note = writeAnonNote
		s.writeKeyOnce.Do(func() { log.Printf("supabase: %s", writeAnonNote) })
	}
	return out
}

// writeKeySummary melaporkan key yang dipakai operasi tulis + catatannya (dipakai endpoint
// status supaya pemilihan key bisa diperiksa tanpa memicu operasi tulis).
func writeKeySummary(cfg settings.Settings) (key, note string) {
	if strings.TrimSpace(cfg.SupabaseServiceKey) != "" {
		return "service_key", ""
	}
	return "anon_key", writeAnonNote
}

// appendNote menambahkan catatan key ke sebuah catatan langkah/ringkasan (no-op kalau kosong).
func appendNote(note, extra string) string {
	if extra == "" {
		return note
	}
	if note == "" {
		return extra
	}
	return note + "; " + extra
}

// supabaseServiceClient membuat klien dengan `supabase_service_key` untuk operasi yang memang
// butuh kunci istimewa (hapus baris mati, RPC retensi deploy_log, backup `user_states`) —
// kuncinya tidak pernah dikirim ke klien dan tidak pernah dipakai untuk operasi yang bisa anon.
func (s *Server) supabaseServiceClient() *supabase.Client {
	cfg := s.currentSettings()
	return supabase.New(cfg.SupabaseURL, cfg.SupabaseServiceKey, cfg.SupabaseTable)
}

// pruneNoServiceKeyNote adalah pesan tunggal untuk semua jalur prune saat service key kosong:
// bukan kegagalan, hanya tidak ada yang bisa dihapus dengan anon key. (Operasi tulis lain —
// push/MarkDead/UpdateVerdict — tetap jalan dengan anon + catatan; DELETE tidak, karena anon
// sudah dicabut hak hapus pada proxy_pool.)
const pruneNoServiceKeyNote = "prune butuh supabase_service_key (service_role key) — tanpa key itu Supabase menolak DELETE ke proxy_pool; operasi tulis lain mengambil jalur anon dengan catatan di ringkasan sinkron"

// handleSupabaseStatus melaporkan konfigurasi + hasil sinkron terakhir. Panggilan cek
// ke Supabase hanya dilakukan kalau diminta (?test=1) supaya endpoint ini selalu cepat —
// pemanggilan langsung ke Supabase bisa lambat/terbatas rate.
func (s *Server) handleSupabaseStatus(w http.ResponseWriter, r *http.Request) {
	cfg := s.currentSettings()
	client := s.supabaseClient()
	writeKey, writeNote := writeKeySummary(cfg)
	s.stateMu.Lock()
	status := s.lastSync
	s.stateMu.Unlock()
	payload := map[string]any{
		"configured":           client.Configured(),
		"url":                  cfg.SupabaseURL,
		"table":                cfg.SupabaseTable,
		"has_key":              cfg.SupabaseKey != "",
		"has_service_key":      cfg.SupabaseServiceKey != "",
		"write_key":            writeKey,
		"write_note":           writeNote,
		"auto_push":            cfg.SupabaseAutoPush,
		"pull_limit":           cfg.SupabasePullLimit,
		"push_org_cap":         cfg.PushOrgCap,
		"push_country_cap":     cfg.PushCountryCap,
		"prune_days":           cfg.SupabasePruneDays,
		"probe_tls":            cfg.ProbeTLS,
		"auto_disable_sources": cfg.AutoDisableSources,
		"zero_streak_limit":    cfg.ZeroStreakLimit,
		"last_sync":            status,
		"remote_total":         nil,
		"remote_alive":         nil,
	}
	if cached, ok := s.lastRemote(); ok {
		payload["remote_total"] = cached.total
		payload["remote_alive"] = cached.alive
	}
	if r.URL.Query().Get("test") == "1" && client.Configured() {
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		if total, alive, err := client.Test(ctx); err == nil {
			payload["remote_total"] = total
			payload["remote_alive"] = alive
			s.setLastRemote(total, alive)
		} else {
			payload["remote_error"] = err.Error()
		}
	}
	writeJSON(w, http.StatusOK, payload)
}

// handleSupabaseTest menguji koneksi ke tabel Supabase.
func (s *Server) handleSupabaseTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	client := s.supabaseClient()
	if !client.Configured() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "isi URL dan key Supabase dulu (menu Supabase)"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	total, alive, err := client.Test(ctx)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	s.setLastRemote(total, alive)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "table": client.Table, "total": total, "alive": alive})
}

// handleSupabasePush mengirim proxy hidup di database lokal ke tabel Supabase (upsert).
func (s *Server) handleSupabasePush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	clients := s.supabaseClients()
	if !clients.read.Configured() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "isi URL dan key Supabase dulu (menu Supabase)"})
		return
	}
	cfg := s.currentSettings()
	limit := atoiDefault(r.URL.Query().Get("limit"), 500)
	if limit > 5000 {
		limit = 5000
	}
	if !s.syncRun.TryLock() {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "sinkronisasi lain sedang berjalan"})
		return
	}
	defer s.syncRun.Unlock()
	edgeDead, edgeErr := clients.read.PullEdgeVerdicts(r.Context(), 5000)
	if edgeErr != nil {
		edgeDead = nil // tabel tanpa kolom tunnel_ok: lanjut tanpa filter edge
	}
	pushed, skippedEdge, err := s.pushToSupabase(r.Context(), clients, cfg, limit, edgeDead, time.Time{})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	payload := map[string]any{"pushed": pushed, "held_edge_dead": skippedEdge, "table": clients.write.Table}
	if clients.note != "" {
		payload["note"] = clients.note
	}
	writeJSON(w, http.StatusOK, payload)
}

// handleSupabasePrune menghapus baris mati lama di Supabase (housekeeping).
func (s *Server) handleSupabasePrune(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	client := s.supabaseClient()
	if !client.Configured() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "isi URL dan key Supabase dulu (menu Supabase)"})
		return
	}
	cfg := s.currentSettings()
	days := atoiDefault(r.URL.Query().Get("days"), cfg.SupabasePruneDays)
	if days <= 0 {
		days = 7
	}
	limit := atoiDefault(r.URL.Query().Get("limit"), 2000)

	if !s.syncRun.TryLock() {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "sinkronisasi lain sedang berjalan"})
		return
	}
	defer s.syncRun.Unlock()

	// Tanpa service key tidak ada gunanya menembak Supabase: jawab jelas (bukan 5xx)
	// bahwa prune butuh `supabase_service_key`, dan jangan kirim request apa pun.
	// (anon sudah tidak punya hak DELETE sejak sql/2026-11-15-pool-write-lockdown.sql, jadi
	// fallback anon di sini hanya akan jadi request sia-sia yang pasti ditolak.)
	deleteClient := s.supabaseServiceClient()
	if !deleteClient.Configured() {
		// Ringkasan sinkron ikut menyebut kenapa tidak ada yang dihapus, supaya catatan key
		// yang dipakai operasi tulis bisa ditelusuri dari GET /api/supabase/status.
		s.recordSync("prune dilewati: "+pruneNoServiceKeyNote, nil)
		writeJSON(w, http.StatusOK, map[string]any{
			"deleted": 0, "older_than_days": days, "kandidat_sebelumnya": 0,
			"note": pruneNoServiceKeyNote,
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	candidates, _ := client.CountDead(ctx, time.Now().Add(-time.Duration(days)*24*time.Hour))
	deleted, err := s.pruneSupabase(ctx, deleteClient, days, limit)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	payload := map[string]any{
		"deleted": deleted, "older_than_days": days, "kandidat_sebelumnya": candidates,
		"note": "jalankan berulang sampai deleted 0 kalau kandidat lebih banyak dari limit",
	}
	if candidates > 0 && deleted == 0 {
		payload["note"] = "0 baris terhapus padahal ada kandidat — pastikan supabase_service_key berisi service_role key (bukan anon), dan policy DELETE untuk service_role sudah ada: jalankan sql/2026-11-15-pool-prune-service-key.sql di panel"
	}
	writeJSON(w, http.StatusOK, payload)
}

// handleSupabasePullVerify menarik baris dari Supabase, memverifikasi satu per satu di sini,
// lalu menulis hasilnya kembali (is_active, ping, last_checked).
func (s *Server) handleSupabasePullVerify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	clients := s.supabaseClients()
	if !clients.read.Configured() {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "isi URL dan key Supabase dulu (menu Supabase)"})
		return
	}
	cfg := s.currentSettings()
	limit := atoiDefault(r.URL.Query().Get("limit"), 50)
	if limit > 2000 {
		limit = 2000
	}
	opts := scraper.OptionsFromSettings(cfg)
	if jobs := atoiDefault(r.URL.Query().Get("jobs"), 0); jobs > 0 {
		opts.Jobs = jobs
	}
	if !s.syncRun.TryLock() {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "sinkronisasi lain sedang berjalan"})
		return
	}
	defer s.syncRun.Unlock()
	summary, err := s.pullVerify(r.Context(), clients, opts, limit)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	// Bentuk balasan dipertahankan (ringkasan apa adanya) supaya klien panel yang sudah ada
	// tidak perlu berubah; pemilihan key tercatat di ringkasan sinkron + log.
	writeJSON(w, http.StatusOK, summary)
}

// ---------------------------------------------------------------- purge proxy_pool (panel CFVPN Hub)
// poolPurgeRateLimit / poolPurgeRateWindow membatasi percobaan POST /api/pool-purge per IP.
// Verifikasi hash di endpoint ini memanggil RPC whoami, jadi tanpa pagar ia menjadi oracle
// hash tanpa batas. Limiter in-memory per proses cukup karena scanner hanya satu instance.
const (
	poolPurgeRateLimit  = 6
	poolPurgeRateWindow = 10 * time.Minute
)

// poolPurgeMissingRPCNote dipakai saat RPC whoami belum ada: itu sinyal migrasi SQL belum
// dijalankan, bukan hash salah, dan endpoint destruktif tidak boleh fail-open.
const poolPurgeMissingRPCNote = "RPC whoami belum ada di database. Jalankan sql/2026-11-14-login-lockout.sql dulu, lalu coba lagi."

// purgeHit adalah jatah percobaan satu IP dalam satu jendela waktu.
type purgeHit struct {
	count int
	reset time.Time
}

// purgeGate mencatat satu percobaan untuk `ip` dan memberi tahu berapa lama pemanggil harus
// menunggu kalau jatahnya habis. Entri kedaluwarsa dibuang tiap panggilan supaya map tidak
// tumbuh tanpa batas.
func (s *Server) purgeGate(ip string) (retryAfter time.Duration, limited bool) {
	now := time.Now()
	s.purgeMu.Lock()
	defer s.purgeMu.Unlock()
	if s.purgeHits == nil {
		s.purgeHits = map[string]purgeHit{}
	}
	for key, hit := range s.purgeHits {
		if !now.Before(hit.reset) {
			delete(s.purgeHits, key)
		}
	}
	hit, ok := s.purgeHits[ip]
	if !ok {
		s.purgeHits[ip] = purgeHit{count: 1, reset: now.Add(poolPurgeRateWindow)}
		return 0, false
	}
	if hit.count >= poolPurgeRateLimit {
		return time.Until(hit.reset), true
	}
	hit.count++
	s.purgeHits[ip] = hit
	return 0, false
}

// clientIP mengambil alamat pemanggil: Cloudflare mengisi CF-Connecting-IP, proxy lain
// X-Forwarded-For (hop pertama), dan uji lokal RemoteAddr.
func clientIP(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("CF-Connecting-IP")); v != "" {
		return v
	}
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		if first, _, ok := strings.Cut(v, ","); ok {
			v = first
		}
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// poolPurgeHTTP adalah klien untuk panggilan langsung ke PostgREST: purge butuh RPC (anon
// key) dan DELETE (service key), sedangkan supabase.Client hanya membungkus satu tabel
// dengan satu key.
var poolPurgeHTTP = &http.Client{Timeout: 20 * time.Second}

// supabaseBaseURL menormalkan URL project (tanpa garis miring di ujung).
func supabaseBaseURL(cfg settings.Settings) string {
	return strings.TrimRight(strings.TrimSpace(cfg.SupabaseURL), "/")
}

// postgrestCode mengambil kode error PostgREST/SQLSTATE dari badan balasan.
func postgrestCode(data []byte) string {
	var body struct {
		Code string `json:"code"`
	}
	if json.Unmarshal(data, &body) == nil {
		return body.Code
	}
	return ""
}

// poolWhoamiRole memanggil RPC whoami lewat PostgREST dengan anon key. Role kosong berarti
// hash tidak cocok / user tidak ada; missing berarti fungsi belum ada di database.
func poolWhoamiRole(ctx context.Context, base, anonKey, username, hash string) (role string, missing bool, err error) {
	payload, err := json.Marshal(map[string]string{"p_username": username, "p_hash": hash})
	if err != nil {
		return "", false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/rest/v1/rpc/whoami", bytes.NewReader(payload))
	if err != nil {
		return "", false, err
	}
	req.Header.Set("apikey", anonKey)
	req.Header.Set("Authorization", "Bearer "+anonKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := poolPurgeHTTP.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", false, err
	}
	if code := postgrestCode(data); code == "PGRST202" || code == "42883" || code == "42P01" {
		return "", true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return "", true, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", false, fmt.Errorf("verifikasi sesi gagal di Supabase: HTTP %d %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	// RPC `returns text`: PostgREST membalas string JSON; sebagian versi membungkusnya array.
	var value any
	if err := json.Unmarshal(data, &value); err != nil {
		return "", false, fmt.Errorf("balasan whoami bukan JSON: %w", err)
	}
	return whoamiRoleOf(value), false, nil
}

// whoamiRoleOf menormalkan balasan whoami (`"admin"`, `["admin"]`, atau null) jadi role.
func whoamiRoleOf(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []any:
		if len(v) > 0 {
			return whoamiRoleOf(v[0])
		}
	}
	return ""
}

// poolDeleteDead menghapus semua baris `is_active = false` memakai service key. Jumlah
// terhapus dibaca dari Content-Range (PostgREST membalas `*/N` untuk DELETE count=exact).
func poolDeleteDead(ctx context.Context, base, serviceKey, table string) (int, error) {
	if strings.TrimSpace(table) == "" {
		table = "proxy_pool"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		base+"/rest/v1/"+neturl.PathEscape(table)+"?is_active=eq.false", nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("apikey", serviceKey)
	req.Header.Set("Authorization", "Bearer "+serviceKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Prefer", "count=exact, return=minimal")
	resp, err := poolPurgeHTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("gagal menghapus baris proxy mati: HTTP %d %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return countFromContentRange(resp.Header.Get("Content-Range")), nil
}

// countFromContentRange membaca N dari `*/N` atau `0-9/N` (0 kalau tidak ada/aneh).
func countFromContentRange(value string) int {
	_, total, ok := strings.Cut(value, "/")
	if !ok || total == "" || total == "*" {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(total))
	if err != nil {
		return 0
	}
	return n
}

// handlePoolPurge menghapus semua baris mati (`is_active = false`) di proxy_pool atas
// permintaan panel CFVPN Hub: POST {username, hash} -> {success, deleted}.
//
// Rute ini publik (tanpa sesi/token scanner) karena panel memverifikasi dirinya sendiri:
// hash sesi panel diperiksa lewat RPC whoami (anon key) dan role wajib `admin`. DELETE-nya
// memakai `supabase_service_key` yang tidak pernah keluar dari mesin scanner.
//
// Fail-closed: tanpa service key, tanpa RPC whoami, atau role bukan admin, tidak ada DELETE.
func (s *Server) handlePoolPurge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	if wait, limited := s.purgeGate(clientIP(r)); limited {
		secs := int(wait.Seconds()) + 1
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		writeJSON(w, http.StatusTooManyRequests, map[string]string{
			"error": fmt.Sprintf("terlalu banyak permintaan. Coba lagi dalam %d detik.", secs),
		})
		return
	}

	cfg := s.currentSettings()
	base := supabaseBaseURL(cfg)
	anonKey := strings.TrimSpace(cfg.SupabaseKey)
	serviceKey := strings.TrimSpace(cfg.SupabaseServiceKey)
	// Service key diperiksa sebelum menyentuh database: tanpa key itu tidak ada jalur sah
	// untuk menghapus, jadi jangan pernah lanjut dengan anon key.
	if serviceKey == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "supabase_service_key belum diset di scanner, jadi tombol bersihkan baris mati dinonaktifkan.",
		})
		return
	}
	if base == "" || anonKey == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "supabase_url/supabase_key belum diset di scanner, jadi sesi panel tidak bisa diverifikasi.",
		})
		return
	}

	var body struct {
		Username string `json:"username"`
		Hash     string `json:"hash"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body JSON tidak valid."})
		return
	}
	username := strings.ToLower(strings.TrimSpace(body.Username))
	if username == "" || strings.ContainsAny(username, " \t\r\n") || body.Hash == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "username/hash tidak valid."})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	role, missing, err := poolWhoamiRole(ctx, base, anonKey, username, body.Hash)
	if missing {
		writeJSON(w, http.StatusConflict, map[string]string{"error": poolPurgeMissingRPCNote})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	if role == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "sesi tidak valid — login ulang."})
		return
	}
	if role != "admin" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "hanya admin yang boleh menghapus baris proxy."})
		return
	}

	deleted, err := poolDeleteDead(ctx, base, serviceKey, cfg.SupabaseTable)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true, "deleted": deleted})
}

// pushMaxAge membatasi umur verdict yang boleh dipakai saat push. `fresh_minutes` (default
// 360) terlalu longgar: proxy yang terakhir dicek 5 jam lalu bisa sudah mati, sedangkan
// panel menyajikannya ke pengguna sebagai proxy aktif. Push hanya boleh memakai verdict
// yang masih segar.
const pushMaxAge = 90 * time.Minute

// pushFreshMinutes mengembalikan jendela kesegaran untuk push: minimal dari setelan pengguna
// dan pushMaxAge.
func pushFreshMinutes(cfg settings.Settings) int {
	limit := int(pushMaxAge.Minutes())
	if cfg.FreshMinutes > 0 && cfg.FreshMinutes < limit {
		return cfg.FreshMinutes
	}
	return limit
}

// exitKey menormalkan alamat keluar untuk dedup: IPv4 dipotong ke /24 (satu penyedia,
// satu subnet), IPv6 dibiarkan utuh, kosong berarti tidak bisa dipakai sebagai kunci.
func exitKey(exitIP string) string {
	ip := net.ParseIP(strings.TrimSpace(exitIP))
	if ip == nil {
		return ""
	}
	if v4 := ip.To4(); v4 != nil {
		return fmt.Sprintf("%d.%d.%d.0/24", v4[0], v4[1], v4[2])
	}
	return ip.String()
}

// hostCheckedAt mengubah waktu cek lokal (unix detik, kolom `last_checked` di database
// scanner) menjadi RFC3339 seperti `last_checked` yang dikirim ke Supabase, jadi
// `tunnel_host_checked` berisi waktu pemeriksaan tunnel itu sendiri — bukan waktu push.
// Nilai yang tidak diketahui (≤ 0) diganti waktu sekarang supaya kolomnya tetap terisi.
func hostCheckedAt(unixSec int64) time.Time {
	if unixSec <= 0 {
		return time.Now().UTC()
	}
	return time.Unix(unixSec, 0).UTC()
}

// pushToSupabase menyinkronkan keputusan verifikasi ke tabel Supabase.
//
// Dua arah, karena `is_active` di tabel itu satu-satunya penentu apa yang dipakai panel:
//   - naikkan: proxy hidup yang **terbukti** sanggup TLS 443 (syarat VLESS) → is_active = true
//   - turunkan: proxy hidup tetapi terbukti tidak sanggup TLS → is_active = false
//
// Tanpa langkah turun, baris lama yang sudah terlanjur aktif (ditandai pengecek HTTP
// dangkal) akan terus dipakai panel dan menghasilkan konfigurasi VLESS yang mati.
// edgeDead: peta "ip:port" -> verdict edge terakhir. Baris yang di edge terbukti mati tidak
// dinaikkan lagi, walau verifikasi lokal menganggapnya hidup.
// since membatasi push ke baris yang dicek setelah waktu itu (push delta). Zero = semua baris
// segar (dipakai pipeline harian supaya Supabase ikut disegarkan menyeluruh).
//
// `clients.write` (service key bila terisi) yang membawa upsert; `clients.read` tetap anon.
func (s *Server) pushToSupabase(ctx context.Context, clients supabaseClients, cfg settings.Settings, limit int, edgeDead map[string]bool, since time.Time) (int, int, error) {
	client := clients.write
	alive := true
	fresh := pushFreshMinutes(cfg)
	promote, err := s.store.List(store.Filter{Alive: &alive, Limit: limit, Order: "score", FreshMinutes: fresh, TLSOnly: cfg.PushRequireTLS, Since: since})
	if err != nil {
		return 0, 0, err
	}
	demoteCap := limit
	if demoteCap < 2000 {
		demoteCap = 2000
	}
	candidates, err := s.store.List(store.Filter{Alive: &alive, Limit: demoteCap, Order: "score", FreshMinutes: fresh, Since: since})
	if err != nil {
		return 0, 0, err
	}

	now := time.Now().UTC().Format(time.RFC3339)
	rows := make([]supabase.Row, 0, len(promote)+len(candidates))
	// minOK: proxy baru naik ke pool setelah minimal dua verifikasi berhasil (bukan sekali
	// hidup lalu mati). seenExit: satu alamat keluar hanya boleh sekali — pool sebaiknya
	// berisi satu orang per IP, bukan lima port dari mesin yang sama.
	const minOK = 2
	seenExit := make(map[string]bool, len(promote))
	// orgCount/countryCount menjaga komposisi pool: tanpa batas ini, satu penyedia (org) atau
	// satu negara bisa menghabiskan seluruh kuota push. Urutan `promote` sudah skor tertinggi
	// lebih dulu, jadi baris yang dipertahankan selalu yang terbaik skornya; sisanya ditahan
	// sampai push berikutnya (barisnya tetap ada di database lokal).
	//
	// `org` kosong tidak dihitung: baris yang penyedianya belum diketahui (mis. hasil scrape
	// yang belum pernah terlihat di tabel Supabase) bukan "satu org yang sama", jadi ia tidak
	// boleh ditahan hanya karena cap. Hal yang sama berlaku untuk negara kosong.
	orgCount := make(map[string]int, len(promote))
	countryCount := make(map[string]int, len(promote))
	skippedYoung, skippedDup, skippedEdge, skippedCap := 0, 0, 0, 0
	promoted := 0
	for _, p := range promote {
		// lewati proxy yang lebih sering gagal daripada berhasil
		if p.FailCount > p.OKCount {
			continue
		}
		if p.OKCount < minOK {
			skippedYoung++
			continue
		}
		// Verdict edge lebih kuat: proxy yang di edge terbukti mati tidak boleh naik lagi.
		if edgeDead[p.IP+":"+strconv.Itoa(p.Port)] {
			skippedEdge++
			continue
		}
		exit := exitKey(p.ExitIP)
		if exit != "" && seenExit[exit] {
			skippedDup++
			continue
		}
		org := strings.TrimSpace(p.Org)
		country := strings.ToUpper(strings.TrimSpace(p.Country))
		if (org != "" && cfg.PushOrgCap > 0 && orgCount[org] >= cfg.PushOrgCap) ||
			(country != "" && cfg.PushCountryCap > 0 && countryCount[country] >= cfg.PushCountryCap) {
			skippedCap++
			continue
		}
		if exit != "" {
			seenExit[exit] = true
		}
		if org != "" {
			orgCount[org]++
		}
		if country != "" {
			countryCount[country]++
		}
		promoted++
		rows = append(rows, supabase.Row{
			IP: p.IP, Port: strconv.Itoa(p.Port), Country: p.Country, Org: org,
			IsActive: true, LastChecked: now, Ping: p.PingMS,
		}.EvidenceRow(p.TLS, p.AltOK, p.ExitIP).
			HostEvidence(p.Alive, p.PingMS, hostCheckedAt(p.LastChecked)))
	}
	demoted := 0
	if cfg.PushRequireTLS {
		for _, p := range candidates {
			if p.TLS {
				continue
			}
			rows = append(rows, supabase.Row{
				IP: p.IP, Port: strconv.Itoa(p.Port), Country: p.Country, Org: strings.TrimSpace(p.Org),
				IsActive: false, LastChecked: now, Ping: p.PingMS,
			}.EvidenceRow(false, p.AltOK, p.ExitIP).
				HostEvidence(p.Alive, p.PingMS, hostCheckedAt(p.LastChecked)))
			demoted++
		}
	}
	if len(rows) == 0 {
		if !since.IsZero() {
			// Push delta: belum ada baris yang berubah sejak push terakhir. Ini hasil normal,
			// bukan kegagalan, jadi status sinkronisasi tidak diisi error.
			s.recordSync("push delta: tidak ada perubahan", nil)
			return 0, skippedEdge, nil
		}
		if cfg.PushRequireTLS {
			return 0, skippedEdge, fmt.Errorf("tidak ada proxy hidup yang lolos uji TLS 443 & segar (matikan push_require_tls kalau ingin mengirim proxy HTTP saja)")
		}
		return 0, skippedEdge, fmt.Errorf("tidak ada proxy hidup & segar di database lokal")
	}
	sent, err := client.Upsert(ctx, rows)
	if err == nil {
		if total, aliveCount, testErr := client.Test(ctx); testErr == nil {
			s.setLastRemote(total, aliveCount)
		}
	}
	// "naik" dihitung dari baris yang benar-benar dikirim sebagai is_active=true (`promoted`),
	// bukan dari selisih daftar kandidat: sekarang ada beberapa saringan (baru 1× berhasil,
	// edge bilang mati, exit IP sama, batas org/negara) dan angka itu harus cocok dengan isi
	// payload.
	note := fmt.Sprintf("push %d baris (naik %d, turun %d, wajib TLS: %v, verdict segar ≤ %d menit",
		sent, promoted, demoted, cfg.PushRequireTLS, fresh)
	if skippedYoung > 0 {
		note += fmt.Sprintf(", %d ditahan (baru 1× berhasil)", skippedYoung)
	}
	if skippedDup > 0 {
		note += fmt.Sprintf(", %d dilewati (exit IP sama)", skippedDup)
	}
	if skippedCap > 0 {
		note += fmt.Sprintf(", %d ditahan (batas org/negara)", skippedCap)
	}
	if skippedEdge > 0 {
		note += fmt.Sprintf(", %d ditahan (edge bilang mati)", skippedEdge)
	}
	if client.EvidenceColumns() {
		note += ", kolom tls_ok/alt_ok/exit_ip ikut dikirim"
	} else {
		note += ", kolom bukti belum ada di tabel → jalankan docs/supabase-policies.sql"
	}
	if client.HostEvidenceColumns() {
		note += ", kolom tunnel_host_ok/tunnel_host_ms/tunnel_host_checked ikut dikirim"
	} else {
		note += ", kolom bukti vantage host belum ada di tabel → jalankan docs/supabase-policies.sql"
	}
	s.recordSync(appendNote(note+")", clients.note), err)
	if err == nil {
		// Penanda push terakhir: push berikutnya (jadwal 6 jam / auto-push) hanya mengirim
		// baris yang berubah sejak titik ini.
		_ = s.store.SetMeta("last_push_at", time.Now().UTC().Format(time.RFC3339))
	}
	return sent, skippedEdge, err
}

// pruneSupabase menghapus baris mati lama di Supabase (housekeeping). `client` harus klien
// dengan service key (lihat supabaseServiceClient): anon key tidak lagi boleh DELETE.
func (s *Server) pruneSupabase(ctx context.Context, client *supabase.Client, days, limit int) (int, error) {
	if days <= 0 {
		return 0, nil
	}
	before := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	deleted, err := client.DeleteDead(ctx, before, limit)
	if err == nil {
		if total, alive, testErr := client.Test(ctx); testErr == nil {
			s.setLastRemote(total, alive)
		}
	}
	s.recordSync(fmt.Sprintf("prune %d baris mati (> %d hari)", deleted, days), err)
	return deleted, err
}

// ---------------------------------------------------------- housekeeping Supabase (retensi + backup)

// trimNoServiceKeyNote menjelaskan kenapa retensi riwayat deploy dilewati: RPC `deploy_log_trim`
// hanya diberi EXECUTE ke service_role (cfvpn-hub/sql/2026-11-15-deploy-log-trim.sql).
const trimNoServiceKeyNote = "dilewati: RPC deploy_log_trim hanya bisa dipanggil dengan supabase_service_key"

// trimDeployLogStep memanggil RPC `deploy_log_trim(p_keep_days)` dan SELALU mengembalikan langkah
// yang tidak menggagalkan pipeline: `deploy_log` hanya tabel audit panel, jadi kegagalan (fungsi
// belum dijalankan, key salah, jaringan) cukup dicatat di `Note`.
func (s *Server) trimDeployLogStep(ctx context.Context, testOnly bool) PipelineStep {
	if testOnly {
		return PipelineStep{Name: "housekeeping deploy_log", OK: true, Note: "dilewati (bukan pipeline penuh)"}
	}
	client := s.supabaseServiceClient()
	if !client.Configured() {
		return PipelineStep{Name: "housekeeping deploy_log", OK: true, Note: trimNoServiceKeyNote}
	}
	started := time.Now()
	step := PipelineStep{Name: "housekeeping deploy_log", OK: true}
	deleted, err := client.TrimDeployLog(ctx, supabase.DeployLogKeepDays)
	if err != nil {
		step.Note = fmt.Sprintf("gagal (best-effort, pipeline lanjut): %v", err)
	} else {
		step.Note = fmt.Sprintf("%d baris riwayat deploy (> %d hari) dihapus", deleted, supabase.DeployLogKeepDays)
		step.Counts = map[string]int{"dihapus": deleted, "hari": supabase.DeployLogKeepDays}
	}
	step.MS = time.Since(started).Milliseconds()
	return step
}

// stateBackupKeep adalah jumlah berkas backup state yang disimpan (14 hari terakhir).
const stateBackupKeep = 14

// stateBackupRowLimit membatasi baris user_states yang disalin sekali jalan. Backup yang
// terpotong ditandai di berkas dan di langkah pipeline supaya tidak terlihat lengkap.
const stateBackupRowLimit = 5000

// errStateBackupNoServiceKey menandai backup tidak bisa berjalan tanpa service key: tabel
// `user_states` sudah dicabut dari anon (cfvpn-hub/sql/2026-11-14-state-hardening.sql).
var errStateBackupNoServiceKey = errors.New("backup state butuh supabase_service_key (service_role key): tabel user_states tidak bisa dibaca dengan anon key")

// stateBackupFile adalah bentuk berkas backup: ringkasan + salinan baris `user_states`.
type stateBackupFile struct {
	GeneratedAt     string               `json:"generated_at"`
	UserStatesCount int                  `json:"user_states_count"`
	ProxyPoolCount  int                  `json:"proxy_pool_count"`
	Truncated       bool                 `json:"truncated"`
	UserStates      []supabase.UserState `json:"user_states"`
}

// stateBackupResult adalah ringkasan hasil backup (balasan POST /api/state-backup + langkah
// pipeline).
type stateBackupResult struct {
	Path        string   `json:"path"`
	Bytes       int64    `json:"bytes"`
	UserStates  int      `json:"user_states"`
	ProxyPool   int      `json:"proxy_pool"`
	GeneratedAt string   `json:"generated_at"`
	Kept        int      `json:"kept"`
	Removed     []string `json:"removed,omitempty"`
	Truncated   bool     `json:"truncated,omitempty"`
}

// backupDir adalah folder berkas backup: <dataDir>/backups (dataDir kosong = folder kerja proses).
func (s *Server) backupDir() string {
	return filepath.Join(s.dataDir, "backups")
}

// backupState menyalin `user_states` — dibaca dengan service key dan hanya kolom
// username/state_data/updated_at, jadi tidak ada sandi/hash yang ikut (lihat
// supabase.FetchUserStates) — ke <dataDir>/backups/user_states-YYYY-MM-DD.json, lengkap dengan
// ringkasan (jumlah baris user_states, jumlah baris proxy_pool, waktu). Setelah menulis, hanya
// 14 berkas terbaru yang disisakan.
func (s *Server) backupState(ctx context.Context) (stateBackupResult, error) {
	client := s.supabaseServiceClient()
	if !client.Configured() {
		return stateBackupResult{}, errStateBackupNoServiceKey
	}
	// Satu baris lebih banyak dari batas: kalau kelebihan, berkasnya ditandai terpotong
	// alih-alih tampak lengkap.
	states, err := client.FetchUserStates(ctx, stateBackupRowLimit+1)
	if err != nil {
		return stateBackupResult{}, fmt.Errorf("baca user_states: %w", err)
	}
	truncated := len(states) > stateBackupRowLimit
	if truncated {
		states = states[:stateBackupRowLimit]
	}
	poolRows, err := client.CountRows(ctx)
	if err != nil {
		return stateBackupResult{}, fmt.Errorf("hitung proxy_pool: %w", err)
	}
	generatedAt := time.Now().UTC()
	dir := s.backupDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return stateBackupResult{}, fmt.Errorf("siapkan folder backup %s: %w", dir, err)
	}
	path := filepath.Join(dir, "user_states-"+generatedAt.Format("2006-01-02")+".json")
	payload := stateBackupFile{
		GeneratedAt:     generatedAt.Format(time.RFC3339),
		UserStatesCount: len(states),
		ProxyPoolCount:  poolRows,
		Truncated:       truncated,
		UserStates:      states,
	}
	data, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return stateBackupResult{}, err
	}
	data = append(data, '\n')
	if err := writeFileAtomic(path, data, 0o600); err != nil {
		return stateBackupResult{}, err
	}
	kept, removed, rotateErr := rotateStateBackups(dir, stateBackupKeep)
	if rotateErr != nil {
		// Berkasnya sudah tertulis utuh; kegagalan merotasi hanya catatan (tidak ada data
		// pengguna yang hilang), jadi backup tetap dilaporkan sukses.
		log.Printf("rotasi backup state gagal: %v", rotateErr)
	}
	s.recordSync(fmt.Sprintf("backup state: %d baris user_states + %d baris proxy_pool → %s", len(states), poolRows, path), nil)
	return stateBackupResult{
		Path:        path,
		Bytes:       int64(len(data)),
		UserStates:  len(states),
		ProxyPool:   poolRows,
		GeneratedAt: payload.GeneratedAt,
		Kept:        kept,
		Removed:     removed,
		Truncated:   truncated,
	}, nil
}

// backupStateStep menjalankan backup state pada pipeline harian. Berbeda dari retensi deploy_log,
// kegagalan di sini MENANDAI langkah gagal: berkas backup adalah jaring pengaman data pengguna,
// jadi diam-diam melewatinya jauh lebih mahal daripada pipeline yang berhenti dengan pesan jelas.
func (s *Server) backupStateStep(ctx context.Context, testOnly bool) PipelineStep {
	if testOnly {
		return PipelineStep{Name: "backup state user_states", OK: true, Note: "dilewati (bukan pipeline penuh)"}
	}
	started := time.Now()
	step := PipelineStep{Name: "backup state user_states"}
	result, err := s.backupState(ctx)
	switch {
	case err != nil:
		step.Note = err.Error()
	default:
		step.OK = true
		step.Note = fmt.Sprintf("%d baris user_states + %d baris proxy_pool → %s (%d B, %d berkas disimpan)",
			result.UserStates, result.ProxyPool, result.Path, result.Bytes, result.Kept)
		if result.Truncated {
			step.Note += fmt.Sprintf("; DIPOTONG: hanya %d baris pertama yang disalin", result.UserStates)
		}
		if len(result.Removed) > 0 {
			step.Note += fmt.Sprintf("; %d berkas lama dihapus", len(result.Removed))
		}
		step.Counts = map[string]int{
			"user_states": result.UserStates, "proxy_pool": result.ProxyPool,
			"disimpan": result.Kept, "dihapus": len(result.Removed),
		}
	}
	step.MS = time.Since(started).Milliseconds()
	return step
}

// writeFileAtomic menulis berkas lewat berkas sementara di folder yang sama lalu me-rename,
// supaya backup yang belum selesai tidak pernah terbaca sebagai berkas jadi.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return fmt.Errorf("tulis %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("pindahkan %s: %w", path, err)
	}
	return nil
}

// rotateStateBackups menyisakan `keep` berkas backup terbaru dan menghapus sisanya. Nama berkas
// memuat tanggal (user_states-YYYY-MM-DD.json), jadi urutan nama = urutan waktu.
func rotateStateBackups(dir string, keep int) (int, []string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasPrefix(name, "user_states-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		names = append(names, name)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	if keep < 0 {
		keep = 0
	}
	kept := min(len(names), keep)
	removed := make([]string, 0, len(names)-kept)
	for _, name := range names[kept:] {
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return kept, removed, err
		}
		removed = append(removed, name)
	}
	return kept, removed, nil
}

// handleStateBackup menulis salinan `user_states` sekarang (tombol manual / klien mesin).
// Rute ini TIDAK publik: gate umum sudah mewajibkan cookie sesi atau header X-Api-Token.
func (s *Server) handleStateBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	result, err := s.backupState(ctx)
	if err != nil {
		status := http.StatusBadGateway
		if errors.Is(err, errStateBackupNoServiceKey) {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, result)
}

// supabaseSink menjembatani scraper.RemoteSink ke klien Supabase (menjaga scraper tetap
// tidak bergantung pada paket Supabase). Verdict ditulis per batch: baris hidup lewat
// upsert (nilai ping berbeda per baris), baris mati lewat satu PATCH per 200 id.
type supabaseSink struct {
	client  *supabase.Client
	onWrite func(written int)
	// requireTLS: baris hidup hanya diaktifkan kalau terbukti sanggup TLS 443. Tanpa ini,
	// jalur pull-verify akan menyalakan kembali proxy HTTP-only yang sudah kita turunkan.
	requireTLS bool
}

func (s supabaseSink) UpdateVerdicts(ctx context.Context, verdicts []scraper.Verdict) error {
	if len(verdicts) == 0 {
		return nil
	}
	now := time.Now().UTC().Format(time.RFC3339)
	alive := make([]supabase.Row, 0, len(verdicts))
	deadIDs := make([]string, 0, len(verdicts))
	for _, v := range verdicts {
		if v.Row.ID == "" {
			continue
		}
		if v.Alive {
			active := v.TLS || !s.requireTLS
			alive = append(alive, supabase.Row{
				IP: v.Row.IP, Port: v.Row.Port, Country: v.Row.Country,
				IsActive: active, LastChecked: now, Ping: v.PingMS,
			}.EvidenceRow(v.TLS, v.AltOK, v.ExitIP).
				HostEvidence(v.Alive, v.PingMS, time.Now().UTC()))
		} else {
			deadIDs = append(deadIDs, v.Row.ID)
		}
	}
	written := 0
	if len(alive) > 0 {
		sent, err := s.client.Upsert(ctx, alive)
		written += sent
		if err != nil {
			return err
		}
	}
	if len(deadIDs) > 0 {
		sent, err := s.client.MarkDead(ctx, deadIDs, time.Now())
		written += sent
		if err != nil {
			return err
		}
	}
	if s.onWrite != nil {
		s.onWrite(written)
	}
	return nil
}

// remoteOrgs mengubah baris Supabase menjadi daftar org untuk store.SetOrgs: hanya baris
// ber-port numerik dan ber-org tidak kosong, karena store tidak boleh menimpa nilai lama
// dengan kosong.
func remoteOrgs(rows []supabase.Row) []store.OrgRef {
	if len(rows) == 0 {
		return nil
	}
	refs := make([]store.OrgRef, 0, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row.Org) == "" {
			continue
		}
		port, err := strconv.Atoi(strings.TrimSpace(row.Port))
		if err != nil {
			continue
		}
		refs = append(refs, store.OrgRef{IP: strings.TrimSpace(row.IP), Port: port, Org: row.Org})
	}
	return refs
}

// pullVerify menarik baris dari Supabase dan memverifikasinya di sini. Barisnya ditarik dengan
// klien anon (`clients.read`), sedangkan verdictnya ditulis dengan `clients.write` (service key
// bila terisi) — anon sudah tidak punya hak tulis pada proxy_pool.
func (s *Server) pullVerify(ctx context.Context, clients supabaseClients, opts scraper.Options, limit int) (scraper.Summary, error) {
	client := clients.read
	cfg := s.currentSettings()
	// Ambil yang paling lama tidak dicek (atau belum pernah) supaya tidak mengulang baris baru.
	staleBefore := time.Now().Add(-cfg.RefreshAfter()).Unix()
	rows, err := client.Pull(ctx, limit, 0, staleBefore, false)
	if err != nil {
		s.recordSync("pull gagal", err)
		return scraper.Summary{}, err
	}
	if len(rows) == 0 {
		rows, err = client.Pull(ctx, limit, 0, 0, false)
		if err != nil {
			s.recordSync("pull gagal", err)
			return scraper.Summary{}, err
		}
	}
	if len(rows) == 0 {
		return scraper.Summary{}, fmt.Errorf("tabel %s kosong", client.Table)
	}
	remote := make([]scraper.RemoteRow, 0, len(rows))
	for _, row := range rows {
		remote = append(remote, scraper.RemoteRow{ID: row.ID, IP: row.IP, Port: row.Port, Country: row.Country})
	}
	// Verdict ditulis lewat klien tulis (service key bila terisi): MarkDead/UpdateVerdict
	// adalah operasi tulis pada proxy_pool.
	summary, err := scraper.VerifyRemote(ctx, s.store, supabaseSink{client: clients.write, requireTLS: s.currentSettings().PushRequireTLS}, remote, opts, s.runner)
	// org (penyedia/ASN) yang dibaca dari tabel Supabase disimpan di database lokal supaya
	// push bisa membatasi berapa baris per penyedia (`push_org_cap`); scanner tidak menebak
	// nilai ini sendiri. Dijalankan SETELAH verifikasi supaya baris yang baru masuk batch ini
	// ikut kebagian. Kegagalan di sini tidak menggagalkan verifikasi — hanya berarti cap org
	// belum bisa dipakai untuk push berikutnya.
	if orgs := remoteOrgs(rows); len(orgs) > 0 {
		if orgErr := s.store.SetOrgs(orgs); orgErr != nil {
			log.Printf("simpan org dari Supabase gagal: %v", orgErr)
		}
	}
	if err == nil {
		if total, alive, testErr := client.Test(ctx); testErr == nil {
			s.setLastRemote(total, alive)
		}
	}
	s.recordSync(appendNote(fmt.Sprintf("verifikasi %d baris Supabase (hidup %d, mati %d)", summary.Checked, summary.Alive, summary.Dead), clients.note), err)
	return summary, err
}

// recordSync menyimpan catatan sinkron terakhir untuk ditampilkan di UI.
func (s *Server) recordSync(note string, err error) {
	entry := map[string]any{"at": time.Now().Format(time.RFC3339), "note": note}
	if err != nil {
		entry["error"] = err.Error()
	}
	s.stateMu.Lock()
	s.lastSync = entry
	s.stateMu.Unlock()
}

type remoteCount struct {
	total int
	alive int
	at    time.Time
}

// setLastRemote menyimpan jumlah baris Supabase terakhir yang diketahui (agar endpoint
// status tetap cepat).
func (s *Server) setLastRemote(total, alive int) {
	s.stateMu.Lock()
	s.remote = remoteCount{total: total, alive: alive, at: time.Now()}
	s.stateMu.Unlock()
}

func (s *Server) lastRemote() (remoteCount, bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.remote.at.IsZero() {
		return remoteCount{}, false
	}
	return s.remote, true
}

// Span terakhir pipeline (untuk UI): tarik dari Supabase → uji → scrape sumber → push.
type PipelineStep struct {
	Name   string         `json:"name"`
	OK     bool           `json:"ok"`
	Note   string         `json:"note"`
	Counts map[string]int `json:"counts,omitempty"`
	MS     int64          `json:"ms"`
}

type PipelineStatus struct {
	Running   bool           `json:"running"`
	StartedAt time.Time      `json:"started_at,omitempty"`
	EndedAt   time.Time      `json:"ended_at,omitempty"`
	Steps     []PipelineStep `json:"steps"`
	Error     string         `json:"error,omitempty"`
}

// RunPipeline menjalankan satu pipeline penuh di latar belakang:
//
//  1. tarik baris dari Supabase -> verifikasi di sini -> tulis hasilnya kembali
//  2. scrape semua sumber -> verifikasi -> simpan ke database lokal
//  3. push proxy hidup & segar ke Supabase
//  4. prune baris mati, retensi riwayat deploy (deploy_log_trim), backup state user_states
//
// Dijalankan oleh penjadwal (mode interval/harian saat pipeline diaktifkan) atau manual
// lewat POST /api/pipeline/run.
// RunPipeline menjalankan pipeline penuh (dipakai tombol manual dan jadwal scrape).
func (s *Server) RunPipeline() error {
	return s.RunPipelineMode(false)
}

// RunPipelineMode menjalankan pipeline. testOnly=true dipakai jadwal 6 jam: tidak menarik baris
// dari Supabase, tidak mengambil sumber baru, tidak prune, dan tidak menjalankan housekeeping
// Supabase (retensi deploy_log + backup state) — isinya cek lokal (refresh baris tersimpan + pass
// target negara) lalu push delta (hanya baris yang berubah sejak meta.last_push_at).
// testOnly=false = siklus penuh: pull-verify, scrape semua sumber, push menyeluruh, prune,
// retensi deploy_log, backup state, housekeeping lokal.
func (s *Server) RunPipelineMode(testOnly bool) error {
	if err := s.beginPipeline(); err != nil {
		return err
	}
	go s.runPipelineLocked(testOnly)
	return nil
}

// RunPipelineSync menjalankan pipeline di goroutine pemanggil dan mengembalikan error sebenarnya.
// Dipakai penjadwal: penanda scrape harian hanya boleh ditulis kalau pipeline benar-benar sukses.
// Tanpa varian ini penjadwal menandai "sudah jalan" padahal pipeline masih berjalan di latar
// belakang, sehingga kegagalan tengah jalan tidak pernah terlihat dan tidak pernah dicoba lagi.
func (s *Server) RunPipelineSync(testOnly bool) error {
	if err := s.beginPipeline(); err != nil {
		return err
	}
	return s.runPipelineLocked(testOnly)
}

// beginPipeline mengambil kunci pipeline dan menandai status sedang berjalan.
func (s *Server) beginPipeline() error {
	if !s.pipelineRun.TryLock() {
		return fmt.Errorf("pipeline lain sedang berjalan")
	}
	s.setPipeline(func(p *PipelineStatus) {
		p.Running = true
		p.StartedAt = time.Now()
		p.EndedAt = time.Time{}
		p.Steps = nil
		p.Error = ""
	})
	return nil
}

// runPipelineLocked menjalankan seluruh langkah pipeline. Pemanggil sudah memegang pipelineRun
// dan fungsi ini selalu melepasnya; error yang dikembalikan mencerminkan langkah yang gagal —
// kecuali langkah retensi deploy_log, yang memang best-effort (hanya menulis catatan).
func (s *Server) runPipelineLocked(testOnly bool) error {
	defer s.pipelineRun.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()
	cfg := s.currentSettings()
	clients := s.supabaseClients()

	// --- langkah 1: tarik & verifikasi baris Supabase ---
	pullLimit := cfg.PipelinePullLimit
	if pullLimit <= 0 {
		pullLimit = cfg.SupabasePullLimit
	}
	if !testOnly && clients.read.Configured() && pullLimit > 0 {
		started := time.Now()
		summary, err := s.pullVerify(ctx, clients, scraper.OptionsFromSettings(cfg), pullLimit)
		step := PipelineStep{
			Name: "tarik & verifikasi Supabase", OK: err == nil, MS: time.Since(started).Milliseconds(),
			Counts: map[string]int{"dicek": summary.Checked, "hidup": summary.Alive, "mati": summary.Dead, "error": summary.Errors},
		}
		if err != nil {
			step.Note = err.Error()
		} else {
			step.Note = fmt.Sprintf("%d baris diuji, %d masih hidup", summary.Checked, summary.Alive)
		}
		step.Note = appendNote(step.Note, clients.note)
		s.addPipelineStep(step)
	} else {
		s.addPipelineStep(PipelineStep{Name: "tarik & verifikasi Supabase", OK: true, Note: "dilewati (belum dikonfigurasi / limit 0)"})
	}

	// --- langkah 2: scrape sumber lalu verifikasi ---
	// Notifikasi harian butuh angka langkah scrape + jumlah baris yang dikirim langkah push,
	// jadi keduanya ditampung dulu; pesannya dikirim setelah langkah push selesai.
	var scrapeCounts map[string]int
	var scrapeMS int64
	pushedRows := -1 // -1 = langkah push dilewati → pesan menulis "-"
	if testOnly {
		// Cek lokal (jadwal 6 jam): refresh baris tersimpan + pass target negara, tanpa
		// mengambil sumber baru dan tanpa menarik ulang dari Supabase.
		started := time.Now()
		localOpts := scraper.OptionsFromSettings(cfg)
		localOpts.LocalOnly = true
		_ = s.waitForCycle(ctx, 10*time.Minute)
		localErr := s.runner.Start(localOpts)
		if localErr == nil {
			localErr = s.waitForCycle(ctx, 30*time.Minute)
		}
		localProgress := s.runner.Progress()
		localStep := PipelineStep{Name: "cek lokal (refresh + target negara)", OK: localErr == nil, MS: time.Since(started).Milliseconds(),
			Counts: map[string]int{"diperiksa": localProgress.Refreshed, "hidup": localProgress.Alive}}
		if localErr != nil {
			localStep.Note = localErr.Error()
		} else {
			localStep.Note = fmt.Sprintf("%d baris diverifikasi ulang", localProgress.Refreshed)
		}
		s.addPipelineStep(localStep)
		// Pesan "cek 6 jam": best-effort; kalau kirim gagal, catatannya menempel di langkah ini.
		s.notifyPipeline(ctx, cfg, localStep.Name, notify.LocalMessage(localStep.Counts))
	} else {
		started := time.Now()
		opts := scraper.OptionsFromSettings(cfg)
		// Siklus manual/otomatis lain bisa sedang berjalan; tunggu dulu supaya langkah ini
		// tidak dicatat gagal hanya karena berebut runner.
		_ = s.waitForCycle(ctx, 15*time.Minute)
		scrapeErr := s.runner.Start(opts)
		if scrapeErr == nil {
			scrapeErr = s.waitForCycle(ctx, 90*time.Minute)
		}
		progress := s.runner.Progress()
		busy := scrapeErr != nil && strings.Contains(scrapeErr.Error(), "siklus lain")
		step := PipelineStep{
			Name: "scrape sumber & verifikasi", OK: scrapeErr == nil || busy, MS: time.Since(started).Milliseconds(),
			Counts: map[string]int{"kandidat": progress.Candidates, "dicek": progress.Checked, "hidup": progress.Alive, "mati": progress.Dead},
		}
		switch {
		case busy:
			step.Note = "dilewati — siklus lain mengerjakan pekerjaan yang sama"
		case scrapeErr != nil:
			step.Note = scrapeErr.Error()
		default:
			step.Note = fmt.Sprintf("%d kandidat diperiksa, %d hidup", progress.Checked, progress.Alive)
		}
		s.addPipelineStep(step)
		scrapeCounts, scrapeMS = step.Counts, step.MS

	}

	// --- langkah 3: push hasil ke Supabase ---
	if clients.read.Configured() && cfg.PipelinePushLimit > 0 {
		started := time.Now()
		edgeDead, edgeErr := clients.read.PullEdgeVerdicts(ctx, 5000)
		if edgeErr != nil {
			edgeDead = nil
		}
		// Push saat cek lokal hanya mengirim yang berubah sejak push terakhir.
		since := time.Time{}
		if testOnly {
			if raw, metaErr := s.store.Meta("last_push_at"); metaErr == nil && raw != "" {
				if stamp, parseErr := time.Parse(time.RFC3339, raw); parseErr == nil {
					since = stamp
				}
			}
		}
		pushed, skippedEdge, err := s.pushToSupabase(ctx, clients, cfg, cfg.PipelinePushLimit, edgeDead, since)
		step := PipelineStep{Name: "push ke Supabase", OK: err == nil, MS: time.Since(started).Milliseconds(), Counts: map[string]int{"dikirim": pushed}}
		if err != nil {
			step.Note = err.Error()
		} else {
			step.Note = fmt.Sprintf("%d proxy dikirim", pushed)
			if skippedEdge > 0 {
				step.Note += fmt.Sprintf(", %d ditahan (edge bilang mati)", skippedEdge)
			}
			step.Counts["ditahan_edge_mati"] = skippedEdge
		}
		step.Note = appendNote(step.Note, clients.note)
		s.addPipelineStep(step)
		pushedRows = pushed
	} else {
		s.addPipelineStep(PipelineStep{Name: "push ke Supabase", OK: true, Note: "dilewati (belum dikonfigurasi / limit 0)"})
	}

	// --- notifikasi harian (best-effort) ---
	// Hanya jalur siklus penuh (harian / catch-up), bukan cek lokal 6 jam — dan hanya setelah
	// langkah push selesai supaya jumlah baris yang dikirim ke pool ikut terlaporkan.
	// Kegagalan kirim tidak pernah menggagalkan pipeline: hanya satu baris log + catatan langkah.
	if !testOnly {
		s.notifyPipeline(ctx, cfg, "scrape sumber & verifikasi",
			notify.DailyMessage(scrapeCounts, pushedRows, time.Duration(scrapeMS)*time.Millisecond))
	}

	// --- langkah 4: housekeeping baris mati di Supabase ---
	// DELETE butuh service key (panel tidak lagi memberi anon izin hapus); tanpa key itu
	// langkah dilewati dengan catatan jelas, bukan dianggap gagal.
	if !testOnly && clients.read.Configured() && cfg.SupabasePruneDays > 0 {
		deleteClient := s.supabaseServiceClient()
		if !deleteClient.Configured() {
			s.addPipelineStep(PipelineStep{Name: "prune baris mati Supabase", OK: true, Note: pruneNoServiceKeyNote})
		} else {
			started := time.Now()
			deleted, err := s.pruneSupabase(ctx, deleteClient, cfg.SupabasePruneDays, 2000)
			step := PipelineStep{Name: "prune baris mati Supabase", OK: err == nil, MS: time.Since(started).Milliseconds(), Counts: map[string]int{"dihapus": deleted, "hari": cfg.SupabasePruneDays}}
			if err != nil {
				step.Note = err.Error()
			} else {
				step.Note = fmt.Sprintf("%d baris mati (> %d hari) dihapus", deleted, cfg.SupabasePruneDays)
			}
			s.addPipelineStep(step)
		}
	} else {
		s.addPipelineStep(PipelineStep{Name: "prune baris mati Supabase", OK: true, Note: "dilewati (supabase_prune_days = 0)"})
	}

	// --- langkah 5: retensi riwayat deploy (RPC deploy_log_trim) ---
	// Best-effort: riwayat deploy hanya tabel audit panel, jadi kegagalannya dicatat di langkah
	// ini dan pipeline tetap lanjut (langkahnya tidak pernah menandai pipeline gagal).
	s.addPipelineStep(s.trimDeployLogStep(ctx, testOnly))

	// --- langkah 6: backup state user_states ---
	s.addPipelineStep(s.backupStateStep(ctx, testOnly))

	// --- langkah 7: housekeeping database lokal ---
	{
		started := time.Now()
		hopeless, err := s.store.DeleteHopeless(2, 5000)
		step := PipelineStep{Name: "bersihkan kandidat gagal berulang", OK: err == nil, MS: time.Since(started).Milliseconds(), Counts: map[string]int{"dihapus": int(hopeless)}}
		if err != nil {
			step.Note = err.Error()
		} else {
			step.Note = fmt.Sprintf("%d baris (>=2× gagal, belum pernah hidup) dihapus", hopeless)
		}
		s.addPipelineStep(step)
	}

	var failure error
	s.setPipeline(func(p *PipelineStatus) {
		p.Running = false
		p.EndedAt = time.Now()
		if p.Error != "" {
			failure = errors.New(p.Error)
		}
	})
	return failure
}

// waitForCycle menunggu siklus scraping yang sedang berjalan selesai.
func (s *Server) waitForCycle(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(3 * time.Second):
		}
		if !s.runner.Progress().Running {
			return nil
		}
	}
	return fmt.Errorf("siklus belum selesai setelah %s", timeout)
}

func (s *Server) setPipeline(fn func(*PipelineStatus)) {
	s.pipelineMu.Lock()
	fn(&s.pipeline)
	s.pipelineMu.Unlock()
}

func (s *Server) addPipelineStep(step PipelineStep) {
	s.setPipeline(func(p *PipelineStatus) {
		p.Steps = append(p.Steps, step)
		if !step.OK {
			p.Error = step.Name + ": " + step.Note
		}
	})
}

// telegramClient membuat klien Telegram dari setelan; tes mengganti pembuatnya supaya tidak
// pernah ada request ke api.telegram.org.
func (s *Server) telegramClient(cfg settings.Settings) *notify.Client {
	if s.notifyClient != nil {
		return s.notifyClient(cfg.TelegramBotToken, cfg.TelegramChatID)
	}
	return notify.New(cfg.TelegramBotToken, cfg.TelegramChatID)
}

// notifyPipeline mengirim satu notifikasi Telegram untuk langkah pipeline `stepName`.
//
// Best-effort: tanpa setelan lengkap fungsi ini tidak melakukan apa pun, dan kegagalan kirim
// hanya menghasilkan satu baris log + catatan singkat pada langkah tersebut — pipeline tetap
// dianggap sukses (langkahnya tidak pernah diubah jadi gagal).
func (s *Server) notifyPipeline(ctx context.Context, cfg settings.Settings, stepName, text string) {
	client := s.telegramClient(cfg)
	if !client.Configured() {
		return
	}
	started := time.Now()
	status, err := client.Send(ctx, text)
	if err != nil {
		log.Printf("notifikasi Telegram gagal (%s): %v", stepName, err)
		s.annotatePipelineStep(stepName, "notifikasi gagal: "+err.Error())
		return
	}
	log.Printf("notifikasi Telegram terkirim (%s): HTTP %d dalam %d ms", stepName, status, time.Since(started).Milliseconds())
}

// annotatePipelineStep menambahkan catatan pada langkah pipeline yang sudah tercatat; langkah
// yang tidak ditemukan (mis. pipeline lain sudah menimpa status) diabaikan tanpa efek samping.
func (s *Server) annotatePipelineStep(name, note string) {
	s.setPipeline(func(p *PipelineStatus) {
		for i := range p.Steps {
			if p.Steps[i].Name == name {
				p.Steps[i].Note = appendNote(p.Steps[i].Note, note)
				return
			}
		}
	})
}

// handlePipeline melaporkan status pipeline terakhir.
func (s *Server) handlePipeline(w http.ResponseWriter, r *http.Request) {
	s.pipelineMu.Lock()
	status := s.pipeline
	s.pipelineMu.Unlock()
	cfg := s.currentSettings()
	writeJSON(w, http.StatusOK, map[string]any{
		"pipeline": status,
		"config": map[string]any{
			"schedule_mode":       cfg.ScheduleMode,
			"daily_at":            cfg.DailyAt,
			"interval_minutes":    cfg.IntervalMinutes,
			"pipeline_enabled":    cfg.PipelineEnabled,
			"pipeline_pull_limit": cfg.PipelinePullLimit,
			"pipeline_push_limit": cfg.PipelinePushLimit,
			"auto":                cfg.Auto,
			"scheduler_running":   s.sched.Running(),
			"next_run":            s.nextRun(),
		},
	})
}

// handlePipelineRun memicu pipeline sekarang (manual).
func (s *Server) handlePipelineRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	if err := s.RunPipeline(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"started": true})
}

// notifyTestMessage adalah isi pesan tombol "Kirim uji" di panel.
const notifyTestMessage = "Pan-VlessCloudflare - uji notifikasi\nSetelan bot dan chat id tersambung."

// handleNotifyTest mengirim satu pesan uji ke Telegram memakai setelan tersimpan, lalu
// meneruskan hasil balasan Telegram apa adanya (status HTTP-nya ikut dilaporkan).
//
// Tanpa setelan lengkap endpoint ini menjawab 503 (token belum ada = fitur belum aktif) atau
// 400 (token ada tapi chat id kosong) dengan pesan jelas — bukan error 500, karena tidak ada
// yang rusak, hanya belum diisi.
func (s *Server) handleNotifyTest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	cfg := s.currentSettings()
	missing := make([]string, 0, 2)
	if strings.TrimSpace(cfg.TelegramBotToken) == "" {
		missing = append(missing, "telegram_bot_token")
	}
	if strings.TrimSpace(cfg.TelegramChatID) == "" {
		missing = append(missing, "telegram_chat_id")
	}
	if len(missing) > 0 {
		code := http.StatusServiceUnavailable
		if strings.TrimSpace(cfg.TelegramBotToken) != "" {
			// Token sudah ada, jadi yang kurang hanya tujuan: itu kesalahan setelan (400).
			code = http.StatusBadRequest
		}
		writeJSON(w, code, map[string]any{
			"ok":      false,
			"error":   "setelan Telegram belum lengkap, isi dulu: " + strings.Join(missing, ", "),
			"missing": missing,
		})
		return
	}

	status, err := s.telegramClient(cfg).Send(r.Context(), notifyTestMessage)
	if err != nil {
		// Balasan Telegram (atau kegagalan jaringan) diteruskan apa adanya; telegram_status 0
		// berarti request tidak sampai sama sekali.
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"ok":              false,
			"error":           err.Error(),
			"telegram_status": status,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "telegram_status": status})
}

// nextRun menghitung perkiraan jadwal berikutnya (untuk ditampilkan di UI).
func (s *Server) nextRun() string {
	cfg := s.currentSettings()
	if !cfg.Auto || !s.sched.Running() {
		return ""
	}
	if cfg.ScheduleMode == "daily" {
		return settings.NextDaily(cfg.DailyAt, time.Now()).Format("2006-01-02 15:04")
	}
	last := s.sched.LastRun()
	if last.IsZero() {
		return time.Now().Format("2006-01-02 15:04")
	}
	return last.Add(cfg.Interval()).Format("2006-01-02 15:04")
}

// AfterCycle dijalankan penjadwal setiap siklus selesai: dorong hasil ke Supabase dan/atau
// verifikasi baris lama dari Supabase, sesuai pengaturan.
func (s *Server) AfterCycle() {
	cfg := s.currentSettings()
	clients := s.supabaseClients()
	if !clients.read.Configured() {
		return
	}
	if !cfg.SupabaseAutoPush && cfg.SupabasePullLimit <= 0 {
		return
	}
	if !s.syncRun.TryLock() {
		return // sinkronisasi sebelumnya masih berjalan
	}
	go func() {
		defer s.syncRun.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		if cfg.SupabaseAutoPush {
			edgeDead, _ := clients.read.PullEdgeVerdicts(ctx, 5000)
			since := time.Time{}
			if raw, metaErr := s.store.Meta("last_push_at"); metaErr == nil && raw != "" {
				if stamp, parseErr := time.Parse(time.RFC3339, raw); parseErr == nil {
					since = stamp
				}
			}
			if pushed, heldEdge, err := s.pushToSupabase(ctx, clients, cfg, 1000, edgeDead, since); err != nil {
				log.Printf("supabase push gagal: %v", err)
			} else {
				log.Printf("supabase: %d proxy didorong (%d ditahan: edge bilang mati)", pushed, heldEdge)
			}
		}
		if cfg.SupabasePullLimit > 0 {
			summary, err := s.pullVerify(ctx, clients, scraper.OptionsFromSettings(cfg), cfg.SupabasePullLimit)
			if err != nil {
				log.Printf("supabase pull-verify gagal: %v", err)
			} else {
				log.Printf("supabase: %d baris diverifikasi (hidup %d, mati %d)", summary.Checked, summary.Alive, summary.Dead)
			}
		}
	}()
}

func atoiDefault(raw string, fallback int) int {
	if n, err := strconv.Atoi(raw); err == nil {
		return n
	}
	return fallback
}

// handleTunnel melaporkan cara mengekspos panel lewat Cloudflare Tunnel: alamat listen,
// perintah cloudflared, config.yml, dan apakah permintaan ini lewat Cloudflare.
func (s *Server) handleTunnel(w http.ResponseWriter, r *http.Request) {
	cfg := s.currentSettings()
	forwardedHost := r.Header.Get("X-Forwarded-Host")
	if forwardedHost == "" {
		forwardedHost = r.Header.Get("Host")
	}
	localURL := "http://" + normaliseLocalAddr(s.addr)
	host, port, _ := net.SplitHostPort(s.addr)
	bindLAN := !(host == "127.0.0.1" || host == "localhost")

	domain := cfg.TunnelDomain
	if domain == "" {
		domain = "scraping.domainmu.com"
	}
	configYAML := strings.Join([]string{
		"# simpan sebagai ~/.cloudflared/config.yml (Windows: C:\\Users\\<user>\\.cloudflared\\config.yml)",
		"tunnel: " + cfg.TunnelName,
		"credentials-file: <path hasil 'cloudflared tunnel create " + cfg.TunnelName + "'>",
		"",
		"ingress:",
		"  - hostname: " + domain,
		"    service: " + localURL,
		"    originRequest:",
		"      noTLSVerify: true",
		"  - service: http_status:404",
		"",
	}, "\n")

	steps := []string{
		"cloudflared tunnel login",
		"cloudflared tunnel create " + cfg.TunnelName,
		"cloudflared tunnel route dns " + cfg.TunnelName + " " + domain,
		"cloudflared tunnel run --url " + localURL + " " + cfg.TunnelName,
	}
	if cfg.TunnelDomain != "" {
		steps[2] = "cloudflared tunnel route dns " + cfg.TunnelName + " " + cfg.TunnelDomain + "   # sudah diatur di pengaturan"
	}

	// Kondisi tunnel: status unit systemd + hasil uji jalur publik. ?probe=1 memaksa uji baru.
	force := r.URL.Query().Get("probe") == "1"

	writeJSON(w, http.StatusOK, map[string]any{
		"state": s.tunnelStateFor(cfg, force),
		"listen": map[string]any{
			"addr":     s.addr,
			"host":     host,
			"port":     port,
			"bind_lan": bindLAN,
			"note":     map[bool]string{true: "listen di semua interface (LAN bisa akses)", false: "hanya localhost"}[bindLAN],
		},
		"local_urls":           append([]string{localURL}, lanURLs(s.addr)...),
		"env_file":             s.envPath,
		"settings_file":        s.cfgPath,
		"public_url":           cfg.PublicURL,
		"tunnel_name":          cfg.TunnelName,
		"tunnel_domain":        cfg.TunnelDomain,
		"cf_ray":               r.Header.Get("Cf-Ray"),
		"cf_connecting_ip":     r.Header.Get("Cf-Connecting-Ip"),
		"forwarded_host":       forwardedHost,
		"via_cloudflare":       r.Header.Get("Cf-Ray") != "",
		"quick_tunnel_command": "cloudflared tunnel --url " + localURL,
		"named_tunnel_steps":   steps,
		"config_yaml":          configYAML,
		"notes": []string{
			"Panel ini bind " + s.addr + " — cloudflared dijalankan di mesin yang sama, mengarah ke " + localURL + ".",
			"Quick tunnel cukup satu perintah dan memberi URL https://<acak>.trycloudflare.com tanpa domain sendiri.",
			"Named tunnel memakai domain sendiri; setelah jalan, isi Public URL di pengaturan agar tercatat di halaman ini.",
			"Kalau panel dibuka ke publik, isi Token API supaya /api/* tidak bisa dipakai orang lain.",
		},
	})
}

func normaliseLocalAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return net.JoinHostPort(host, port)
}

func (s *Server) currentSettings() settings.Settings {
	s.mu.Lock()
	defer s.mu.Unlock()
	return *s.settings
}

func (s *Server) currentToken() string { return s.currentSettings().Token }

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "time": time.Now().UTC()})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	cfg := s.currentSettings()
	stats, err := s.store.Stats(cfg.Fresh())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"stats":    stats,
		"progress": s.runner.Progress(),
		"auto":     map[string]any{"running": s.sched.Running(), "last_run": s.sched.LastRun(), "interval_minutes": cfg.IntervalMinutes},
	})
}

// handleCountries menghitung proxy hidup per negara.
func (s *Server) handleCountries(w http.ResponseWriter, r *http.Request) {
	aliveOnly := r.URL.Query().Get("all") != "true"
	list, err := s.store.Countries(aliveOnly)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(list), "countries": list})
}

// handleSettings mengembalikan atau mengganti pengaturan (sebagian saja boleh).
func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		cfg := s.currentSettings()
		writeJSON(w, http.StatusOK, map[string]any{"settings": publicSettings(cfg), "runtime": s.runtimeInfo()})
		return
	}
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai GET atau POST"})
		return
	}

	var patch struct {
		Auto                *bool           `json:"auto"`
		IntervalMinutes     *int            `json:"interval_minutes"`
		AutoRotate          *bool           `json:"auto_rotate"`
		Cap                 *int            `json:"cap"`
		Jobs                *int            `json:"jobs"`
		MinSources          *int            `json:"min_sources"`
		RefreshLimit        *int            `json:"refresh_limit"`
		RefreshAfterMinutes *int            `json:"refresh_after_minutes"`
		FreshMinutes        *int            `json:"fresh_minutes"`
		PurgeDeadHours      *int            `json:"purge_dead_hours"`
		Token               *string         `json:"token"`
		AdminUser           *string         `json:"admin_user"`
		AdminPassword       *string         `json:"admin_password"`
		PublicURL           *string         `json:"public_url"`
		TunnelName          *string         `json:"tunnel_name"`
		TunnelDomain        *string         `json:"tunnel_domain"`
		SupabaseURL         *string         `json:"supabase_url"`
		SupabaseKey         *string         `json:"supabase_key"`
		SupabaseServiceKey  *string         `json:"supabase_service_key"`
		SupabaseTable       *string         `json:"supabase_table"`
		SupabaseAutoPush    *bool           `json:"supabase_auto_push"`
		SupabasePullLimit   *int            `json:"supabase_pull_limit"`
		PushOrgCap          *int            `json:"push_org_cap"`
		PushCountryCap      *int            `json:"push_country_cap"`
		ScheduleMode        *string         `json:"schedule_mode"`
		DailyAt             *string         `json:"daily_at"`
		PipelineEnabled     *bool           `json:"pipeline_enabled"`
		PipelinePullLimit   *int            `json:"pipeline_pull_limit"`
		PipelinePushLimit   *int            `json:"pipeline_push_limit"`
		ProbeTLS            *bool           `json:"probe_tls"`
		ProbeAlt            *bool           `json:"probe_alt"`
		PushRequireTLS      *bool           `json:"push_require_tls"`
		SupabasePruneDays   *int            `json:"supabase_prune_days"`
		AutoDisableSources  *bool           `json:"auto_disable_sources"`
		ZeroStreakLimit     *int            `json:"zero_streak_limit"`
		CountryTargets      *map[string]int `json:"country_targets"`
		ScheduleTimezone    *string         `json:"schedule_timezone"`
		TestIntervalHours   *int            `json:"test_interval_hours"`
		AutoDisableLowYield *bool           `json:"auto_disable_low_yield"`
		LowYieldCycles      *int            `json:"low_yield_cycles"`
		LowYieldMinEntries  *int            `json:"low_yield_min_entries"`
		CountryTopUp        *int            `json:"country_top_up"`
		TelegramBotToken    *string         `json:"telegram_bot_token"`
		TelegramChatID      *string         `json:"telegram_chat_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body JSON tidak valid: " + err.Error()})
		return
	}

	s.mu.Lock()
	cfg := *s.settings
	if patch.Auto != nil {
		cfg.Auto = *patch.Auto
	}
	if patch.IntervalMinutes != nil {
		cfg.IntervalMinutes = *patch.IntervalMinutes
	}
	if patch.AutoRotate != nil {
		cfg.AutoRotate = *patch.AutoRotate
	}
	if patch.Cap != nil {
		cfg.Cap = *patch.Cap
	}
	if patch.Jobs != nil {
		cfg.Jobs = *patch.Jobs
	}
	if patch.MinSources != nil {
		cfg.MinSources = *patch.MinSources
	}
	if patch.RefreshLimit != nil {
		cfg.RefreshLimit = *patch.RefreshLimit
	}
	if patch.RefreshAfterMinutes != nil {
		cfg.RefreshAfterMinutes = *patch.RefreshAfterMinutes
	}
	if patch.FreshMinutes != nil {
		cfg.FreshMinutes = *patch.FreshMinutes
	}
	if patch.PurgeDeadHours != nil {
		cfg.PurgeDeadHours = *patch.PurgeDeadHours
	}
	if patch.Token != nil {
		cfg.Token = strings.TrimSpace(*patch.Token)
	}
	if patch.AdminUser != nil && strings.TrimSpace(*patch.AdminUser) != "" {
		cfg.AdminUser = strings.TrimSpace(*patch.AdminUser)
	}
	if patch.AdminPassword != nil && *patch.AdminPassword != "" {
		cfg.AdminPasswordHash = settings.HashPassword(*patch.AdminPassword)
	}
	if patch.PublicURL != nil {
		cfg.PublicURL = *patch.PublicURL
	}
	if patch.TunnelName != nil && strings.TrimSpace(*patch.TunnelName) != "" {
		cfg.TunnelName = strings.TrimSpace(*patch.TunnelName)
	}
	if patch.TunnelDomain != nil {
		cfg.TunnelDomain = *patch.TunnelDomain
	}
	if patch.SupabaseURL != nil {
		cfg.SupabaseURL = *patch.SupabaseURL
	}
	if patch.SupabaseKey != nil {
		cfg.SupabaseKey = *patch.SupabaseKey
	}
	if patch.SupabaseServiceKey != nil {
		cfg.SupabaseServiceKey = *patch.SupabaseServiceKey
	}
	if patch.SupabaseTable != nil && strings.TrimSpace(*patch.SupabaseTable) != "" {
		cfg.SupabaseTable = *patch.SupabaseTable
	}
	if patch.SupabaseAutoPush != nil {
		cfg.SupabaseAutoPush = *patch.SupabaseAutoPush
	}
	if patch.SupabasePullLimit != nil {
		cfg.SupabasePullLimit = *patch.SupabasePullLimit
	}
	if patch.PushOrgCap != nil {
		cfg.PushOrgCap = *patch.PushOrgCap
	}
	if patch.PushCountryCap != nil {
		cfg.PushCountryCap = *patch.PushCountryCap
	}
	if patch.ScheduleMode != nil {
		cfg.ScheduleMode = strings.TrimSpace(*patch.ScheduleMode)
	}
	if patch.DailyAt != nil {
		cfg.DailyAt = strings.TrimSpace(*patch.DailyAt)
	}
	if patch.PipelineEnabled != nil {
		cfg.PipelineEnabled = *patch.PipelineEnabled
	}
	if patch.PipelinePullLimit != nil {
		cfg.PipelinePullLimit = *patch.PipelinePullLimit
	}
	if patch.PipelinePushLimit != nil {
		cfg.PipelinePushLimit = *patch.PipelinePushLimit
	}
	if patch.ProbeTLS != nil {
		cfg.ProbeTLS = *patch.ProbeTLS
	}
	if patch.ProbeAlt != nil {
		cfg.ProbeAlt = *patch.ProbeAlt
	}
	if patch.PushRequireTLS != nil {
		cfg.PushRequireTLS = *patch.PushRequireTLS
	}
	if patch.SupabasePruneDays != nil {
		cfg.SupabasePruneDays = *patch.SupabasePruneDays
	}
	if patch.AutoDisableSources != nil {
		cfg.AutoDisableSources = *patch.AutoDisableSources
	}
	if patch.ZeroStreakLimit != nil {
		cfg.ZeroStreakLimit = *patch.ZeroStreakLimit
	}
	if patch.CountryTargets != nil {
		cfg.CountryTargets = *patch.CountryTargets
	}
	if patch.ScheduleTimezone != nil {
		cfg.ScheduleTimezone = strings.TrimSpace(*patch.ScheduleTimezone)
	}
	if patch.TestIntervalHours != nil {
		cfg.TestIntervalHours = *patch.TestIntervalHours
	}
	if patch.AutoDisableLowYield != nil {
		cfg.AutoDisableLowYield = *patch.AutoDisableLowYield
	}
	if patch.LowYieldCycles != nil {
		cfg.LowYieldCycles = *patch.LowYieldCycles
	}
	if patch.LowYieldMinEntries != nil {
		cfg.LowYieldMinEntries = *patch.LowYieldMinEntries
	}
	if patch.CountryTopUp != nil {
		cfg.CountryTopUp = *patch.CountryTopUp
	}
	// Token bot boleh dikosongkan lewat API (mematikan notifikasi), tapi tidak pernah dikirim
	// balik ke klien — lihat publicSettings.
	if patch.TelegramBotToken != nil {
		cfg.TelegramBotToken = *patch.TelegramBotToken
	}
	if patch.TelegramChatID != nil {
		cfg.TelegramChatID = *patch.TelegramChatID
	}
	cfg.Normalize()
	*s.settings = cfg
	s.mu.Unlock()

	if err := cfg.Save(s.cfgPath); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "gagal menyimpan pengaturan: " + err.Error()})
		return
	}
	s.sched.Update(cfg)
	writeJSON(w, http.StatusOK, map[string]any{"settings": publicSettings(cfg), "runtime": s.runtimeInfo()})
}

// publicSettings membuang hash sandi sebelum dikirim ke klien.
func publicSettings(cfg settings.Settings) map[string]any {
	passwordSet := cfg.AdminPasswordHash != settings.DefaultPasswordHash()
	return map[string]any{
		"auto":                     cfg.Auto,
		"interval_minutes":         cfg.IntervalMinutes,
		"auto_rotate":              cfg.AutoRotate,
		"cap":                      cfg.Cap,
		"jobs":                     cfg.Jobs,
		"min_sources":              cfg.MinSources,
		"refresh_limit":            cfg.RefreshLimit,
		"refresh_after_minutes":    cfg.RefreshAfterMinutes,
		"fresh_minutes":            cfg.FreshMinutes,
		"purge_dead_hours":         cfg.PurgeDeadHours,
		"admin_user":               cfg.AdminUser,
		"admin_password_set":       passwordSet,
		"public_url":               cfg.PublicURL,
		"tunnel_name":              cfg.TunnelName,
		"tunnel_domain":            cfg.TunnelDomain,
		"supabase_url":             cfg.SupabaseURL,
		"supabase_key_set":         cfg.SupabaseKey != "",
		"supabase_service_key_set": cfg.SupabaseServiceKey != "",
		"supabase_table":           cfg.SupabaseTable,
		"supabase_auto_push":       cfg.SupabaseAutoPush,
		"supabase_pull_limit":      cfg.SupabasePullLimit,
		"push_org_cap":             cfg.PushOrgCap,
		"push_country_cap":         cfg.PushCountryCap,
		"schedule_mode":            cfg.ScheduleMode,
		"daily_at":                 cfg.DailyAt,
		"pipeline_enabled":         cfg.PipelineEnabled,
		"pipeline_pull_limit":      cfg.PipelinePullLimit,
		"pipeline_push_limit":      cfg.PipelinePushLimit,
		"probe_tls":                cfg.ProbeTLS,
		"probe_alt":                cfg.ProbeAlt,
		"push_require_tls":         cfg.PushRequireTLS,
		"supabase_prune_days":      cfg.SupabasePruneDays,
		"auto_disable_sources":     cfg.AutoDisableSources,
		"zero_streak_limit":        cfg.ZeroStreakLimit,
		"country_targets":          cfg.CountryTargets,
		"schedule_timezone":        cfg.ScheduleTimezone,
		"test_interval_hours":      cfg.TestIntervalHours,
		"auto_disable_low_yield":   cfg.AutoDisableLowYield,
		"low_yield_cycles":         cfg.LowYieldCycles,
		"low_yield_min_entries":    cfg.LowYieldMinEntries,
		"country_top_up":           cfg.CountryTopUp,
		"token":                    cfg.Token,
		"telegram_bot_token_set":   cfg.TelegramBotToken != "",
		"telegram_chat_id":         cfg.TelegramChatID,
	}
}

// handleAutoStart / handleAutoStop menyalakan atau mematikan siklus otomatis.
func (s *Server) handleAutoStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	cfg := s.currentSettings()
	cfg.Auto = true
	s.mu.Lock()
	*s.settings = cfg
	s.mu.Unlock()
	if err := cfg.Save(s.cfgPath); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := s.sched.Start(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"auto": true, "settings": publicSettings(cfg)})
}

func (s *Server) handleAutoStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	s.sched.Stop()
	cfg := s.currentSettings()
	cfg.Auto = false
	s.mu.Lock()
	*s.settings = cfg
	s.mu.Unlock()
	if err := cfg.Save(s.cfgPath); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"auto": false})
}

// handleNow memicu satu siklus sekarang memakai pengaturan yang tersimpan.
func (s *Server) handleNow(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	if err := s.sched.TriggerNow(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"started": true, "settings": publicSettings(s.currentSettings())})
}

// handlePurge menghapus catatan mati (housekeeping): yang lebih tua dari `hours`, plus
// kandidat yang sudah berkali-kali gagal dan belum pernah berhasil (sampah mass-scan).
func (s *Server) handlePurge(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	hours := 24
	if v, err := strconv.Atoi(r.URL.Query().Get("hours")); err == nil && v > 0 {
		hours = v
	}
	removed, err := s.store.DeleteDead(time.Duration(hours) * time.Hour)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	hopeless, err := s.store.DeleteHopeless(2, 5000)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"removed": removed, "older_than_hours": hours,
		"gagal_berulang": hopeless,
		"note":           "gagal_berulang = baris yang sudah >=2 kali diuji dan belum pernah hidup",
	})
}

// runtimeInfo menjelaskan alamat, alamat LAN, lokasi database, dan status penjadwal.
func (s *Server) runtimeInfo() map[string]any {
	cfg := s.currentSettings()
	enabled := 0
	if list, err := s.store.Sources(true); err == nil {
		enabled = len(list)
	}
	total := 0
	if list, err := s.store.Sources(false); err == nil {
		total = len(list)
	}
	return map[string]any{
		"addr":               s.addr,
		"lan_urls":           lanURLs(s.addr),
		"db":                 s.cfgPath,
		"auto_running":       s.sched.Running(),
		"auto_last_run":      s.sched.LastRun(),
		"api_token_required": cfg.Token != "",
		"sources":            total,
		"sources_enabled":    enabled,
		"supabase_ready":     cfg.SupabaseURL != "" && cfg.SupabaseKey != "",
	}
}

// lanURLs menghitung URL yang bisa dipakai dari perangkat lain di jaringan lokal.
func lanURLs(addr string) []string {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		port = "8787"
	}
	urls := []string{}
	ifaces, err := net.Interfaces()
	if err != nil {
		return urls
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipNet.IP.To4()
			if ip4 == nil || !ip4.IsPrivate() {
				continue
			}
			urls = append(urls, "http://"+net.JoinHostPort(ip4.String(), port))
		}
	}
	return urls
}

func (s *Server) handleProxies(w http.ResponseWriter, r *http.Request) {
	filter, err := filterFromQuery(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	list, err := s.store.List(filter)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(list), "proxies": list})
}

func (s *Server) handleProxiesText(w http.ResponseWriter, r *http.Request) {
	filter, err := filterFromQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	list, err := s.store.List(filter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for _, p := range list {
		fmt.Fprintln(w, p.Addr())
	}
}

// handleSources melaporkan daftar sumber (dari database) + hasil siklus terakhir.
//
//	GET  /api/sources      -> daftar + hasil terakhir
//	POST /api/sources      -> tambah sumber {name,url,kind,optional}
//	DELETE /api/sources    -> hapus sumber ?name=
func (s *Server) handleSources(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var body struct {
			Name     string `json:"name"`
			URL      string `json:"url"`
			Kind     string `json:"kind"`
			Optional bool   `json:"optional"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "body JSON tidak valid"})
			return
		}
		url := strings.TrimSpace(body.URL)
		if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "url wajib diawali http:// atau https://"})
			return
		}
		name := strings.TrimSpace(body.Name)
		if name == "" {
			name = autoSourceName(url)
		}
		kind := strings.TrimSpace(body.Kind)
		if kind == "" {
			kind = "txt"
		}
		if err := s.store.AddSource(store.SourceSeed{Name: name, URL: url, Kind: kind, Optional: body.Optional}); err != nil {
			writeJSON(w, http.StatusConflict, map[string]string{"error": "gagal menambah sumber: " + err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"added": name})
		return
	case http.MethodDelete:
		name := strings.TrimSpace(r.URL.Query().Get("name"))
		if name == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "butuh ?name="})
			return
		}
		removed, err := s.store.DeleteSource(name)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"removed": removed, "name": name})
		return
	}

	rows, err := s.store.Sources(false)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	enabledRows, _ := s.store.Sources(true)
	health, _ := s.store.SourceHealthList()
	lastResults := map[string]scraper.SourceStat{}
	for _, stat := range s.runner.Progress().Sources {
		lastResults[stat.Name] = stat
	}

	list := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		entry := map[string]any{
			"name":     row.Name,
			"url":      row.URL,
			"kind":     row.Kind,
			"optional": row.Optional,
			"enabled":  false,
		}
		for _, e := range enabledRows {
			if e.Name == row.Name {
				entry["enabled"] = true
			}
		}
		if h, ok := health[row.Name]; ok {
			entry["zero_streak"] = h.ZeroStreak
			entry["auto_disabled"] = h.AutoDisabled
			if h.LastError != "" {
				entry["last_error"] = h.LastError
			}
		}
		if stat, ok := lastResults[row.Name]; ok {
			entry["status"] = stat.Status
			entry["count"] = stat.Count
			entry["ms"] = stat.MS
			entry["bytes"] = stat.Bytes
			if stat.Error != "" {
				entry["error"] = stat.Error
			}
		}
		list = append(list, entry)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"count":   len(list),
		"enabled": len(enabledRows),
		"running": len(lastResults) > 0,
		"sources": list,
	})
}

// handleSourceToggle menyalakan/mematikan satu sumber tanpa menghapusnya.
func (s *Server) handleSourceToggle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	name := strings.TrimSpace(r.URL.Query().Get("name"))
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "butuh ?name="})
		return
	}
	enabled := true
	if v := r.URL.Query().Get("enabled"); v != "" {
		enabled = v == "true" || v == "1"
	}
	affected, err := s.store.SetSourceEnabled(name, enabled)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"updated": affected, "name": name, "enabled": enabled})
}

// autoSourceName membuat nama sumber dari URL kalau pengguna tidak mengisinya.
func autoSourceName(raw string) string {
	trimmed := strings.TrimPrefix(strings.TrimPrefix(raw, "https://"), "http://")
	trimmed = strings.ReplaceAll(trimmed, "/", "-")
	trimmed = strings.ReplaceAll(trimmed, "?", "-")
	trimmed = strings.ReplaceAll(trimmed, "&", "-")
	if len(trimmed) > 40 {
		trimmed = trimmed[:40]
	}
	return strings.Trim(trimmed, "-")
}

// handleScrape memicu satu siklus dengan opsi eksplisit (tanpa mengubah pengaturan).
// Parameter opsional: cap, offset, jobs, min_sources.
func (s *Server) handleScrape(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	opts := scraper.OptionsFromSettings(s.currentSettings())
	q := r.URL.Query()
	if v, err := strconv.Atoi(q.Get("cap")); err == nil && v > 0 {
		opts.Cap = v
	}
	if v, err := strconv.Atoi(q.Get("offset")); err == nil && v >= 0 {
		opts.Offset = v
	}
	if v, err := strconv.Atoi(q.Get("jobs")); err == nil && v > 0 && v <= 2000 {
		opts.Jobs = v
	}
	if v, err := strconv.Atoi(q.Get("min_sources")); err == nil && v >= 0 {
		opts.MinSources = v
	}
	if err := s.runner.Start(opts); err != nil {
		writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"started": true, "options": map[string]any{
		"cap": opts.Cap, "offset": opts.Offset, "jobs": opts.Jobs, "min_sources": opts.MinSources,
	}})
}

// handleStop menghentikan siklus yang sedang berjalan.
func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "pakai POST"})
		return
	}
	s.runner.Stop()
	writeJSON(w, http.StatusOK, map[string]any{"stopped": true})
}

// handleClash menghasilkan konfigurasi Clash/Mihomo siap pakai dengan worker VLESS.
// Parameter: host (wajib), uuid (wajib), port (default 443 = TLS), limit, path_mode.
func (s *Server) handleClash(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	host := strings.TrimSpace(q.Get("host"))
	uuid := strings.TrimSpace(q.Get("uuid"))
	if host == "" || uuid == "" {
		http.Error(w, "parameter host dan uuid wajib", http.StatusBadRequest)
		return
	}
	port := 443
	if v, err := strconv.Atoi(q.Get("port")); err == nil && v > 0 {
		port = v
	}
	tls := port == 443
	filter, err := filterFromQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if filter.Alive == nil {
		alive := true
		filter.Alive = &alive
	}
	s.requireTunnelQuality(q, port, &filter)
	list, err := s.store.List(filter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if len(list) == 0 {
		http.Error(w, "tidak ada proxy yang cocok", http.StatusNotFound)
		return
	}

	var b strings.Builder
	b.WriteString("# dibangkitkan oleh panproxyscanner\n")
	fmt.Fprintf(&b, "# worker: %s  port: %d  jumlah proxy: %d\n", host, port, len(list))
	b.WriteString("proxies:\n")
	names := make([]string, 0, len(list))
	for i, p := range list {
		name := fmt.Sprintf("%d %s %dms", i+1, p.Addr(), p.PingMS)
		name = strings.ReplaceAll(name, `"`, "")
		names = append(names, name)
		fmt.Fprintf(&b, "  - name: \"%s\"\n", name)
		b.WriteString("    type: vless\n")
		fmt.Fprintf(&b, "    server: %s\n", host)
		fmt.Fprintf(&b, "    port: %d\n", port)
		fmt.Fprintf(&b, "    uuid: %s\n", uuid)
		b.WriteString("    udp: true\n")
		fmt.Fprintf(&b, "    tls: %t\n", tls)
		b.WriteString("    network: ws\n")
		fmt.Fprintf(&b, "    servername: %s\n", host)
		b.WriteString("    ws-opts:\n")
		fmt.Fprintf(&b, "      path: \"/%s-%d\"\n", p.IP, p.Port)
		b.WriteString("      headers:\n")
		fmt.Fprintf(&b, "        Host: %s\n", host)
	}
	b.WriteString("proxy-groups:\n  - name: PROXY\n    type: url-test\n")
	b.WriteString("    url: \"http://cp.cloudflare.com/generate_204\"\n    interval: 300\n    proxies:\n")
	for _, name := range names {
		fmt.Fprintf(&b, "      - \"%s\"\n", name)
	}
	b.WriteString("rules:\n  - MATCH,PROXY\n")

	w.Header().Set("Content-Type", "text/yaml; charset=utf-8")
	w.Write([]byte(b.String()))
}

// requireTunnelQuality menerapkan syarat kemampuan tunnel pada keluaran VLESS. Panel
// memakai proxy ini untuk WebSocket ke worker, jadi proxy yang cuma lolos HTTP :80 akan
// menghasilkan konfigurasi mati di sisi pengguna:
//   - worker di port 443 → proxy wajib sanggup tunnel TLS 443 (`tls_ok`, hasil uji nyata)
//   - worker di port lain → proxy wajib sanggup tunnel port non-standar (`alt_ok`)
//
// Pemanggil masih bisa memaksa dengan `?tls=true` (wajib) atau `?tls=false` (jangan saring).
func (s *Server) requireTunnelQuality(q neturl.Values, port int, f *store.Filter) {
	if q.Has("tls") || q.Has("alt") {
		return
	}
	cfg := s.currentSettings()
	if !cfg.PushRequireTLS {
		return
	}
	if port == 443 {
		f.TLSOnly = true
		return
	}
	f.AltOnly = true
}

// handleRaw menghasilkan URI vless:// (format mentah seperti generator panel).
func (s *Server) handleRaw(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	host := strings.TrimSpace(q.Get("host"))
	uuid := strings.TrimSpace(q.Get("uuid"))
	if host == "" || uuid == "" {
		http.Error(w, "parameter host dan uuid wajib", http.StatusBadRequest)
		return
	}
	port := 443
	if v, err := strconv.Atoi(q.Get("port")); err == nil && v > 0 {
		port = v
	}
	security := "tls"
	if port != 443 {
		security = "none"
	}
	filter, err := filterFromQuery(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if filter.Alive == nil {
		alive := true
		filter.Alive = &alive
	}
	s.requireTunnelQuality(q, port, &filter)
	list, err := s.store.List(filter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	for _, p := range list {
		path := fmt.Sprintf("/%s-%d", p.IP, p.Port)
		label := fmt.Sprintf("%s %dms", p.Addr(), p.PingMS)
		fmt.Fprintf(w, "vless://%s@%s?encryption=none&type=ws&host=%s&security=%s&sni=%s&path=%s#%s\n",
			uuid, net.JoinHostPort(host, strconv.Itoa(port)), host, security, host, urlEscape(path), urlEscape(label))
	}
}

func urlEscape(s string) string {
	replacer := strings.NewReplacer("/", "%2F", " ", "%20", ":", "%3A", "#", "%23")
	return replacer.Replace(s)
}

func filterFromQuery(r *http.Request) (store.Filter, error) {
	q := r.URL.Query()
	f := store.Filter{Order: q.Get("order")}
	if v := q.Get("alive"); v != "" {
		alive := v != "false" && v != "0"
		f.Alive = &alive
	}
	f.Protocol = q.Get("protocol")
	f.Country = q.Get("country")
	f.TLSOnly = q.Get("tls") == "true" || q.Get("tls") == "1"
	f.AltOnly = q.Get("alt") == "true" || q.Get("alt") == "1"
	if v, err := strconv.Atoi(q.Get("max_ping")); err == nil {
		f.MaxPing = v
	}
	if v, err := strconv.Atoi(q.Get("min_sources")); err == nil {
		f.MinSources = v
	}
	if v, err := strconv.Atoi(q.Get("fresh_minutes")); err == nil {
		f.FreshMinutes = v
	}
	if v, err := strconv.Atoi(q.Get("limit")); err == nil {
		f.Limit = v
	}
	if v, err := strconv.Atoi(q.Get("offset")); err == nil {
		f.Offset = v
	}
	return f, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(payload)
}
