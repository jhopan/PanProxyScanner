// Package api menyediakan website + API HTTP untuk daftar proxy hasil scraping.
package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jhopan/Pan-ProxyScanner/internal/auth"
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
}

// New membuat server. `cfgPath` adalah lokasi settings.json (dipakai saat menyimpan).
func New(st *store.Store, runner *scraper.Runner, sched *scraper.Scheduler, cfg *settings.Settings, cfgPath, addr string) *Server {
	return &Server{store: st, runner: runner, sched: sched, settings: cfg, sessions: auth.New(), cfgPath: cfgPath, addr: addr}
}

// SetEnvPath mencatat berkas .env yang dibaca saat startup (ditampilkan di halaman Tunnel).
func (s *Server) SetEnvPath(path string) { s.envPath = path }

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
	mux.HandleFunc("/api/supabase/status", s.handleSupabaseStatus)
	mux.HandleFunc("/api/supabase/test", s.handleSupabaseTest)
	mux.HandleFunc("/api/supabase/push", s.handleSupabasePush)
	mux.HandleFunc("/api/supabase/pull-verify", s.handleSupabasePullVerify)
	mux.HandleFunc("/api/supabase/prune", s.handleSupabasePrune)
	mux.HandleFunc("/api/vless/clash", s.handleClash)
	mux.HandleFunc("/api/vless/raw", s.handleRaw)
	return s.withCommon(mux)
}

// withCommon menambahkan CORS, lalu mewajibkan login (cookie sesi) atau token API.
//
//   - /login dan /api/login terbuka.
//   - /api/health terbuka supaya bisa dipakai monitoring/tunnel check.
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
		public := path == "/login" || path == "/api/login" || path == "/api/health"
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

// handleSupabaseStatus melaporkan konfigurasi + hasil sinkron terakhir. Panggilan cek
// ke Supabase hanya dilakukan kalau diminta (?test=1) supaya endpoint ini selalu cepat —
// pemanggilan langsung ke Supabase bisa lambat/terbatas rate.
func (s *Server) handleSupabaseStatus(w http.ResponseWriter, r *http.Request) {
	cfg := s.currentSettings()
	client := s.supabaseClient()
	s.stateMu.Lock()
	status := s.lastSync
	s.stateMu.Unlock()
	payload := map[string]any{
		"configured":           client.Configured(),
		"url":                  cfg.SupabaseURL,
		"table":                cfg.SupabaseTable,
		"has_key":              cfg.SupabaseKey != "",
		"auto_push":            cfg.SupabaseAutoPush,
		"pull_limit":           cfg.SupabasePullLimit,
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
	client := s.supabaseClient()
	if !client.Configured() {
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
	edgeDead, edgeErr := client.PullEdgeVerdicts(r.Context(), 5000)
	if edgeErr != nil {
		edgeDead = nil // tabel tanpa kolom tunnel_ok: lanjut tanpa filter edge
	}
	pushed, skippedEdge, err := s.pushToSupabase(r.Context(), client, cfg, limit, edgeDead)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"pushed": pushed, "held_edge_dead": skippedEdge, "table": client.Table})
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

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	candidates, _ := client.CountDead(ctx, time.Now().Add(-time.Duration(days)*24*time.Hour))
	deleted, err := s.pruneSupabase(ctx, client, days, limit)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	payload := map[string]any{
		"deleted": deleted, "older_than_days": days, "kandidat_sebelumnya": candidates,
		"note": "jalankan berulang sampai deleted 0 kalau kandidat lebih banyak dari limit",
	}
	if candidates > 0 && deleted == 0 {
		payload["note"] = "0 baris terhapus padahal ada kandidat — biasanya policy RLS untuk DELETE belum ada; jalankan docs/supabase-policies.sql di Supabase SQL Editor"
		payload["hint"] = "butuh policy: create policy ... on proxy_pool for delete to anon using (true);"
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
	client := s.supabaseClient()
	if !client.Configured() {
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
	summary, err := s.pullVerify(r.Context(), client, opts, limit)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, summary)
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
func (s *Server) pushToSupabase(ctx context.Context, client *supabase.Client, cfg settings.Settings, limit int, edgeDead map[string]bool) (int, int, error) {
	alive := true
	fresh := pushFreshMinutes(cfg)
	promote, err := s.store.List(store.Filter{Alive: &alive, Limit: limit, Order: "score", FreshMinutes: fresh, TLSOnly: cfg.PushRequireTLS})
	if err != nil {
		return 0, 0, err
	}
	demoteCap := limit
	if demoteCap < 2000 {
		demoteCap = 2000
	}
	candidates, err := s.store.List(store.Filter{Alive: &alive, Limit: demoteCap, Order: "score", FreshMinutes: fresh})
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
	skippedYoung, skippedDup, skippedEdge := 0, 0, 0
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
		if key := exitKey(p.ExitIP); key != "" {
			if seenExit[key] {
				skippedDup++
				continue
			}
			seenExit[key] = true
		}
		rows = append(rows, supabase.Row{
			IP: p.IP, Port: strconv.Itoa(p.Port), Country: p.Country,
			IsActive: true, LastChecked: now, Ping: p.PingMS,
		}.EvidenceRow(p.TLS, p.AltOK, p.ExitIP))
	}
	demoted := 0
	if cfg.PushRequireTLS {
		for _, p := range candidates {
			if p.TLS {
				continue
			}
			rows = append(rows, supabase.Row{
				IP: p.IP, Port: strconv.Itoa(p.Port), Country: p.Country,
				IsActive: false, LastChecked: now, Ping: p.PingMS,
			}.EvidenceRow(false, p.AltOK, p.ExitIP))
			demoted++
		}
	}
	if len(rows) == 0 {
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
	note := fmt.Sprintf("push %d baris (naik %d, turun %d, wajib TLS: %v, verdict segar ≤ %d menit",
		sent, len(promote)-skippedYoung-skippedDup, demoted, cfg.PushRequireTLS, fresh)
	if skippedYoung > 0 {
		note += fmt.Sprintf(", %d ditahan (baru 1× berhasil)", skippedYoung)
	}
	if skippedDup > 0 {
		note += fmt.Sprintf(", %d dilewati (exit IP sama)", skippedDup)
	}
	if skippedEdge > 0 {
		note += fmt.Sprintf(", %d ditahan (edge bilang mati)", skippedEdge)
	}
	if client.EvidenceColumns() {
		note += ", kolom tls_ok/alt_ok/exit_ip ikut dikirim"
	} else {
		note += ", kolom bukti belum ada di tabel → jalankan docs/supabase-policies.sql"
	}
	s.recordSync(note+")", err)
	return sent, skippedEdge, err
}

// pruneSupabase menghapus baris mati lama di Supabase (housekeeping).
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
			}.EvidenceRow(v.TLS, v.AltOK, v.ExitIP))
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

// pullVerify menarik baris dari Supabase dan memverifikasinya di sini.
func (s *Server) pullVerify(ctx context.Context, client *supabase.Client, opts scraper.Options, limit int) (scraper.Summary, error) {
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
	summary, err := scraper.VerifyRemote(ctx, s.store, supabaseSink{client: client, requireTLS: s.currentSettings().PushRequireTLS}, remote, opts, s.runner)
	if err == nil {
		if total, alive, testErr := client.Test(ctx); testErr == nil {
			s.setLastRemote(total, alive)
		}
	}
	s.recordSync(fmt.Sprintf("verifikasi %d baris Supabase (hidup %d, mati %d)", summary.Checked, summary.Alive, summary.Dead), err)
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
//
// Dijalankan oleh penjadwal (mode interval/harian saat pipeline diaktifkan) atau manual
// lewat POST /api/pipeline/run.
// RunPipeline menjalankan pipeline penuh (dipakai tombol manual dan jadwal scrape).
func (s *Server) RunPipeline() error {
	return s.RunPipelineMode(false)
}

// RunPipelineMode menjalankan pipeline. testOnly=true melewati langkah scrape sumber baru:
// tarik-verifikasi Supabase, push bukti terbaru, dan housekeeping tetap jalan. Dipakai jadwal
// uji berkala (6 jam) supaya pool terjaga tanpa memakan sumber setiap kali.
func (s *Server) RunPipelineMode(testOnly bool) error {
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

	go func() {
		defer s.pipelineRun.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
		defer cancel()
		cfg := s.currentSettings()
		client := s.supabaseClient()

		// --- langkah 1: tarik & verifikasi baris Supabase ---
		pullLimit := cfg.PipelinePullLimit
		if pullLimit <= 0 {
			pullLimit = cfg.SupabasePullLimit
		}
		if client.Configured() && pullLimit > 0 {
			started := time.Now()
			summary, err := s.pullVerify(ctx, client, scraper.OptionsFromSettings(cfg), pullLimit)
			step := PipelineStep{
				Name: "tarik & verifikasi Supabase", OK: err == nil, MS: time.Since(started).Milliseconds(),
				Counts: map[string]int{"dicek": summary.Checked, "hidup": summary.Alive, "mati": summary.Dead, "error": summary.Errors},
			}
			if err != nil {
				step.Note = err.Error()
			} else {
				step.Note = fmt.Sprintf("%d baris diuji, %d masih hidup", summary.Checked, summary.Alive)
			}
			s.addPipelineStep(step)
		} else {
			s.addPipelineStep(PipelineStep{Name: "tarik & verifikasi Supabase", OK: true, Note: "dilewati (belum dikonfigurasi / limit 0)"})
		}

		// --- langkah 2: scrape sumber lalu verifikasi ---
		if testOnly {
			s.addPipelineStep(PipelineStep{Name: "scrape sumber & verifikasi", OK: true, Note: "dilewati (mode tes berkala)"})
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

		}

		// --- langkah 3: push hasil ke Supabase ---
		if client.Configured() && cfg.PipelinePushLimit > 0 {
			started := time.Now()
			edgeDead, edgeErr := client.PullEdgeVerdicts(ctx, 5000)
			if edgeErr != nil {
				edgeDead = nil
			}
			pushed, skippedEdge, err := s.pushToSupabase(ctx, client, cfg, cfg.PipelinePushLimit, edgeDead)
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
			s.addPipelineStep(step)
		} else {
			s.addPipelineStep(PipelineStep{Name: "push ke Supabase", OK: true, Note: "dilewati (belum dikonfigurasi / limit 0)"})
		}

		// --- langkah 4: housekeeping baris mati di Supabase ---
		if client.Configured() && cfg.SupabasePruneDays > 0 {
			started := time.Now()
			deleted, err := s.pruneSupabase(ctx, client, cfg.SupabasePruneDays, 2000)
			step := PipelineStep{Name: "prune baris mati Supabase", OK: err == nil, MS: time.Since(started).Milliseconds(), Counts: map[string]int{"dihapus": deleted, "hari": cfg.SupabasePruneDays}}
			if err != nil {
				step.Note = err.Error()
			} else {
				step.Note = fmt.Sprintf("%d baris mati (> %d hari) dihapus", deleted, cfg.SupabasePruneDays)
			}
			s.addPipelineStep(step)
		} else {
			s.addPipelineStep(PipelineStep{Name: "prune baris mati Supabase", OK: true, Note: "dilewati (supabase_prune_days = 0)"})
		}

		// --- langkah 5: housekeeping database lokal ---
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

		s.setPipeline(func(p *PipelineStatus) {
			p.Running = false
			p.EndedAt = time.Now()
		})
	}()
	return nil
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
	client := s.supabaseClient()
	if !client.Configured() {
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
			edgeDead, _ := client.PullEdgeVerdicts(ctx, 5000)
			if pushed, heldEdge, err := s.pushToSupabase(ctx, client, cfg, 1000, edgeDead); err != nil {
				log.Printf("supabase push gagal: %v", err)
			} else {
				log.Printf("supabase: %d proxy didorong (%d ditahan: edge bilang mati)", pushed, heldEdge)
			}
		}
		if cfg.SupabasePullLimit > 0 {
			summary, err := s.pullVerify(ctx, client, scraper.OptionsFromSettings(cfg), cfg.SupabasePullLimit)
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
		SupabaseTable       *string         `json:"supabase_table"`
		SupabaseAutoPush    *bool           `json:"supabase_auto_push"`
		SupabasePullLimit   *int            `json:"supabase_pull_limit"`
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
	if patch.SupabaseTable != nil && strings.TrimSpace(*patch.SupabaseTable) != "" {
		cfg.SupabaseTable = *patch.SupabaseTable
	}
	if patch.SupabaseAutoPush != nil {
		cfg.SupabaseAutoPush = *patch.SupabaseAutoPush
	}
	if patch.SupabasePullLimit != nil {
		cfg.SupabasePullLimit = *patch.SupabasePullLimit
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
		"auto":                  cfg.Auto,
		"interval_minutes":      cfg.IntervalMinutes,
		"auto_rotate":           cfg.AutoRotate,
		"cap":                   cfg.Cap,
		"jobs":                  cfg.Jobs,
		"min_sources":           cfg.MinSources,
		"refresh_limit":         cfg.RefreshLimit,
		"refresh_after_minutes": cfg.RefreshAfterMinutes,
		"fresh_minutes":         cfg.FreshMinutes,
		"purge_dead_hours":      cfg.PurgeDeadHours,
		"admin_user":            cfg.AdminUser,
		"admin_password_set":    passwordSet,
		"public_url":            cfg.PublicURL,
		"tunnel_name":           cfg.TunnelName,
		"tunnel_domain":         cfg.TunnelDomain,
		"supabase_url":          cfg.SupabaseURL,
		"supabase_key_set":      cfg.SupabaseKey != "",
		"supabase_table":        cfg.SupabaseTable,
		"supabase_auto_push":    cfg.SupabaseAutoPush,
		"supabase_pull_limit":   cfg.SupabasePullLimit,
		"schedule_mode":         cfg.ScheduleMode,
		"daily_at":              cfg.DailyAt,
		"pipeline_enabled":      cfg.PipelineEnabled,
		"pipeline_pull_limit":   cfg.PipelinePullLimit,
		"pipeline_push_limit":   cfg.PipelinePushLimit,
		"probe_tls":             cfg.ProbeTLS,
		"probe_alt":             cfg.ProbeAlt,
		"push_require_tls":      cfg.PushRequireTLS,
		"supabase_prune_days":   cfg.SupabasePruneDays,
		"auto_disable_sources":  cfg.AutoDisableSources,
		"zero_streak_limit":     cfg.ZeroStreakLimit,
		"country_targets":       cfg.CountryTargets,
		"schedule_timezone":     cfg.ScheduleTimezone,
		"test_interval_hours":   cfg.TestIntervalHours,
		"token":                 cfg.Token,
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
