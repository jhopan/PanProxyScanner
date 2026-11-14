// Package store menyimpan hasil verifikasi proxy di SQLite (driver pure-Go, tanpa cgo).
package store

import (
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Proxy adalah satu baris tabel proxies.
type Proxy struct {
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Protocol    string `json:"protocol"`
	Country     string `json:"country"`
	TLS         bool   `json:"tls_ok"`
	AltOK       bool   `json:"alt_ok"`
	ExitIP      string `json:"exit_ip"`
	Alive       bool   `json:"alive"`
	PingMS      int    `json:"ping_ms"`
	LastChecked int64  `json:"last_checked"`
	FirstSeen   int64  `json:"first_seen"`
	OKCount     int    `json:"ok_count"`
	FailCount   int    `json:"fail_count"`
	Sources     int    `json:"sources"`
}

// Score memberi peringkat proxy: keberhasilan dulu, lalu ping.
func (p Proxy) Score() float64 {
	return float64(p.OKCount*100-p.FailCount*25) - float64(p.PingMS)/10
}

// Addr mengembalikan alamat siap dial ("ip:port", IPv6 diberi bracket).
func (p Proxy) Addr() string { return net.JoinHostPort(p.IP, strconv.Itoa(p.Port)) }

// Filter dipakai untuk query daftar proxy dari API.
type Filter struct {
	Alive        *bool
	Protocol     string
	Country      string
	TLSOnly      bool
	AltOnly      bool
	MaxPing      int
	MinSources   int
	FreshMinutes int
	// Since membatasi ke baris yang dicek setelah waktu ini (dipakai push delta: hanya yang
	// berubah sejak push terakhir). Zero = tanpa batas.
	Since  time.Time
	Limit  int
	Offset int
	Order  string // ping | last_checked | sources
}

// CountryCount adalah jumlah proxy hidup per negara.
type CountryCount struct {
	Country string `json:"country"`
	Alive   int    `json:"alive"`
	TLS     int    `json:"tls"` // hidup DAN sanggup TLS 443 — inilah yang dipakai worker
	Total   int    `json:"total"`
}

// Stats adalah ringkasan isi database.
type Stats struct {
	Total        int   `json:"total"`
	Alive        int   `json:"alive"`
	AliveFresh   int   `json:"alive_fresh"`
	Socks5       int   `json:"alive_socks5"`
	HTTP         int   `json:"alive_http"`
	AliveTLS     int   `json:"alive_tls"`
	AliveAlt     int   `json:"alive_alt"`
	AvgPingMS    int   `json:"avg_ping_ms"`
	LastChecked  int64 `json:"last_checked"`
	FreshMinutes int   `json:"fresh_minutes"`
}

// Store membungkus koneksi SQLite.
type Store struct {
	db *sql.DB
}

// Open membuka (dan membuat) database. WAL + busy_timeout supaya aman ditulis scraper
// sambil dibaca API.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // SQLite: satu penulis, hindari SQLITE_BUSY
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS proxies (
  ip           TEXT    NOT NULL,
  port         INTEGER NOT NULL,
  protocol     TEXT,
  country      TEXT,
  tls_ok       INTEGER NOT NULL DEFAULT 0,
  alt_ok       INTEGER NOT NULL DEFAULT 0,
  exit_ip      TEXT,
  alive        INTEGER NOT NULL DEFAULT 0,
  ping_ms      INTEGER,
  last_checked INTEGER NOT NULL,
  first_seen   INTEGER NOT NULL,
  ok_count     INTEGER NOT NULL DEFAULT 0,
  fail_count   INTEGER NOT NULL DEFAULT 0,
  sources      INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (ip, port)
);
CREATE INDEX IF NOT EXISTS idx_proxies_alive ON proxies(alive, last_checked DESC);
CREATE INDEX IF NOT EXISTS idx_proxies_last ON proxies(last_checked);
CREATE INDEX IF NOT EXISTS idx_proxies_ping ON proxies(alive, ping_ms);
`)
	if err != nil {
		return err
	}
	// Database lama (sebelum kolom country) tetap bisa dibuka: tambahkan kolomnya.
	var hasCountry bool
	rows, err := s.db.Query(`PRAGMA table_info(proxies)`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == "country" {
			hasCountry = true
		}
	}
	rows.Close()
	if !hasCountry {
		if _, err := s.db.Exec(`ALTER TABLE proxies ADD COLUMN country TEXT`); err != nil {
			return err
		}
	}
	if !columnExists(s.db, "proxies", "tls_ok") {
		if _, err := s.db.Exec(`ALTER TABLE proxies ADD COLUMN tls_ok INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	// hasil verifikasi v2: tunnel ke port non-standar + exit IP sebenarnya
	for _, col := range []struct{ name, ddl string }{
		{"alt_ok", `ALTER TABLE proxies ADD COLUMN alt_ok INTEGER NOT NULL DEFAULT 0`},
		{"exit_ip", `ALTER TABLE proxies ADD COLUMN exit_ip TEXT`},
	} {
		if !columnExists(s.db, "proxies", col.name) {
			if _, err := s.db.Exec(col.ddl); err != nil {
				return err
			}
		}
	}
	// Tabel sources harus dibuat SEBELUM migrasi kolomnya: pada database baru,
	// `ALTER TABLE sources ADD COLUMN` gagal kalau tabelnya belum ada.
	if _, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS sources (
  name     TEXT PRIMARY KEY,
  url      TEXT NOT NULL,
  kind     TEXT NOT NULL DEFAULT 'txt',
  optional INTEGER NOT NULL DEFAULT 0,
  enabled  INTEGER NOT NULL DEFAULT 1,
  added_at INTEGER NOT NULL
);
`); err != nil {
		return err
	}

	// meta: penanda kecil (tanggal scrape penuh terakhir, waktu push terakhir).
	if _, err := s.db.Exec(`CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`); err != nil {
		return err
	}

	// kesehatan sumber: berapa kali berturut-turut sebuah sumber tidak menghasilkan proxy
	for _, col := range []struct{ name, ddl string }{
		{"last_count", `ALTER TABLE sources ADD COLUMN last_count INTEGER NOT NULL DEFAULT 0`},
		{"zero_streak", `ALTER TABLE sources ADD COLUMN zero_streak INTEGER NOT NULL DEFAULT 0`},
		// yield_ok: berapa proxy dari sumber ini yang benar-benar lolos (hidup + sanggup TLS).
		// Sumber yang banyak entri tapi selalu nol hasil terlihat dari sini.
		{"yield_ok", `ALTER TABLE sources ADD COLUMN yield_ok INTEGER NOT NULL DEFAULT 0`},
		// cycles: berapa kali sumber ini sudah diambil; dasar penilaian yield (bukan sekadar kosong).
		{"cycles", `ALTER TABLE sources ADD COLUMN cycles INTEGER NOT NULL DEFAULT 0`},
		{"auto_disabled", `ALTER TABLE sources ADD COLUMN auto_disabled INTEGER NOT NULL DEFAULT 0`},
		{"last_error", `ALTER TABLE sources ADD COLUMN last_error TEXT`},
	} {
		if !columnExists(s.db, "sources", col.name) {
			if _, err := s.db.Exec(col.ddl); err != nil {
				return err
			}
		}
	}
	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_proxies_country ON proxies(country, alive)`)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_proxies_score ON proxies(alive, ok_count DESC, fail_count ASC, ping_ms ASC)`)
	return err
}

// columnExists memeriksa kolom di tabel (untuk migrasi ALTER TABLE yang idempoten).
func columnExists(db *sql.DB, table, column string) bool {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return false
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull, pk int
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false
		}
		if name == column {
			return true
		}
	}
	return false
}

// SourceSeed adalah sumber bawaan yang dipakai saat pertama kali (katalog di package sources).
type SourceSeed struct {
	Name     string
	URL      string
	Kind     string
	Optional bool
}

// Meta membaca nilai kecil non-rahasia (penanda harian, waktu push terakhir).
func (s *Store) Meta(key string) (string, error) {
	var value string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

// SetMeta menyimpan nilai kecil (upsert).
func (s *Store) SetMeta(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO meta (key, value) VALUES (?, ?)
		ON CONFLICT (key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// EnsureSources menambahkan sumber bawaan yang belum ada di database (tidak menghapus
// yang sudah diubah pengguna).
func (s *Store) EnsureSources(seeds []SourceSeed) error {
	now := time.Now().Unix()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, seed := range seeds {
		if _, err := tx.Exec(`
INSERT INTO sources (name, url, kind, optional, enabled, added_at) VALUES (?, ?, ?, ?, 1, ?)
ON CONFLICT(name) DO NOTHING`, seed.Name, seed.URL, seed.Kind, boolToInt(seed.Optional), now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Sources mengambil daftar sumber; enabledOnly membatasi ke yang aktif.
func (s *Store) Sources(enabledOnly bool) ([]SourceSeed, error) {
	query := `SELECT name, url, kind, optional FROM sources`
	if enabledOnly {
		query += ` WHERE enabled = 1`
	}
	query += ` ORDER BY name`
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SourceSeed
	for rows.Next() {
		var seed SourceSeed
		var optional int
		if err := rows.Scan(&seed.Name, &seed.URL, &seed.Kind, &optional); err != nil {
			return nil, err
		}
		seed.Optional = optional == 1
		out = append(out, seed)
	}
	return out, rows.Err()
}

// AddSource menambahkan sumber baru (nama wajib unik).
func (s *Store) AddSource(seed SourceSeed) error {
	_, err := s.db.Exec(`
INSERT INTO sources (name, url, kind, optional, enabled, added_at) VALUES (?, ?, ?, ?, 1, ?)`,
		seed.Name, seed.URL, seed.Kind, boolToInt(seed.Optional), time.Now().Unix())
	return err
}

// DeleteSource menghapus sumber berdasarkan nama. Sumber bawaan juga bisa dihapus.
func (s *Store) DeleteSource(name string) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM sources WHERE name = ?`, name)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// SetSourceEnabled menyalakan/mematikan sumber tanpa menghapusnya.
func (s *Store) SetSourceEnabled(name string, enabled bool) (int64, error) {
	res, err := s.db.Exec(`UPDATE sources SET enabled = ?, auto_disabled = 0, zero_streak = 0 WHERE name = ?`, boolToInt(enabled), name)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// SourceHealth adalah kondisi satu sumber setelah siklus terakhir.
type SourceHealth struct {
	Name         string `json:"name"`
	LastCount    int    `json:"last_count"`
	ZeroStreak   int    `json:"zero_streak"`
	AutoDisabled bool   `json:"auto_disabled"`
	LastError    string `json:"last_error,omitempty"`
	Enabled      bool   `json:"enabled"`
	YieldOK      int    `json:"yield_ok"` // proxy dari sumber ini yang lolos verifikasi
}

// UpdateSourceHealth mencatat hasil satu sumber pada siklus terakhir dan mematikan otomatis
// sumber yang berkali-kali tidak menghasilkan proxy. Mengembalikan daftar sumber yang baru
// dimatikan otomatis.
// UpdateSourceHealth mencatat kesehatan sumber. Selain "kosong berkali-kali", sumber yang
// berkali-kali memberi entri TAPI tidak pernah menghasilkan proxy yang lolos verifikasi juga
// dimatikan — itu tanda daftar mass-scan (ratusan ribu entri, hampir semuanya mati).
// Sumber yang dimatikan tetap bisa dinyalakan lagi dari halaman Sumber.
func (s *Store) UpdateSourceHealth(name string, count int, errText string, zeroStreakLimit, lowYieldCycles, lowYieldMinEntries int) (bool, error) {
	var err error
	autoDisabled := false
	if count > 0 {
		_, err = s.db.Exec(`UPDATE sources SET last_count = ?, zero_streak = 0, cycles = cycles + 1, last_error = NULL WHERE name = ?`, count, name)
		if err != nil {
			return false, err
		}
		if lowYieldCycles > 0 && lowYieldMinEntries > 0 {
			// Dinilai hanya kalau sumbernya memang rutin memberi entri; sumber kecil yang
			// kebetulan belum menghasilkan tidak dihukum.
			res, err := s.db.Exec(`
UPDATE sources SET enabled = 0, auto_disabled = 1
WHERE name = ? AND optional = 0 AND auto_disabled = 0
  AND ((cycles >= ? AND last_count >= ? AND yield_ok = 0)
       OR (cycles >= 2 AND last_count >= 5000 AND yield_ok = 0))`, name, lowYieldCycles, lowYieldMinEntries)
			if err != nil {
				return false, err
			}
			if n, _ := res.RowsAffected(); n > 0 {
				return true, nil
			}
		}
		return false, nil
	}
	_, err = s.db.Exec(`UPDATE sources SET last_count = 0, zero_streak = zero_streak + 1, last_error = ? WHERE name = ?`, errText, name)
	if err != nil {
		return false, err
	}
	if zeroStreakLimit > 0 {
		// hanya sumber wajib (bukan opsional) yang dimatikan otomatis
		res, err := s.db.Exec(`
UPDATE sources SET enabled = 0, auto_disabled = 1
WHERE name = ? AND optional = 0 AND auto_disabled = 0 AND zero_streak >= ?`, name, zeroStreakLimit)
		if err != nil {
			return false, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			autoDisabled = true
		}
	}
	return autoDisabled, nil
}

// SourceHealthList melaporkan kondisi semua sumber.
func (s *Store) SourceHealthList() (map[string]SourceHealth, error) {
	rows, err := s.db.Query(`SELECT name, COALESCE(last_count,0), COALESCE(zero_streak,0), COALESCE(auto_disabled,0), COALESCE(last_error,''), enabled, COALESCE(yield_ok,0) FROM sources`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]SourceHealth)
	for rows.Next() {
		var h SourceHealth
		var autoDisabled, enabled int
		if err := rows.Scan(&h.Name, &h.LastCount, &h.ZeroStreak, &autoDisabled, &h.LastError, &enabled, &h.YieldOK); err != nil {
			return nil, err
		}
		h.AutoDisabled = autoDisabled == 1
		h.Enabled = enabled == 1
		out[h.Name] = h
	}
	return out, rows.Err()
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

// RecordAlive mencatat hasil verifikasi yang hidup. `tlsOK` menandai tunnel ke 443 dengan
// TLS terverifikasi, `altOK` menandai tunnel ke port non-standar (8443/2053), dan `exitIP`
// adalah alamat keluar proxy yang sebenarnya (dari trace Cloudflare).
//
// Kedua flag kualitas disimpan sebagai **verdict terakhir**, bukan MAX: proxy yang kemarin
// sanggup TLS tapi hari ini tidak lagi harus langsung kehilangan label itu.
func (s *Store) RecordAlive(ip string, port int, protocol, country string, pingMS, sources int, tlsOK, altOK bool, exitIP string) error {
	now := time.Now().Unix()
	_, err := s.db.Exec(`
INSERT INTO proxies (ip, port, protocol, country, tls_ok, alt_ok, exit_ip, alive, ping_ms, last_checked, first_seen, ok_count, fail_count, sources)
VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, 1, 0, ?)
ON CONFLICT(ip, port) DO UPDATE SET
  protocol = excluded.protocol,
  country = CASE WHEN excluded.country <> '' THEN excluded.country ELSE proxies.country END,
  tls_ok = excluded.tls_ok,
  alt_ok = excluded.alt_ok,
  exit_ip = CASE WHEN excluded.exit_ip <> '' THEN excluded.exit_ip ELSE proxies.exit_ip END,
  alive = 1,
  ping_ms = CASE WHEN proxies.ping_ms IS NULL OR excluded.ping_ms <= 0 THEN excluded.ping_ms
                 ELSE CAST(0.6 * proxies.ping_ms + 0.4 * excluded.ping_ms AS INTEGER) END,
  last_checked = excluded.last_checked,
  ok_count = proxies.ok_count + 1,
  sources = MAX(proxies.sources, excluded.sources)`,
		ip, port, protocol, strings.ToUpper(country), boolToInt(tlsOK), boolToInt(altOK), exitIP, pingMS, now, now, sources)
	return err
}

// RecordDead mencatat hasil verifikasi yang mati.
func (s *Store) RecordDead(ip string, port int, country string, sources int) error {
	now := time.Now().Unix()
	_, err := s.db.Exec(`
INSERT INTO proxies (ip, port, country, alive, last_checked, first_seen, ok_count, fail_count, sources)
VALUES (?, ?, ?, 0, ?, ?, 0, 1, ?)
ON CONFLICT(ip, port) DO UPDATE SET
  country = CASE WHEN excluded.country <> '' THEN excluded.country ELSE proxies.country END,
  alive = 0,
  ping_ms = NULL,
  last_checked = excluded.last_checked,
  fail_count = proxies.fail_count + 1,
  sources = MAX(proxies.sources, excluded.sources)`,
		ip, port, strings.ToUpper(country), now, now, sources)
	return err
}

// Known mengembalikan peta "ip:port" yang sudah pernah tersimpan.
func (s *Store) Known() (map[string]bool, error) {
	rows, err := s.db.Query(`SELECT ip, port FROM proxies`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]bool, 4096)
	for rows.Next() {
		var ip string
		var port int
		if err := rows.Scan(&ip, &port); err != nil {
			return nil, err
		}
		out[fmt.Sprintf("%s:%d", ip, port)] = true
	}
	return out, rows.Err()
}

// Stale mengembalikan kandidat yang perlu diverifikasi ulang: catatan yang terakhir dicek
// lebih tua dari `olderThan`. Baris **hidup** didahulukan (itu produk yang dipakai panel),
// dan baris mati hanya diambil kalau umurnya lebih tua dari `deadOlderThan`.
//
// Alasannya: memverifikasi ulang ribuan kandidat mass-scan yang sudah mati hanya membakar
// waktu dan bandwidth — mereka sudah tercatat mati dan tinggal menunggu purge. Jatah
// refresh dihabiskan untuk proxy yang masih berguna.
func (s *Store) Stale(olderThan, deadOlderThan time.Duration, limit int) ([]Proxy, error) {
	cutoff := time.Now().Add(-olderThan).Unix()
	deadCutoff := time.Now().Add(-deadOlderThan).Unix()
	rows, err := s.db.Query(`
SELECT ip, port, COALESCE(protocol,''), COALESCE(country,''), tls_ok, alt_ok, COALESCE(exit_ip,''), alive, COALESCE(ping_ms,0), last_checked, first_seen, ok_count, fail_count, sources
FROM proxies
WHERE (alive = 1 AND last_checked < ?) OR (alive = 0 AND last_checked < ?)
ORDER BY alive DESC, last_checked ASC
LIMIT ?`, cutoff, deadCutoff, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProxies(rows)
}

// List mengambil daftar proxy sesuai filter.
func (s *Store) List(f Filter) ([]Proxy, error) {
	var where []string
	var args []any
	if f.Alive != nil {
		if *f.Alive {
			where = append(where, "alive = 1")
		} else {
			where = append(where, "alive = 0")
		}
	}
	if f.Protocol != "" {
		where = append(where, "protocol = ?")
		args = append(args, f.Protocol)
	}
	if f.Country != "" {
		codes := make([]string, 0, 4)
		for _, c := range strings.Split(f.Country, ",") {
			c = strings.ToUpper(strings.TrimSpace(c))
			if c != "" {
				codes = append(codes, c)
			}
		}
		if len(codes) == 1 {
			where = append(where, "country = ?")
			args = append(args, codes[0])
		} else if len(codes) > 1 {
			where = append(where, "country IN ("+strings.TrimSuffix(strings.Repeat("?,", len(codes)), ",")+")")
			for _, c := range codes {
				args = append(args, c)
			}
		}
	}
	if f.TLSOnly {
		where = append(where, "tls_ok = 1")
	}
	if f.AltOnly {
		where = append(where, "alt_ok = 1")
	}
	if f.MaxPing > 0 {
		where = append(where, "ping_ms IS NOT NULL AND ping_ms <= ?")
		args = append(args, f.MaxPing)
	}
	if f.MinSources > 0 {
		where = append(where, "sources >= ?")
		args = append(args, f.MinSources)
	}
	if !f.Since.IsZero() {
		where = append(where, "last_checked > ?")
		args = append(args, f.Since.UTC().Format(time.RFC3339))
	}
	if f.FreshMinutes > 0 {
		where = append(where, "last_checked >= ?")
		args = append(args, time.Now().Add(-time.Duration(f.FreshMinutes)*time.Minute).Unix())
	}
	order := "ping_ms IS NULL, ping_ms ASC"
	switch f.Order {
	case "last_checked":
		order = "last_checked DESC"
	case "sources":
		order = "sources DESC, ping_ms IS NULL, ping_ms ASC"
	case "score":
		// skor = keberhasilan tinggi, kegagalan rendah, ping kecil
		order = "(ok_count * 100 - fail_count * 25 - COALESCE(ping_ms, 99999) / 10) DESC, ping_ms IS NULL, ping_ms ASC"
	}
	query := `SELECT ip, port, COALESCE(protocol,''), COALESCE(country,''), tls_ok, alt_ok, COALESCE(exit_ip,''), alive, COALESCE(ping_ms,0), last_checked, first_seen, ok_count, fail_count, sources FROM proxies`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += " ORDER BY " + order
	limit := f.Limit
	if limit <= 0 || limit > 5000 {
		limit = 200
	}
	query += " LIMIT ? OFFSET ?"
	args = append(args, limit, f.Offset)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProxies(rows)
}

// Stats menghitung ringkasan, dengan jendela kesegaran `fresh` menit.
func (s *Store) Stats(fresh time.Duration) (Stats, error) {
	st := Stats{FreshMinutes: int(fresh.Minutes())}
	cutoff := time.Now().Add(-fresh).Unix()
	row := s.db.QueryRow(`
SELECT
  COUNT(*),
  COALESCE(SUM(alive), 0),
  COALESCE(SUM(CASE WHEN alive = 1 AND last_checked >= ? THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN alive = 1 AND protocol = 'socks5' THEN 1 ELSE 0 END), 0),
	COALESCE(SUM(CASE WHEN alive = 1 AND protocol = 'http' THEN 1 ELSE 0 END), 0),
	COALESCE(SUM(CASE WHEN alive = 1 AND tls_ok = 1 THEN 1 ELSE 0 END), 0),
	COALESCE(SUM(CASE WHEN alive = 1 AND alt_ok = 1 THEN 1 ELSE 0 END), 0),
	COALESCE(CAST(AVG(CASE WHEN alive = 1 THEN ping_ms END) AS INTEGER), 0),
	COALESCE(MAX(last_checked), 0)
FROM proxies`, cutoff)
	if err := row.Scan(&st.Total, &st.Alive, &st.AliveFresh, &st.Socks5, &st.HTTP, &st.AliveTLS, &st.AliveAlt, &st.AvgPingMS, &st.LastChecked); err != nil {
		return st, err
	}
	return st, nil
}

// Countries menghitung proxy per negara (hanya yang punya kode negara).
func (s *Store) Countries(aliveOnly bool) ([]CountryCount, error) {
	query := `SELECT COALESCE(country,''), COALESCE(SUM(alive),0), COALESCE(SUM(CASE WHEN alive = 1 AND tls_ok = 1 THEN 1 ELSE 0 END),0), COUNT(*) FROM proxies WHERE country IS NOT NULL AND country <> '' GROUP BY country ORDER BY 2 DESC, 4 DESC LIMIT 200`
	if aliveOnly {
		query = `SELECT COALESCE(country,''), COALESCE(SUM(alive),0), COALESCE(SUM(CASE WHEN alive = 1 AND tls_ok = 1 THEN 1 ELSE 0 END),0), COUNT(*) FROM proxies WHERE country IS NOT NULL AND country <> '' AND alive = 1 GROUP BY country ORDER BY 2 DESC, 4 DESC LIMIT 200`
	}
	rows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CountryCount
	for rows.Next() {
		var c CountryCount
		if err := rows.Scan(&c.Country, &c.Alive, &c.TLS, &c.Total); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// DeleteDead menghapus catatan mati yang sudah lama tidak dicek (housekeeping).
func (s *Store) DeleteDead(olderThan time.Duration) (int64, error) {
	cutoff := time.Now().Add(-olderThan).Unix()
	res, err := s.db.Exec(`DELETE FROM proxies WHERE alive = 0 AND last_checked < ?`, cutoff)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// DeleteHopeless menghapus kandidat yang sudah beberapa kali diuji dan **belum pernah**
// berhasil. Daftar mass-scan menyumbang mayoritas baris seperti ini: sekali gagal bisa jadi
// gangguan jaringan kita, dua kali gagal di siklus berbeda artinya proxy itu memang mati.
// Hasilnya database tetap ramping dan siklus tidak menghabiskan waktu untuk sampah.
func (s *Store) DeleteHopeless(minFail int, limit int) (int64, error) {
	if minFail < 2 {
		minFail = 2
	}
	if limit <= 0 {
		limit = 5000
	}
	res, err := s.db.Exec(`
DELETE FROM proxies WHERE rowid IN (
  SELECT rowid FROM proxies
  WHERE alive = 0 AND ok_count = 0 AND fail_count >= ?
  ORDER BY fail_count DESC, last_checked ASC
  LIMIT ?
)`, minFail, limit)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func scanProxies(rows *sql.Rows) ([]Proxy, error) {
	var out []Proxy
	for rows.Next() {
		var p Proxy
		var alive, tlsOK, altOK int
		if err := rows.Scan(&p.IP, &p.Port, &p.Protocol, &p.Country, &tlsOK, &altOK, &p.ExitIP, &alive, &p.PingMS, &p.LastChecked, &p.FirstSeen, &p.OKCount, &p.FailCount, &p.Sources); err != nil {
			return nil, err
		}
		p.Alive = alive == 1
		p.TLS = tlsOK == 1
		p.AltOK = altOK == 1
		out = append(out, p)
	}
	return out, rows.Err()
}

// AddSourceYield menambah hitungan hasil (proxy yang lolos verifikasi) untuk satu sumber.
func (s *Store) AddSourceYield(name string, delta int) error {
	if name == "" || delta == 0 {
		return nil
	}
	_, err := s.db.Exec("UPDATE sources SET yield_ok = yield_ok + ? WHERE name = ?", delta, name)
	return err
}

// SourceYields mengembalikan hitungan hasil per sumber (untuk memberi bobot pada kandidat:
// kandidat dari sumber yang produktif diverifikasi lebih dulu).
func (s *Store) SourceYields() (map[string]int, error) {
	rows, err := s.db.Query("SELECT name, COALESCE(yield_ok,0) FROM sources")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int)
	for rows.Next() {
		var name string
		var n int
		if err := rows.Scan(&name, &n); err != nil {
			return nil, err
		}
		out[name] = n
	}
	return out, rows.Err()
}

// ThinCountryCandidates mengambil baris tersimpan dari negara tertentu yang belum terbukti
// sanggup TLS, untuk diverifikasi ulang. Dipakai pass tambahan per negara: tanpa ini, negara
// yang stoknya tipis hanya bergantung pada kandidat baru yang kebetulan berlabel negara itu.
func (s *Store) ThinCountryCandidates(countries []string, limit int, olderThan time.Duration) ([]string, error) {
	if len(countries) == 0 || limit <= 0 {
		return nil, nil
	}
	codes := make([]string, 0, len(countries))
	args := make([]any, 0, len(countries)+3)
	for _, code := range countries {
		code = strings.ToUpper(strings.TrimSpace(code))
		if len(code) != 2 {
			continue
		}
		codes = append(codes, code)
		args = append(args, code)
	}
	if len(codes) == 0 {
		return nil, nil
	}
	mark := strings.TrimSuffix(strings.Repeat("?,", len(codes)), ",")
	query := "SELECT ip, port FROM proxies WHERE tls_ok = 0 AND country IN (" + mark + ")"
	if olderThan > 0 {
		query += " AND (last_checked IS NULL OR last_checked < ?)"
		args = append(args, time.Now().Add(-olderThan).UTC().Format(time.RFC3339))
	}
	query += " ORDER BY last_checked IS NULL DESC, last_checked ASC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var ip, port string
		if err := rows.Scan(&ip, &port); err != nil {
			return nil, err
		}
		out = append(out, ip+":"+port)
	}
	return out, rows.Err()
}
