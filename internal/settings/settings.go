// Package settings menyimpan pengaturan yang bisa diubah dari website (disimpan sebagai
// settings.json di samping database) dan menerjemahkannya ke opsi scraper.
package settings

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Settings adalah konfigurasi yang bisa diubah tanpa restart.
type Settings struct {
	Auto                bool   `json:"auto"`                  // siklus otomatis aktif
	IntervalMinutes     int    `json:"interval_minutes"`      // jarak antar siklus (mode interval)
	ScheduleMode        string `json:"schedule_mode"`         // "interval" atau "daily"
	DailyAt             string `json:"daily_at"`              // "HH:MM" untuk mode daily (waktu lokal)
	PipelineEnabled     bool   `json:"pipeline_enabled"`      // siklus otomatis menjalankan pipeline penuh
	PipelinePullLimit   int    `json:"pipeline_pull_limit"`   // baris Supabase yang diuji di awal pipeline (0 = pakai supabase_pull_limit)
	PipelinePushLimit   int    `json:"pipeline_push_limit"`   // proxy yang dikirim ke Supabase di akhir pipeline
	ProbeTLS            bool   `json:"probe_tls"`             // uji tambahan tunnel 443 dengan TLS terverifikasi
	ProbeAlt            bool   `json:"probe_alt"`             // uji tambahan port non-standar (8443/2053)
	PushRequireTLS      bool   `json:"push_require_tls"`      // hanya kirim proxy yang sanggup TLS 443 ke Supabase
	SupabasePruneDays   int    `json:"supabase_prune_days"`   // hapus baris mati di Supabase yang lebih tua dari N hari (0 = tidak)
	AutoDisableSources  bool   `json:"auto_disable_sources"`  // matikan sumber yang berkali-kali kosong
	ZeroStreakLimit     int    `json:"zero_streak_limit"`     // berapa kali kosong berturut-turut sebelum dimatikan
	AutoRotate          bool   `json:"auto_rotate"`           // geser slice tiap siklus
	Cap                 int    `json:"cap"`                   // kandidat baru per siklus
	Jobs                int    `json:"jobs"`                  // verifikasi paralel
	MinSources          int    `json:"min_sources"`           // hanya kandidat muncul di >= N sumber
	RefreshLimit        int    `json:"refresh_limit"`         // baris lama yang diverifikasi ulang
	RefreshAfterMinutes int    `json:"refresh_after_minutes"` // umur catatan sebelum diverifikasi ulang
	FreshMinutes        int    `json:"fresh_minutes"`         // jendela kesegaran statistik/API
	PurgeDeadHours      int    `json:"purge_dead_hours"`      // hapus catatan mati lebih tua dari N jam (0 = tidak)
	AdminUser           string `json:"admin_user"`            // user untuk login website
	AdminPasswordHash   string `json:"admin_password_hash"`   // SHA-256 hex; tidak pernah dikirim ke klien
	PublicURL           string `json:"public_url"`            // URL publik (mis. hasil CF Tunnel), opsional
	TunnelName          string `json:"tunnel_name"`           // nama tunnel Cloudflare
	TunnelDomain        string `json:"tunnel_domain"`         // domain yang diarahkan ke panel
	SupabaseURL         string `json:"supabase_url"`          // URL project Supabase (untuk sinkron proxy_pool)
	SupabaseKey         string `json:"supabase_key"`          // anon/service key Supabase
	SupabaseTable       string `json:"supabase_table"`        // tabel tujuan (default proxy_pool)
	SupabaseAutoPush    bool   `json:"supabase_auto_push"`    // dorong hasil siklus ke Supabase otomatis
	SupabasePullLimit   int    `json:"supabase_pull_limit"`   // baris Supabase yang diverifikasi ulang per siklus (0 = tidak)
	Token               string `json:"token,omitempty"`       // kalau diisi, API wajib token (untuk klien mesin)
}

// DefaultPasswordHash adalah hash dari sandi bawaan "admin" (admin/admin).
func DefaultPasswordHash() string {
	sum := sha256.Sum256([]byte("admin"))
	return hex.EncodeToString(sum[:])
}

// HashPassword menghasilkan SHA-256 hex dari sandi.
func HashPassword(password string) string {
	sum := sha256.Sum256([]byte(password))
	return hex.EncodeToString(sum[:])
}

// Default mengembalikan pengaturan awal yang wajar.
func Default() Settings {
	return Settings{
		Auto:                false,
		IntervalMinutes:     30,
		ScheduleMode:        "interval",
		DailyAt:             "05:30",
		PipelineEnabled:     false,
		PipelinePullLimit:   500,
		PipelinePushLimit:   1000,
		ProbeTLS:            true,
		ProbeAlt:            true,
		PushRequireTLS:      true,
		SupabasePruneDays:   7,
		AutoDisableSources:  true,
		ZeroStreakLimit:     3,
		AutoRotate:          true,
		Cap:                 20000,
		Jobs:                150,
		MinSources:          0,
		RefreshLimit:        300,
		RefreshAfterMinutes: 30,
		FreshMinutes:        360,
		PurgeDeadHours:      0,
		AdminUser:           "admin",
		AdminPasswordHash:   DefaultPasswordHash(),
		PublicURL:           "",
		TunnelName:          "panproxyscanner",
		TunnelDomain:        "",
		SupabaseURL:         "",
		SupabaseKey:         "",
		SupabaseTable:       "proxy_pool",
		SupabaseAutoPush:    false,
		SupabasePullLimit:   0,
	}
}

// Normalize memperbaiki nilai yang tidak masuk akal dari input pengguna.
func (s *Settings) Normalize() {
	d := Default()
	if s.IntervalMinutes < 1 {
		s.IntervalMinutes = d.IntervalMinutes
	}
	if s.IntervalMinutes > 24*60 {
		s.IntervalMinutes = 24 * 60
	}
	if s.Cap < 50 {
		s.Cap = 50
	}
	if s.Cap > 500000 {
		s.Cap = 500000
	}
	if s.Jobs < 1 {
		s.Jobs = d.Jobs
	}
	if s.Jobs > 2000 {
		s.Jobs = 2000
	}
	if s.MinSources < 0 {
		s.MinSources = 0
	}
	if s.RefreshLimit < 0 {
		s.RefreshLimit = 0
	}
	if s.RefreshLimit > 50000 {
		s.RefreshLimit = 50000
	}
	if s.RefreshAfterMinutes < 1 {
		s.RefreshAfterMinutes = d.RefreshAfterMinutes
	}
	if s.FreshMinutes < 1 {
		s.FreshMinutes = d.FreshMinutes
	}
	if s.PurgeDeadHours < 0 {
		s.PurgeDeadHours = 0
	}
	if strings.TrimSpace(s.AdminUser) == "" {
		s.AdminUser = d.AdminUser
	}
	if len(s.AdminPasswordHash) != 64 {
		s.AdminPasswordHash = d.AdminPasswordHash
	}
	s.PublicURL = strings.TrimRight(strings.TrimSpace(s.PublicURL), "/")
	s.TunnelName = strings.TrimSpace(s.TunnelName)
	if s.TunnelName == "" {
		s.TunnelName = d.TunnelName
	}
	s.TunnelDomain = strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(s.TunnelDomain), "https://"), "http://")
	s.TunnelDomain = strings.TrimRight(s.TunnelDomain, "/")
	s.SupabaseURL = strings.TrimRight(strings.TrimSpace(s.SupabaseURL), "/")
	s.SupabaseKey = strings.TrimSpace(s.SupabaseKey)
	s.SupabaseTable = strings.TrimSpace(s.SupabaseTable)
	if s.SupabaseTable == "" {
		s.SupabaseTable = d.SupabaseTable
	}
	if s.SupabasePullLimit < 0 {
		s.SupabasePullLimit = 0
	}
	if s.SupabasePullLimit > 20000 {
		s.SupabasePullLimit = 20000
	}
	if s.ScheduleMode != "daily" {
		s.ScheduleMode = "interval"
	}
	if !validClock(s.DailyAt) {
		s.DailyAt = d.DailyAt
	}
	if s.PipelinePullLimit < 0 {
		s.PipelinePullLimit = 0
	}
	if s.PipelinePullLimit > 20000 {
		s.PipelinePullLimit = 20000
	}
	if s.PipelinePushLimit < 0 {
		s.PipelinePushLimit = 0
	}
	if s.PipelinePushLimit > 20000 {
		s.PipelinePushLimit = 20000
	}
	if s.SupabasePruneDays < 0 {
		s.SupabasePruneDays = 0
	}
	if s.SupabasePruneDays > 3650 {
		s.SupabasePruneDays = 3650
	}
	if s.ZeroStreakLimit < 0 {
		s.ZeroStreakLimit = 0
	}
	if s.ZeroStreakLimit > 100 {
		s.ZeroStreakLimit = 100
	}
}

// validClock memeriksa format "HH:MM".
func validClock(value string) bool {
	parts := strings.Split(strings.TrimSpace(value), ":")
	if len(parts) != 2 {
		return false
	}
	hour, err1 := strconv.Atoi(parts[0])
	minute, err2 := strconv.Atoi(parts[1])
	return err1 == nil && err2 == nil && hour >= 0 && hour < 24 && minute >= 0 && minute < 60
}

// NextDaily mengembalikan waktu jalan berikutnya untuk mode daily ("HH:MM" waktu lokal).
func NextDaily(dailyAt string, now time.Time) time.Time {
	if !validClock(dailyAt) {
		dailyAt = Default().DailyAt
	}
	parts := strings.Split(dailyAt, ":")
	hour, _ := strconv.Atoi(parts[0])
	minute, _ := strconv.Atoi(parts[1])
	next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, now.Location())
	if !next.After(now) {
		next = next.Add(24 * time.Hour)
	}
	return next
}

// Interval mengembalikan durasi siklus otomatis.
func (s Settings) Interval() time.Duration { return time.Duration(s.IntervalMinutes) * time.Minute }

// RefreshAfter mengembalikan umur catatan sebelum diverifikasi ulang.
func (s Settings) RefreshAfter() time.Duration {
	return time.Duration(s.RefreshAfterMinutes) * time.Minute
}

// Fresh mengembalikan jendela kesegaran.
func (s Settings) Fresh() time.Duration { return time.Duration(s.FreshMinutes) * time.Minute }

// ConfigDir mengembalikan folder tempat settings.json disimpan.
func ConfigDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return "."
	}
	return filepath.Join(home, ".panproxyscanner")
}

// Load membaca settings.json; berkas tidak ada → pakai default.
func Load(path string) (Settings, error) {
	s := Default()
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return Default(), fmt.Errorf("settings.json rusak: %w", err)
	}
	s.Normalize()
	return s, nil
}

// Save menulis settings.json (mode 0600 karena bisa memuat token).
func (s Settings) Save(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}
