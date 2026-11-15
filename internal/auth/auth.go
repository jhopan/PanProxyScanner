// Package auth menangani login sederhana untuk website: satu akun admin (bawaan
// admin/admin), sesi disimpan di memori proses, dan token opsional untuk klien mesin.
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"sync"
	"time"
)

// CookieName adalah nama cookie sesi.
const CookieName = "ps_session"

// SessionTTL adalah umur sesi.
const SessionTTL = 24 * time.Hour

type session struct {
	user    string
	expires time.Time
	// mustChange menandai sesi yang masuk dengan sandi bawaan: panel wajib mengganti sandi
	// sebelum endpoint lain boleh dipakai.
	mustChange bool
}

// State adalah kondisi sesi berjalan: user yang login dan apakah sesi itu masih wajib
// mengganti sandi (ditandai saat login memakai hash bawaan).
type State struct {
	User           string
	MustChangePass bool
}

// Manager menyimpan sesi login di memori. Restart proses = semua sesi logout.
type Manager struct {
	mu       sync.Mutex
	sessions map[string]session
}

// New membuat manager sesi.
func New() *Manager {
	return &Manager{sessions: make(map[string]session)}
}

// PasswordMatches membandingkan sandi dengan hash tersimpan dalam waktu konstan.
func PasswordMatches(hash, password string, hashPassword func(string) string) bool {
	return subtle.ConstantTimeCompare([]byte(hashPassword(password)), []byte(hash)) == 1
}

// Login memeriksa kredensial (dibandingkan dalam waktu konstan) lalu membuat sesi.
// Sesi ditandai wajib-ganti-sandi kalau hash yang dipakai untuk masuk masih hash bawaan
// (`defaultHash`), supaya sandi bawaan tidak bisa dipakai mengelola apa pun.
// Mengembalikan token sesi, tanda wajib-ganti-sandi, dan status keberhasilan.
func (m *Manager) Login(user, password, wantUser, wantHash, defaultHash string, hashPassword func(string) string) (string, bool, bool) {
	if subtle.ConstantTimeCompare([]byte(user), []byte(wantUser)) != 1 {
		return "", false, false
	}
	if !PasswordMatches(wantHash, password, hashPassword) {
		return "", false, false
	}
	token, err := randomToken()
	if err != nil {
		return "", false, false
	}
	mustChange := defaultHash != "" && wantHash == defaultHash
	m.mu.Lock()
	m.sessions[token] = session{user: user, expires: time.Now().Add(SessionTTL), mustChange: mustChange}
	m.mu.Unlock()
	return token, mustChange, true
}

// Validate memeriksa token sesi.
func (m *Manager) Validate(token string) (string, bool) {
	st, ok := m.State(token)
	return st.User, ok
}

// State mengembalikan kondisi sesi untuk token; ok=false kalau token kosong/tidak dikenal/
// sudah kedaluwarsa.
func (m *Manager) State(token string) (State, bool) {
	if token == "" {
		return State{}, false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[token]
	if !ok {
		return State{}, false
	}
	if time.Now().After(s.expires) {
		delete(m.sessions, token)
		return State{}, false
	}
	return State{User: s.user, MustChangePass: s.mustChange}, true
}

// ClearMustChange menghapus tanda wajib-ganti-sandi dari sebuah sesi (dipakai setelah sandi
// benar-benar diganti). Sesi sudah hilang/kedaluwarsa = no-op.
func (m *Manager) ClearMustChange(token string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[token]
	if !ok {
		return
	}
	s.mustChange = false
	m.sessions[token] = s
}

// Logout menghapus sesi.
func (m *Manager) Logout(token string) {
	m.mu.Lock()
	delete(m.sessions, token)
	m.mu.Unlock()
}

// UserFromRequest mengambil user dari cookie sesi.
func (m *Manager) UserFromRequest(r *http.Request) (string, bool) {
	_, st, ok := m.RequestState(r)
	return st.User, ok
}

// RequestState mengembalikan token cookie + kondisi sesi permintaan. Dipakai gate (withCommon)
// dan handler yang butuh token untuk membersihkan tanda wajib-ganti-sandi.
func (m *Manager) RequestState(r *http.Request) (string, State, bool) {
	cookie, err := r.Cookie(CookieName)
	if err != nil {
		return "", State{}, false
	}
	st, ok := m.State(cookie.Value)
	if !ok {
		return "", State{}, false
	}
	return cookie.Value, st, true
}

// SetCookie menulis cookie sesi.
func SetCookie(w http.ResponseWriter, r *http.Request, token string) {
	secure := r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https"
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   secure,
		MaxAge:   int(SessionTTL.Seconds()),
	})
}

// ClearCookie menghapus cookie sesi.
func ClearCookie(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     CookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   r.TLS != nil || r.Header.Get("X-Forwarded-Proto") == "https",
		MaxAge:   -1,
	})
}

func randomToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}
