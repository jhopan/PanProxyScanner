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

// Login memeriksa kredensial (dibandingkan dalam waktu konstan) lalu membuat sesi.
// Mengembalikan token sesi kalau berhasil.
func (m *Manager) Login(user, password, wantUser, wantHash string, hashPassword func(string) string) (string, bool) {
	if subtle.ConstantTimeCompare([]byte(user), []byte(wantUser)) != 1 {
		return "", false
	}
	if subtle.ConstantTimeCompare([]byte(hashPassword(password)), []byte(wantHash)) != 1 {
		return "", false
	}
	token, err := randomToken()
	if err != nil {
		return "", false
	}
	m.mu.Lock()
	m.sessions[token] = session{user: user, expires: time.Now().Add(SessionTTL)}
	m.mu.Unlock()
	return token, true
}

// Validate memeriksa token sesi.
func (m *Manager) Validate(token string) (string, bool) {
	if token == "" {
		return "", false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[token]
	if !ok {
		return "", false
	}
	if time.Now().After(s.expires) {
		delete(m.sessions, token)
		return "", false
	}
	return s.user, true
}

// Logout menghapus sesi.
func (m *Manager) Logout(token string) {
	m.mu.Lock()
	delete(m.sessions, token)
	m.mu.Unlock()
}

// UserFromRequest mengambil user dari cookie sesi.
func (m *Manager) UserFromRequest(r *http.Request) (string, bool) {
	cookie, err := r.Cookie(CookieName)
	if err != nil {
		return "", false
	}
	return m.Validate(cookie.Value)
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
