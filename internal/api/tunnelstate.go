package api

import (
	"context"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/jhopan/Pan-ProxyScanner/internal/settings"
)

// Kondisi tunnel yang ditampilkan di halaman "Tunnel CF": status unit systemd yang
// menjalankan cloudflared untuk panel ini, plus hasil uji jalur publik — panel memanggil
// /api/health miliknya sendiri lewat Public URL, jadi "ok" berarti jalur Cloudflare benar
// benar tembus dari luar ke panel ini.
const (
	tunnelProbeEvery = 30 * time.Second // jeda minimal antar uji jalur publik
	tunnelExecEvery  = 10 * time.Second // jeda minimal antar pembacaan status layanan
	tunnelExecLimit  = 3 * time.Second
	tunnelProbeLimit = 6 * time.Second
)

// tunnelServiceState menggambarkan unit systemd tunnel (cloudflared-<nama tunnel>).
type tunnelServiceState struct {
	Unit      string `json:"unit"`
	Available bool   `json:"available"` // unit dikenal systemd (LoadState=loaded)
	Load      string `json:"load,omitempty"`
	Active    string `json:"active,omitempty"`
	Sub       string `json:"sub,omitempty"`
	Enabled   string `json:"enabled,omitempty"`
	Since     string `json:"since,omitempty"`
	Restarts  string `json:"restarts,omitempty"`
	Error     string `json:"error,omitempty"`
}

// tunnelPublicProbe adalah hasil uji jalur publik (panel -> Cloudflare -> panel).
type tunnelPublicProbe struct {
	URL       string `json:"url"`
	OK        bool   `json:"ok"`
	Status    int    `json:"status,omitempty"`
	MS        int64  `json:"ms,omitempty"`
	CheckedAt string `json:"checked_at,omitempty"`
	Error     string `json:"error,omitempty"`
}

// tunnelState adalah kondisi tunnel yang dilaporkan /api/tunnel.
type tunnelState struct {
	Service   tunnelServiceState `json:"service"`
	Public    tunnelPublicProbe  `json:"public"`
	CheckedAt string             `json:"checked_at,omitempty"`
}

// tunnelUnitName menebak nama unit systemd dari nama tunnel (mis. "panproxyscanner" ->
// "cloudflared-panproxyscanner.service"), sama seperti unit yang dipasang install.sh.
func tunnelUnitName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "panproxyscanner"
	}
	return "cloudflared-" + name + ".service"
}

// readTunnelService membaca status unit systemd. Di sistem tanpa systemd (Windows, Termux)
// hasilnya `available:false` plus keterangan, bukan galat.
func readTunnelService(unit string) tunnelServiceState {
	path, err := exec.LookPath("systemctl")
	if err != nil {
		return tunnelServiceState{Unit: unit, Error: "systemctl tidak tersedia di sistem ini; pantau layanan cloudflared dari luar panel"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), tunnelExecLimit)
	defer cancel()
	out, err := exec.CommandContext(ctx, path, "show",
		"-p", "LoadState", "-p", "ActiveState", "-p", "SubState", "-p", "UnitFileState",
		"-p", "ExecMainStartTimestamp", "-p", "NRestarts", "--", unit).Output()
	if len(out) == 0 {
		msg := "tidak bisa membaca status " + unit
		if err != nil {
			msg += ": " + err.Error()
		}
		return tunnelServiceState{Unit: unit, Error: msg}
	}
	return parseSystemctlShow(unit, string(out))
}

// parseSystemctlShow mengurai keluaran `systemctl show -p ...` (baris `Key=Value`).
func parseSystemctlShow(unit, out string) tunnelServiceState {
	st := tunnelServiceState{Unit: unit}
	for _, line := range strings.Split(out, "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		switch strings.TrimSpace(key) {
		case "LoadState":
			st.Load = value
		case "ActiveState":
			st.Active = value
		case "SubState":
			st.Sub = value
		case "UnitFileState":
			st.Enabled = value
		case "ExecMainStartTimestamp":
			st.Since = value
		case "NRestarts":
			st.Restarts = value
		}
	}
	st.Available = st.Load == "loaded"
	if !st.Available {
		st.Error = "unit " + unit + " tidak dikenal di sistem ini"
		if st.Load != "" {
			st.Error += " (LoadState=" + st.Load + ")"
		}
	}
	return st
}

// tunnelHealthURL menyusun alamat /api/health dari Public URL yang diisi di pengaturan.
func tunnelHealthURL(publicURL string) string {
	base := strings.TrimSpace(publicURL)
	if base == "" {
		return ""
	}
	return strings.TrimRight(base, "/") + "/api/health"
}

// probeTunnelPublic menguji Public URL yang tersimpan di pengaturan.
func probeTunnelPublic(publicURL string) tunnelPublicProbe {
	target := tunnelHealthURL(publicURL)
	if target == "" {
		return tunnelPublicProbe{Error: "Public URL belum diisi (menu Scheduler → Akun & API)"}
	}
	p := tunnelPublicProbe{URL: target}
	ctx, cancel := context.WithTimeout(context.Background(), tunnelProbeLimit)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		p.Error = err.Error()
		return p
	}
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	p.MS = time.Since(start).Milliseconds()
	if err != nil {
		p.Error = err.Error()
		return p
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	p.Status = resp.StatusCode
	p.OK = resp.StatusCode == http.StatusOK
	p.CheckedAt = time.Now().Format(time.RFC3339)
	if !p.OK {
		p.Error = "balasan " + strconv.Itoa(resp.StatusCode) + " dari jalur publik"
	}
	return p
}

// tunnelStateFor mengembalikan kondisi tunnel. Hasil uji jalur publik diambil dari cache dan
// disegarkan di latar belakang; `force` (dipakai tombol "Uji ulang" lewat ?probe=1) menunggu
// uji selesai supaya jawabannya selalu yang terbaru.
func (s *Server) tunnelStateFor(cfg settings.Settings, force bool) tunnelState {
	s.tunnelMu.Lock()
	defer s.tunnelMu.Unlock()

	age := time.Since(s.tunnelStateAt)
	if !force && age <= tunnelExecEvery {
		return s.tunnelState
	}
	s.tunnelState.Service = readTunnelService(tunnelUnitName(cfg.TunnelName))
	if force {
		s.tunnelState.Public = probeTunnelPublic(cfg.PublicURL)
		s.tunnelState.CheckedAt = time.Now().Format(time.RFC3339)
		s.tunnelStateAt = time.Now()
		return s.tunnelState
	}
	if s.tunnelProbing {
		return s.tunnelState
	}
	s.tunnelProbing = true
	publicURL := cfg.PublicURL
	go func() {
		probe := probeTunnelPublic(publicURL)
		s.tunnelMu.Lock()
		s.tunnelState.Public = probe
		s.tunnelState.CheckedAt = time.Now().Format(time.RFC3339)
		s.tunnelStateAt = time.Now()
		s.tunnelProbing = false
		s.tunnelMu.Unlock()
	}()
	return s.tunnelState
}
