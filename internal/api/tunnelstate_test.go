package api

import (
	"strings"
	"testing"
)

// Halaman "Tunnel CF" menampilkan kondisi tunnel dari dua sumber yang rapuh terhadap format
// keluaran: `systemctl show` dan Public URL di pengaturan. Keduanya diurai fungsi murni di
// bawah supaya bisa diuji tanpa systemd/ jaringan.
func TestParseSystemctlShow(t *testing.T) {
	loaded := strings.Join([]string{
		"LoadState=loaded",
		"ActiveState=active",
		"SubState=running",
		"UnitFileState=enabled",
		"ExecMainStartTimestamp=Thu 2026-11-12 23:00:28 WIB",
		"NRestarts=0",
		"",
	}, "\n")
	st := parseSystemctlShow("cloudflared-panproxyscanner.service", loaded)
	if !st.Available {
		t.Fatal("unit loaded harus dianggap dikenal (Available=true)")
	}
	if st.Active != "active" || st.Sub != "running" || st.Enabled != "enabled" || st.Restarts != "0" {
		t.Fatalf("hasil parse salah: %+v", st)
	}
	if st.Since != "Thu 2026-11-12 23:00:28 WIB" {
		t.Fatalf("waktu mulai tidak terbaca: %q", st.Since)
	}
	if st.Error != "" {
		t.Fatalf("unit yang dikenal tidak boleh punya error: %q", st.Error)
	}

	// Unit yang tidak ada: systemctl tetap membalas LoadState=not-found, jadi panel harus
	// memberi keterangan, bukan seolah-olah layanannya mati.
	missing := parseSystemctlShow("cloudflared-tidak-ada.service", "LoadState=not-found\nActiveState=inactive\nSubState=dead\n")
	if missing.Available {
		t.Fatal("LoadState=not-found tidak boleh dianggap unit dikenal")
	}
	if missing.Error == "" || !strings.Contains(missing.Error, "cloudflared-tidak-ada.service") {
		t.Fatalf("error harus menyebut nama unit, dapat %q", missing.Error)
	}

	// Baris aneh (tanpa "=", kosong, spasi) tidak boleh bikin salah nilai.
	st = parseSystemctlShow("u.service", "\nActiveState= active \nsampah\n=\n")
	if st.Active != "active" {
		t.Fatalf("nilai harus di-trim, dapat %q", st.Active)
	}
	if st.Available {
		t.Fatal("tanpa LoadState=loaded unit tidak dianggap dikenal")
	}
}

// Nama unit mengikuti pola installer (cloudflared-<nama tunnel>), dan tetap masuk akal saat
// nama tunnel belum diisi di pengaturan.
func TestTunnelUnitName(t *testing.T) {
	cases := map[string]string{
		"panproxyscanner": "cloudflared-panproxyscanner.service",
		"  scraping  ":    "cloudflared-scraping.service",
		"":                "cloudflared-panproxyscanner.service",
	}
	for in, want := range cases {
		if got := tunnelUnitName(in); got != want {
			t.Fatalf("tunnelUnitName(%q) = %q, mau %q", in, got, want)
		}
	}
}

// Uji jalur publik selalu memakai /api/health (publik, tanpa login) dan tidak boleh
// menghasilkan garis miring ganda walau Public URL diakhiri "/".
func TestTunnelHealthURL(t *testing.T) {
	cases := map[string]string{
		"https://panproxyscanner.jhopan.my.id":  "https://panproxyscanner.jhopan.my.id/api/health",
		"https://panproxyscanner.jhopan.my.id/": "https://panproxyscanner.jhopan.my.id/api/health",
		"  http://192.168.10.21:8787/  ":        "http://192.168.10.21:8787/api/health",
		"":                                      "",
	}
	for in, want := range cases {
		if got := tunnelHealthURL(in); got != want {
			t.Fatalf("tunnelHealthURL(%q) = %q, mau %q", in, got, want)
		}
	}
	// Public URL kosong harus menghasilkan keterangan, bukan uji ke alamat aneh.
	if p := probeTunnelPublic(""); p.Error == "" || p.URL != "" {
		t.Fatalf("probe tanpa Public URL harus memberi error tanpa URL, dapat %+v", p)
	}
}
