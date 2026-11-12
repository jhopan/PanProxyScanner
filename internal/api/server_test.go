package api

import (
	"testing"

	"github.com/jhopan/pan-proxy-scanner/internal/settings"
)

// exitKey dipakai untuk dedup alamat keluar di pool: IPv4 dinormalkan per /24 supaya
// beberapa port dari subnet yang sama tidak memenuhi pool, IPv6 dibiarkan utuh.
func TestExitKey(t *testing.T) {
	cases := map[string]string{
		"103.18.78.157":        "103.18.78.0/24",
		" 103.18.78.9 ":        "103.18.78.0/24",
		"2400:6180:0:d0::e5:1": "2400:6180:0:d0::e5:1",
		"":                     "",
		"bukan-ip":             "",
	}
	for in, want := range cases {
		if got := exitKey(in); got != want {
			t.Fatalf("exitKey(%q) = %q, mau %q", in, got, want)
		}
	}
	if exitKey("103.18.78.157") != exitKey("103.18.78.200") {
		t.Fatal("dua alamat di /24 yang sama harus menghasilkan kunci yang sama")
	}
	if exitKey("103.18.78.157") == exitKey("103.18.79.1") {
		t.Fatal("/24 berbeda tidak boleh dianggap duplikat")
	}
}

// pushFreshMinutes membatasi umur verdict yang boleh dipakai saat push (≤ 90 menit),
// karena fresh_minutes bawaan (360) terlalu longgar untuk panel.
func TestPushFreshMinutes(t *testing.T) {
	cases := []struct{ in, want int }{
		{in: 360, want: 90},
		{in: 0, want: 90},
		{in: 45, want: 45},
		{in: 90, want: 90},
		{in: 120, want: 90},
	}
	for _, c := range cases {
		got := pushFreshMinutes(settings.Settings{FreshMinutes: c.in})
		if got != c.want {
			t.Fatalf("fresh_minutes=%d → %d, mau %d", c.in, got, c.want)
		}
	}
}
