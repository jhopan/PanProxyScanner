package checker

import (
	"context"
	"testing"
	"time"
)

// parseTrace harus membaca baris `kunci=nilai` dari /cdn-cgi/trace; ini sumber
// exit_ip/loc/colo yang dipakai untuk mengisi database.
func TestParseTrace(t *testing.T) {
	body := "fl=1176f79\nh=cp.cloudflare.com\nip=45.194.33.12\nts=1794467504.000\nvisit_scheme=https\ncolo=HKG\nloc=HK\n\n"
	got := parseTrace(body)
	want := map[string]string{
		"h": "cp.cloudflare.com", "ip": "45.194.33.12", "colo": "HKG", "loc": "HK",
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("parseTrace()[%q] = %q, mau %q", k, got[k], v)
		}
	}
	if len(got) != 7 {
		t.Fatalf("parseTrace menghasilkan %d field, mau 7 (baris kosong harus dilewati)", len(got))
	}
	if fields := parseTrace("baris tanpa sama dengan\n"); len(fields) != 0 {
		t.Fatalf("baris tanpa '=' seharusnya diabaikan, dapat %v", fields)
	}
}

// normalizeCountry hanya meloloskan kode ISO 2 huruf yang bermakna; trace Cloudflare bisa
// mengirim "XX" (tidak diketahui) dan "T1" (Tor).
func TestNormalizeCountry(t *testing.T) {
	cases := map[string]string{
		"id":  "ID",
		" hk": "HK",
		"XX":  "",
		"xx":  "",
		"T1":  "",
		"USA": "",
		"":    "",
	}
	for in, want := range cases {
		if got := normalizeCountry(in); got != want {
			t.Fatalf("normalizeCountry(%q) = %q, mau %q", in, got, want)
		}
	}
}

// Alamat yang tidak bisa dihubungi harus mati (bukan panik, bukan hidup), dan Detail harus
// menjelaskan sebabnya supaya bisa dibaca di UI/log.
func TestCheckDeadAddressIsNotAlive(t *testing.T) {
	opts := DefaultOptions()
	opts.Budget = 3 * time.Second
	opts.ProbeTLS = true
	opts.ProbeAlt = true

	res := Check(context.Background(), "127.0.0.1:1", opts)
	if res.Alive {
		t.Fatalf("127.0.0.1:1 seharusnya mati, tapi Alive=true (detail: %s)", res.Detail)
	}
	if res.TLS || res.AltOK {
		t.Fatalf("alamat mati tidak boleh punya TLS/alt: %+v", res)
	}
	if res.Detail == "" {
		t.Fatal("Detail harus menjelaskan kenapa mati")
	}
}
