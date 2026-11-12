package main

import "testing"

// resolveAddr menentukan alamat listen: .env menang, kalau tidak ada dipakai flag -addr.
// Sebelum ini flag -addr diabaikan total sehingga port selalu 8787.
func TestResolveAddr(t *testing.T) {
	cases := []struct {
		name     string
		flagAddr string
		env      map[string]string
		want     string
	}{
		{
			name:     "flag dipakai kalau .env kosong",
			flagAddr: "0.0.0.0:8898",
			env:      map[string]string{},
			want:     "0.0.0.0:8898",
		},
		{
			name:     "PS_ADDR menang atas semuanya",
			flagAddr: "0.0.0.0:8898",
			env:      map[string]string{"PS_ADDR": "127.0.0.1:9000", "PS_PORT": "1111"},
			want:     "127.0.0.1:9000",
		},
		{
			name:     "PS_PORT menimpa port flag, host tetap dari flag",
			flagAddr: "127.0.0.1:8898",
			env:      map[string]string{"PS_PORT": "7000"},
			want:     "127.0.0.1:7000",
		},
		{
			name:     "PS_HOST menimpa host flag",
			flagAddr: "127.0.0.1:8898",
			env:      map[string]string{"PS_HOST": "0.0.0.0"},
			want:     "0.0.0.0:8898",
		},
		{
			name:     "PS_BIND_LAN=false memaksa localhost",
			flagAddr: "0.0.0.0:8898",
			env:      map[string]string{"PS_BIND_LAN": "false"},
			want:     "127.0.0.1:8898",
		},
		{
			name:     "PS_BIND_LAN=true kembali ke semua interface",
			flagAddr: "127.0.0.1:8898",
			env:      map[string]string{"PS_BIND_LAN": "true"},
			want:     "0.0.0.0:8898",
		},
		{
			name:     "flag rusak jatuh ke default 8787",
			flagAddr: "bukan-alamat",
			env:      map[string]string{},
			want:     "0.0.0.0:8787",
		},
	}
	for _, c := range cases {
		if got := resolveAddr(c.flagAddr, c.env); got != c.want {
			t.Fatalf("%s: resolveAddr(%q) = %q, mau %q", c.name, c.flagAddr, got, c.want)
		}
	}
}
