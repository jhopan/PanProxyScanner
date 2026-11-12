package scraper

import "testing"

func TestSplitAddr(t *testing.T) {
	cases := []struct {
		in, ip, port string
		wantErr      bool
	}{
		{in: "1.2.3.4:1080", ip: "1.2.3.4", port: "1080"},
		{in: "2001:db8::1:1080", ip: "2001:db8::1", port: "1080"},
		{in: "tanpa-port", wantErr: true},
		{in: ":1080", wantErr: true},
	}
	for _, c := range cases {
		ip, port, err := splitAddr(c.in)
		if c.wantErr {
			if err == nil {
				t.Fatalf("splitAddr(%q) harus error", c.in)
			}
			continue
		}
		if err != nil || ip != c.ip || port != c.port {
			t.Fatalf("splitAddr(%q) = (%q,%q,%v), mau (%q,%q,nil)", c.in, ip, port, err, c.ip, c.port)
		}
	}
}

func TestPreferredPort(t *testing.T) {
	// preferredPort mengembalikan kunci urutan (0 = port umum, 1 = lainnya), bukan nomor port.
	if preferredPort("1.2.3.4:1080") != 0 {
		t.Fatal("port 1080 harus dianggap port umum proxy (nilai 0)")
	}
	if preferredPort("1.2.3.4:54321") != 1 {
		t.Fatal("port acak tidak boleh masuk daftar port umum (nilai 1)")
	}
	if preferredPort("rusak") != 1 {
		t.Fatal("alamat rusak harus jatuh ke nilai 1, bukan panik")
	}
}

// rank mengutamakan kandidat yang muncul di banyak sumber, lalu port umum; minSources
// membuang kandidat yang sumbernya terlalu sedikit.
func TestRankPrefersMultiSource(t *testing.T) {
	union := map[string]struct{}{
		"1.1.1.1:1080":  {},
		"2.2.2.2:55555": {},
		"3.3.3.3:4145":  {},
	}
	counts := map[string]int{"1.1.1.1:1080": 1, "2.2.2.2:55555": 5, "3.3.3.3:4145": 3}

	order := rank(union, counts, 0)
	if len(order) != 3 || order[0] != "2.2.2.2:55555" || order[1] != "3.3.3.3:4145" {
		t.Fatalf("rank harus urut jumlah sumber: %v", order)
	}
	// sumber sama banyak → port umum menang
	tie := rank(map[string]struct{}{"4.4.4.4:55555": {}, "5.5.5.5:1080": {}},
		map[string]int{"4.4.4.4:55555": 2, "5.5.5.5:1080": 2}, 0)
	if len(tie) != 2 || tie[0] != "5.5.5.5:1080" {
		t.Fatalf("port umum harus didahulukan saat seri: %v", tie)
	}
	// minSources menyaring kandidat ber-sumber sedikit
	filtered := rank(union, counts, 2)
	for _, addr := range filtered {
		if addr == "1.1.1.1:1080" {
			t.Fatalf("kandidat dengan 1 sumber harus tersaring: %v", filtered)
		}
	}
	if len(filtered) != 2 {
		t.Fatalf("minSources=2 mau 2 kandidat, dapat %d", len(filtered))
	}
}
