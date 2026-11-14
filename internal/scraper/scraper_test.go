package scraper

import (
	"context"
	"github.com/jhopan/Pan-ProxyScanner/internal/settings"
	"path/filepath"
	"testing"
	"time"

	"github.com/jhopan/Pan-ProxyScanner/internal/store"
)

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

	order := rank(union, counts, nil, nil, nil, 0)
	if len(order) != 3 || order[0] != "2.2.2.2:55555" || order[1] != "3.3.3.3:4145" {
		t.Fatalf("rank harus urut jumlah sumber: %v", order)
	}
	// sumber sama banyak → port umum menang
	tie := rank(map[string]struct{}{"4.4.4.4:55555": {}, "5.5.5.5:1080": {}},
		map[string]int{"4.4.4.4:55555": 2, "5.5.5.5:1080": 2}, nil, nil, nil, 0)
	if len(tie) != 2 || tie[0] != "5.5.5.5:1080" {
		t.Fatalf("port umum harus didahulukan saat seri: %v", tie)
	}
	// minSources menyaring kandidat ber-sumber sedikit
	filtered := rank(union, counts, nil, nil, nil, 2)
	for _, addr := range filtered {
		if addr == "1.1.1.1:1080" {
			t.Fatalf("kandidat dengan 1 sumber harus tersaring: %v", filtered)
		}
	}
	if len(filtered) != 2 {
		t.Fatalf("minSources=2 mau 2 kandidat, dapat %d", len(filtered))
	}
}

// Negara yang stoknya tipis harus didahulukan, walaupun kandidatnya hanya muncul di satu sumber
// dan portnya bukan port umum. Tanpa ini, negara seperti ID/SG selalu kalah oleh daftar mass-scan.
func TestRankPrefersThinCountries(t *testing.T) {
	union := map[string]struct{}{"1.1.1.1:1080": {}, "2.2.2.2:34567": {}, "3.3.3.3:1080": {}}
	counts := map[string]int{"1.1.1.1:1080": 5, "2.2.2.2:34567": 1, "3.3.3.3:1080": 1}
	countries := map[string]string{"2.2.2.2:34567": "ID", "3.3.3.3:1080": "US"}

	got := rank(union, counts, counts, countries, map[string]bool{"ID": true}, 0)
	if got[0] != "2.2.2.2:34567" {
		t.Fatalf("kandidat negara tipis harus lebih dulu, dapat %v", got)
	}

	// Tanpa negara tipis, urutan kembali ke jumlah sumber (sinyal kualitas).
	got = rank(union, counts, counts, countries, map[string]bool{}, 0)
	if got[0] != "1.1.1.1:1080" {
		t.Fatalf("tanpa prioritas, kandidat terbanyak-sumber harus lebih dulu, dapat %v", got)
	}
}

// Target per negara: negara dengan target khusus dianggap tipis sampai stok TLS-nya mencapai
// target; negara lain memakai default; target 0 berarti tidak diprioritaskan.
func TestThinFromCounts(t *testing.T) {
	list := []store.CountryCount{
		{Country: "ID", Alive: 4, TLS: 1},
		{Country: "US", Alive: 9, TLS: 9},
		{Country: "SG", Alive: 3, TLS: 3},
		{Country: "JP", Alive: 2, TLS: 2},
	}
	targets := map[string]int{"ID": 5, "SG": 3, "JP": 0}
	thin := thinFromCounts(list, targets)
	if !thin["ID"] {
		t.Fatal("ID (1 dari target 5) harus tipis")
	}
	if thin["SG"] {
		t.Fatal("SG sudah mencapai target 3, tidak boleh tipis")
	}
	if thin["JP"] {
		t.Fatal("target 0 berarti tidak diprioritaskan")
	}
	// Negara tanpa target khusus memakai default (3): US 9 sudah cukup.
	if thin["US"] {
		t.Fatal("US tanpa target khusus sudah di atas default, tidak boleh tipis")
	}
	// Negara bertarget tapi belum punya baris sama sekali tetap tipis.
	if !thin["MY"] && !thin["ID"] {
		t.Fatal("negara bertarget khusus harus dianggap tipis minimal saat stok kurang")
	}
}

// Atribusi hasil: hanya proxy hidup+TLS yang menambah yield sumber asalnya.
func TestCreditSources(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "y.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.EnsureSources([]store.SourceSeed{{Name: "src-a", URL: "https://a.test/list"}, {Name: "src-b", URL: "https://b.test/list"}}); err != nil {
		t.Fatal(err)
	}
	origin := map[string][]string{"1.2.3.4:1080": {"src-a", "src-b"}}
	creditSources(st, origin, "1.2.3.4:1080", true)
	creditSources(st, origin, "1.2.3.4:1080", false) // tidak dihitung: bukan TLS
	yields, err := st.SourceYields()
	if err != nil {
		t.Fatal(err)
	}
	if yields["src-a"] != 1 || yields["src-b"] != 1 {
		t.Fatalf("yield harus 1 per sumber, dapat %v", yields)
	}
}

// Negara yang belum mencapai target harus terdeteksi (dasar pass verifikasi tambahan).
func TestUnmetFromCounts(t *testing.T) {
	list := []store.CountryCount{
		{Country: "ID", Alive: 4, TLS: 1},
		{Country: "SG", Alive: 5, TLS: 5},
		{Country: "JP", Alive: 0, TLS: 0},
	}
	targets := map[string]int{"ID": 5, "SG": 3, "JP": 2, "MY": 3}
	got := unmetFromCounts(list, targets)
	want := []string{"ID", "JP", "MY"}
	if len(got) != len(want) {
		t.Fatalf("unmet = %v, mau %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unmet = %v, mau %v", got, want)
		}
	}
	if got := unmetFromCounts(list, map[string]int{"SG": 0}); len(got) != 0 {
		t.Fatalf("target 0 tidak boleh menghasilkan unmet: %v", got)
	}
}

// Cek lokal (jadwal 6 jam) tidak boleh menyentuh jaringan sumber: cukup mengembalikan tanpa
// error walau database kosong dan tanpa mengambil apa pun.
func TestRunLocalOnly(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "l.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	opts := DefaultOptions()
	opts.LocalOnly = true
	opts.RefreshLimit = 0
	opts.CountryTopUp = 0
	stats, err := Run(context.Background(), st, opts, nil)
	if err != nil {
		t.Fatalf("Run(LocalOnly) tidak boleh error: %v", err)
	}
	if len(stats) != 0 {
		t.Fatalf("LocalOnly tidak mengambil sumber, stats harus kosong: %d", len(stats))
	}
}

// Penanda scrape harian: tanpa penanda -> jalan; setelah ditandai hari ini -> tidak; penanda
// kemarin -> jalan lagi (catch-up saat proses sempat mati).
func TestNeedsDailyRun(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "d.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := NewScheduler(nil, settings.Settings{})
	s.Store = st
	if !s.needsDailyRun() {
		t.Fatal("tanpa penanda, scrape penuh harus dianggap perlu")
	}
	s.markDailyRun()
	if s.needsDailyRun() {
		t.Fatal("setelah ditandai hari ini, tidak boleh jalan lagi")
	}
	if err := st.SetMeta(metaLastFull, time.Now().UTC().Add(-24*time.Hour).Format(time.RFC3339)); err != nil {
		t.Fatal(err)
	}
	if !s.needsDailyRun() {
		t.Fatal("penanda kemarin harus memicu scrape penuh lagi")
	}
}
