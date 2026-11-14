package store

import (
	"path/filepath"
	"testing"
	"time"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// Verdict kualitas harus mengikuti hasil TERAKHIR, bukan MAX seumur hidup: proxy yang
// kehilangan kemampuan TLS tidak boleh tetap berlabel sanggup TLS (bug lama).
func TestRecordAliveUsesLatestVerdict(t *testing.T) {
	st := openTest(t)
	if err := st.RecordAlive("1.1.1.1", 1080, "socks5", "HK", 900, 3, true, false, "9.9.9.9"); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordAlive("1.1.1.1", 1080, "socks5", "HK", 700, 3, false, true, ""); err != nil {
		t.Fatal(err)
	}
	list, err := st.List(Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("mau 1 baris, dapat %d", len(list))
	}
	p := list[0]
	if p.TLS {
		t.Fatal("tls_ok harus turun ke false setelah verdict terakhir tanpa TLS")
	}
	if !p.AltOK {
		t.Fatal("alt_ok harus ikut verdict terakhir (true)")
	}
	if p.ExitIP != "9.9.9.9" {
		t.Fatalf("exit_ip lama harus dipertahankan kalau verdict baru kosong, dapat %q", p.ExitIP)
	}
	if p.OKCount != 2 || p.FailCount != 0 {
		t.Fatalf("ok_count harus 2 dan fail_count 0, dapat %d/%d", p.OKCount, p.FailCount)
	}
	if !p.Alive || p.PingMS != 820 {
		// ping dihaluskan: 0,6*900 + 0,4*700 = 820
		t.Fatalf("alive/ping salah: %+v", p)
	}
}

func TestRecordDeadClearsPing(t *testing.T) {
	st := openTest(t)
	if err := st.RecordAlive("2.2.2.2", 8080, "http", "SG", 500, 1, true, false, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordDead("2.2.2.2", 8080, "SG", 1); err != nil {
		t.Fatal(err)
	}
	alive := false
	list, err := st.List(Filter{Alive: &alive})
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].PingMS != 0 || list[0].Alive {
		t.Fatalf("baris mati harus alive=false dan ping 0: %+v", list)
	}
	if list[0].OKCount != 1 || list[0].FailCount != 1 {
		t.Fatalf("hitungan salah: ok=%d fail=%d", list[0].OKCount, list[0].FailCount)
	}
}

// Jatah refresh: baris hidup yang basi didahulukan, baris mati hanya kalau umurnya melewati
// deadOlderThan. Ini yang membuat siklus tidak membuang waktu untuk sampah mass-scan.
func TestStaleOrderAndWindows(t *testing.T) {
	st := openTest(t)
	now := time.Now().Unix()
	insert := func(ip string, alive bool, ageMinutes int64) {
		t.Helper()
		checked := now - ageMinutes*60
		if _, err := st.db.Exec(`INSERT INTO proxies (ip, port, alive, last_checked, first_seen) VALUES (?, 1080, ?, ?, ?)`,
			ip, boolToInt(alive), checked, checked); err != nil {
			t.Fatal(err)
		}
	}
	insert("10.0.0.1", true, 1)    // hidup, baru → tidak ikut
	insert("10.0.0.2", true, 45)   // hidup, basi → ikut (paling awal)
	insert("10.0.0.3", false, 200) // mati, basi sedang → ikut bila deadWindow 12 jam? tidak (200m < 720m)
	insert("10.0.0.4", false, 800) // mati, sangat basi → ikut

	stale, err := st.Stale(30*time.Minute, 720*time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(stale))
	for _, p := range stale {
		got = append(got, p.IP)
	}
	want := []string{"10.0.0.2", "10.0.0.4"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("Stale = %v, mau %v (hidup dulu, lalu mati yang sangat tua)", got, want)
	}
}

func TestDeleteHopeless(t *testing.T) {
	st := openTest(t)
	insert := func(ip string, ok, fail int) {
		t.Helper()
		if _, err := st.db.Exec(`INSERT INTO proxies (ip, port, alive, last_checked, first_seen, ok_count, fail_count) VALUES (?, 1080, 0, ?, ?, ?, ?)`,
			ip, time.Now().Unix(), time.Now().Unix(), ok, fail); err != nil {
			t.Fatal(err)
		}
	}
	insert("11.0.0.1", 0, 1) // baru gagal sekali → disimpan (bisa jadi gangguan sesaat)
	insert("11.0.0.2", 0, 2) // gagal 2× dan belum pernah hidup → dihapus
	insert("11.0.0.3", 3, 9) // pernah hidup → disimpan

	removed, err := st.DeleteHopeless(2, 100)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("mau 1 baris dihapus, dapat %d", removed)
	}
	all, err := st.List(Filter{})
	if err != nil {
		t.Fatal(err)
	}
	left := map[string]bool{}
	for _, p := range all {
		left[p.IP] = true
	}
	if left["11.0.0.2"] || !left["11.0.0.1"] || !left["11.0.0.3"] {
		t.Fatalf("sisa baris salah: %v", left)
	}
}

func TestListFiltersAndOrder(t *testing.T) {
	st := openTest(t)
	if err := st.RecordAlive("12.0.0.1", 1080, "socks5", "ID", 3000, 2, true, false, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordAlive("12.0.0.2", 1080, "socks5", "SG", 500, 2, true, true, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordAlive("12.0.0.3", 1080, "http", "ID", 900, 2, false, false, ""); err != nil {
		t.Fatal(err)
	}

	tlsOnly, err := st.List(Filter{TLSOnly: true, Order: "ping"})
	if err != nil {
		t.Fatal(err)
	}
	if len(tlsOnly) != 2 {
		t.Fatalf("TLSOnly mau 2 baris, dapat %d", len(tlsOnly))
	}
	if tlsOnly[0].IP != "12.0.0.2" {
		t.Fatalf("order=ping harus tercepat dulu, dapat %s", tlsOnly[0].IP)
	}

	altOnly, err := st.List(Filter{AltOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(altOnly) != 1 || altOnly[0].IP != "12.0.0.2" {
		t.Fatalf("AltOnly salah: %+v", altOnly)
	}

	idOnly, err := st.List(Filter{Country: "id"})
	if err != nil {
		t.Fatal(err)
	}
	if len(idOnly) != 2 {
		t.Fatalf("filter negara harus case-insensitive dan dapat 2 baris, dapat %d", len(idOnly))
	}
}

// ping_ms memakai EWMA (0,6 lama + 0,4 baru) supaya satu sampel lambat tidak langsung
// menggeser peringkat proxy; sampel mati mengosongkan ping.
func TestRecordAliveSmoothsPing(t *testing.T) {
	st := openTest(t)
	if err := st.RecordAlive("14.0.0.1", 1080, "socks5", "ID", 1000, 1, true, false, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordAlive("14.0.0.1", 1080, "socks5", "ID", 2000, 1, true, false, ""); err != nil {
		t.Fatal(err)
	}
	list, err := st.List(Filter{})
	if err != nil {
		t.Fatal(err)
	}
	// 0,6*1000 + 0,4*2000 = 1400 (kalau nilai baru dipakai mentah, hasilnya 2000)
	if list[0].PingMS != 1400 {
		t.Fatalf("ping harus dihaluskan ke 1400, dapat %d", list[0].PingMS)
	}
}

// Statistik harus melaporkan kemampuan TLS/alt secara terpisah dari "hidup".
func TestStatsCountsTLSCapability(t *testing.T) {
	st := openTest(t)
	if err := st.RecordAlive("13.0.0.1", 1080, "socks5", "ID", 1000, 1, true, false, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordAlive("13.0.0.2", 1080, "socks5", "ID", 1000, 1, true, true, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordAlive("13.0.0.3", 1080, "socks5", "ID", 1000, 1, false, false, ""); err != nil {
		t.Fatal(err)
	}
	s, err := st.Stats(6 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if s.Alive != 3 || s.AliveTLS != 2 || s.AliveAlt != 1 {
		t.Fatalf("statistik salah: alive=%d tls=%d alt=%d", s.Alive, s.AliveTLS, s.AliveAlt)
	}
}

// Countries harus memisahkan "hidup" dari "sanggup TLS": panel memakai angka TLS untuk
// menentukan negara mana yang stoknya masih tipis.
func TestCountriesCountsTLS(t *testing.T) {
	st := openTest(t)
	if err := st.RecordAlive("1.1.1.1", 1080, "socks5", "ID", 900, 2, true, false, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordAlive("2.2.2.2", 1080, "http", "ID", 700, 2, false, false, ""); err != nil {
		t.Fatal(err)
	}
	list, err := st.Countries(true)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range list {
		if c.Country != "ID" {
			continue
		}
		if c.Alive != 2 || c.TLS != 1 {
			t.Fatalf("ID: alive=%d tls=%d, mau alive=2 tls=1", c.Alive, c.TLS)
		}
		return
	}
	t.Fatalf("negara ID tidak muncul di hasil: %+v", list)
}
