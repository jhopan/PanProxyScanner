// Patch 9: API settings baru + tes untuk aturan baru.
import fs from "node:fs";
const must = (label, ok) => { if (!ok) { console.error("GAGAL: " + label); process.exit(1); } };
const edit = (path, from, to, label) => {
  const s = fs.readFileSync(path, "utf8");
  must(label, s.includes(from));
  fs.writeFileSync(path, s.replace(from, to));
};

// ---------------------------------------------------------------- API settings
edit("internal/api/server.go",
  '\t\tTestIntervalHours   *int    `json:"test_interval_hours"`',
  [
    '\t\tTestIntervalHours   *int    `json:"test_interval_hours"`',
    '\t\tAutoDisableLowYield *bool   `json:"auto_disable_low_yield"`',
    '\t\tLowYieldCycles      *int    `json:"low_yield_cycles"`',
    '\t\tLowYieldMinEntries  *int    `json:"low_yield_min_entries"`',
    '\t\tCountryTopUp        *int    `json:"country_top_up"`',
  ].join("\n"), "patch struct settings");

edit("internal/api/server.go",
  "\tif patch.TestIntervalHours != nil {\n\t\tcfg.TestIntervalHours = *patch.TestIntervalHours\n\t}",
  [
    "\tif patch.TestIntervalHours != nil {",
    "\t\tcfg.TestIntervalHours = *patch.TestIntervalHours",
    "\t}",
    "\tif patch.AutoDisableLowYield != nil {",
    "\t\tcfg.AutoDisableLowYield = *patch.AutoDisableLowYield",
    "\t}",
    "\tif patch.LowYieldCycles != nil {",
    "\t\tcfg.LowYieldCycles = *patch.LowYieldCycles",
    "\t}",
    "\tif patch.LowYieldMinEntries != nil {",
    "\t\tcfg.LowYieldMinEntries = *patch.LowYieldMinEntries",
    "\t}",
    "\tif patch.CountryTopUp != nil {",
    "\t\tcfg.CountryTopUp = *patch.CountryTopUp",
    "\t}",
  ].join("\n"), "patch apply settings");

edit("internal/api/server.go",
  '\t\t"test_interval_hours":   cfg.TestIntervalHours,',
  [
    '\t\t"test_interval_hours":   cfg.TestIntervalHours,',
    '\t\t"auto_disable_low_yield": cfg.AutoDisableLowYield,',
    '\t\t"low_yield_cycles":       cfg.LowYieldCycles,',
    '\t\t"low_yield_min_entries":  cfg.LowYieldMinEntries,',
    '\t\t"country_top_up":         cfg.CountryTopUp,',
  ].join("\n"), "publicSettings fields");

// ---------------------------------------------------------------- tes scraper
fs.appendFileSync("internal/scraper/scraper_test.go", `
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
	// target 0 = tidak ditargetkan, jangan masuk daftar
	if got := unmetFromCounts(list, map[string]int{"SG": 0}); len(got) != 0 {
		t.Fatalf("target 0 tidak boleh menghasilkan unmet: %v", got)
	}
}
`);

// ---------------------------------------------------------------- tes store
fs.appendFileSync("internal/store/store_test.go", `
// Sumber yang berkali-kali memberi entri tapi nol hasil harus dimatikan otomatis; yang pernah
// menghasilkan tetap hidup. Ini yang membuang daftar mass-scan (ratusan ribu entri, nol hasil).
func TestLowYieldAutoDisable(t *testing.T) {
	st := openTest(t)
	if err := st.EnsureSources([]SourceSeed{
		{Name: "junk", URL: "https://junk.test/list"},
		{Name: "good", URL: "https://good.test/list"},
	}); err != nil {
		t.Fatal(err)
	}
	// good: pernah menghasilkan
	if err := st.AddSourceYield("good", 3); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, err := st.UpdateSourceHealth("junk", 500, "", 0, 5, 50); err != nil {
			t.Fatal(err)
		}
		if _, err := st.UpdateSourceHealth("good", 10, "", 0, 5, 50); err != nil {
			t.Fatal(err)
		}
	}
	health, err := st.SourceHealthList()
	if err != nil {
		t.Fatal(err)
	}
	if health["junk"].Enabled {
		t.Fatal("sumber nol-hasil setelah 5 siklus harus dimatikan")
	}
	if !health["junk"].AutoDisabled {
		t.Fatal("penanda auto_disabled harus terpasang")
	}
	if !health["good"].Enabled {
		t.Fatal("sumber yang pernah menghasilkan tidak boleh dimatikan")
	}
}

// Kandidat per negara untuk pass tambahan: hanya baris yang belum terbukti TLS.
func TestThinCountryCandidates(t *testing.T) {
	st := openTest(t)
	if err := st.RecordAlive("1.1.1.1", 1080, "socks5", "ID", 900, 2, false, false, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordAlive("2.2.2.2", 1080, "socks5", "ID", 700, 2, true, false, ""); err != nil {
		t.Fatal(err)
	}
	if err := st.RecordAlive("3.3.3.3", 1080, "socks5", "SG", 500, 2, false, false, ""); err != nil {
		t.Fatal(err)
	}
	got, err := st.ThinCountryCandidates([]string{"ID"}, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "1.1.1.1:1080" {
		t.Fatalf("hanya baris ID tanpa TLS yang boleh terpilih, dapat %v", got)
	}
}
`);

console.log("API + tes selesai");