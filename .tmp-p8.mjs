// Patch 8: auto-disable sumber nol-hasil + pass tambahan untuk negara di bawah target.
import fs from "node:fs";
const must = (label, ok) => { if (!ok) { console.error("GAGAL: " + label); process.exit(1); } };
const edit = (path, from, to, label) => {
  const s = fs.readFileSync(path, "utf8");
  must(label, s.includes(from));
  fs.writeFileSync(path, s.replace(from, to));
};

// ============================================================ settings
edit("internal/settings/settings.go",
  "\tTestIntervalHours   int    `json:\"test_interval_hours\"`   // uji ulang berkala (jam); 0 = mati",
  [
    "\tTestIntervalHours   int    `json:\"test_interval_hours\"`   // uji ulang berkala (jam); 0 = mati",
    "\tAutoDisableLowYield bool   `json:\"auto_disable_low_yield\"` // matikan sumber yang selalu nol hasil",
    "\tLowYieldCycles      int    `json:\"low_yield_cycles\"`      // berapa siklus tanpa hasil sebelum dimatikan",
    "\tLowYieldMinEntries  int    `json:\"low_yield_min_entries\"` // sumber harus punya minimal N entri agar dinilai",
    "\tCountryTopUp        int    `json:\"country_top_up\"`        // kandidat tambahan per siklus untuk negara di bawah target",
  ].join("\n"), "settings fields 2");

edit("internal/settings/settings.go",
  "\t\tTestIntervalHours:   6,",
  [
    "\t\tTestIntervalHours:   6,",
    "\t\tAutoDisableLowYield: true,",
    "\t\tLowYieldCycles:      5,",
    "\t\tLowYieldMinEntries:  50,",
    "\t\tCountryTopUp:        300,",
  ].join("\n"), "settings defaults 2");

edit("internal/settings/settings.go",
  "\tif s.TestIntervalHours > 24 {\n\t\ts.TestIntervalHours = 24\n\t}",
  [
    "\tif s.TestIntervalHours > 24 {",
    "\t\ts.TestIntervalHours = 24",
    "\t}",
    "\tif s.LowYieldCycles < 4 {",
    "\t\ts.LowYieldCycles = 4",
    "\t}",
    "\tif s.LowYieldCycles > 50 {",
    "\t\ts.LowYieldCycles = 50",
    "\t}",
    "\tif s.LowYieldMinEntries < 10 {",
    "\t\ts.LowYieldMinEntries = 10",
    "\t}",
    "\tif s.CountryTopUp < 0 {",
    "\t\ts.CountryTopUp = 0",
    "\t}",
    "\tif s.CountryTopUp > 5000 {",
    "\t\ts.CountryTopUp = 5000",
    "\t}",
  ].join("\n"), "settings normalize 2");

// ============================================================ store
edit("internal/store/store.go",
  '\t\t{"yield_ok", `ALTER TABLE sources ADD COLUMN yield_ok INTEGER NOT NULL DEFAULT 0`},',
  '\t\t{"yield_ok", `ALTER TABLE sources ADD COLUMN yield_ok INTEGER NOT NULL DEFAULT 0`},\n\t\t// cycles: berapa kali sumber ini sudah diambil. Dipakai menilai yield, bukan sekadar kosong.\n\t\t{"cycles", `ALTER TABLE sources ADD COLUMN cycles INTEGER NOT NULL DEFAULT 0`},',
  "migrasi cycles");

edit("internal/store/store.go",
  "func (s *Store) UpdateSourceHealth(name string, count int, errText string, zeroStreakLimit int) (bool, error) {\n\tvar err error\n\tautoDisabled := false\n\tif count > 0 {\n\t\t_, err = s.db.Exec(`UPDATE sources SET last_count = ?, zero_streak = 0, last_error = NULL WHERE name = ?`, count, name)\n\t\treturn false, err\n\t}",
  [
    "// UpdateSourceHealth mencatat kesehatan sumber. Selain \"kosong berkali-kali\", sumber yang",
    "// berkali-kali punya entri TAPI tidak pernah menghasilkan proxy yang lolos verifikasi juga",
    "// dimatikan: itu tanda daftar mass-scan (ratusan ribu entri, hampir semuanya mati). Sumber",
    "// yang dimatikan tetap bisa dinyalakan lagi dari halaman Sumber.",
    "func (s *Store) UpdateSourceHealth(name string, count int, errText string, zeroStreakLimit, lowYieldCycles, lowYieldMinEntries int) (bool, error) {",
    "\tvar err error",
    "\tautoDisabled := false",
    "\tif count > 0 {",
    "\t\t_, err = s.db.Exec(`UPDATE sources SET last_count = ?, zero_streak = 0, cycles = cycles + 1, last_error = NULL WHERE name = ?`, count, name)",
    "\t\tif err != nil {",
    "\t\t\treturn false, err",
    "\t\t}",
    "\t\tif lowYieldCycles > 0 && lowYieldMinEntries > 0 {",
    "\t\t\t// Dinilai hanya kalau sumber ini memang rutin memberi entri: sumber kecil yang",
    "\t\t\t// kebetulan belum menghasilkan tidak dihukum.",
    "\t\t\tres, err := s.db.Exec(`",
    "UPDATE sources SET enabled = 0, auto_disabled = 1",
    "WHERE name = ? AND optional = 0 AND auto_disabled = 0",
    "  AND cycles >= ? AND last_count >= ? AND yield_ok = 0`, name, lowYieldCycles, lowYieldMinEntries)",
    "\t\t\tif err != nil {",
    "\t\t\t\treturn false, err",
    "\t\t\t}",
    "\t\t\tif n, _ := res.RowsAffected(); n > 0 {",
    "\t\t\t\treturn true, nil",
    "\t\t\t}",
    "\t\t}",
    "\t\treturn false, nil",
    "\t}",
  ].join("\n"), "UpdateSourceHealth yield rule");

// Query baru: kandidat lokal dari negara yang stoknya kurang, untuk diverifikasi ulang.
fs.appendFileSync("internal/store/store.go", `
// ThinCountryCandidates mengambil baris tersimpan dari negara tertentu yang belum terbukti
// sanggup TLS, untuk diverifikasi ulang. Dipakai pass tambahan per negara: tanpa ini, negara
// yang stoknya tipis hanya bergantung pada kandidat baru yang kebetulan berlabel negara itu.
func (s *Store) ThinCountryCandidates(countries []string, limit int, olderThan time.Duration) ([]string, error) {
	if len(countries) == 0 || limit <= 0 {
		return nil, nil
	}
	codes := make([]string, 0, len(countries))
	args := make([]any, 0, len(countries)+3)
	for _, code := range countries {
		code = strings.ToUpper(strings.TrimSpace(code))
		if len(code) != 2 {
			continue
		}
		codes = append(codes, code)
		args = append(args, code)
	}
	if len(codes) == 0 {
		return nil, nil
	}
	mark := strings.TrimSuffix(strings.Repeat("?,", len(codes)), ",")
	query := "SELECT ip, port FROM proxies WHERE tls_ok = 0 AND country IN (" + mark + ")"
	if olderThan > 0 {
		query += " AND (last_checked IS NULL OR last_checked < ?)"
		args = append(args, time.Now().Add(-olderThan).UTC().Format(time.RFC3339))
	}
	query += " ORDER BY last_checked IS NULL DESC, last_checked ASC LIMIT ?"
	args = append(args, limit)
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]string, 0, limit)
	for rows.Next() {
		var ip, port string
		if err := rows.Scan(&ip, &port); err != nil {
			return nil, err
		}
		out = append(out, ip+":"+port)
	}
	return out, rows.Err()
}
`);

console.log("store + settings selesai");