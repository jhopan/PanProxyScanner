package settings

import (
	"testing"
	"time"
)

// 08:00 WITA harus sama dengan 00:00 UTC: jadwal harian ditulis dalam zona pengaturan, jadi
// konversinya harus benar-benar memakai zona itu, bukan zona mesin.
func TestNextDailyInWITA(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Makassar")
	if err != nil {
		t.Skip("zona Asia/Makassar tidak tersedia di mesin ini")
	}
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC) // 20:00 WITA
	next := NextDailyIn("08:00", now, loc)
	if next.UTC() != time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC) {
		t.Fatalf("NextDailyIn = %s, mau 2026-11-15 00:00 UTC", next.UTC())
	}
	if next.Hour() != 8 {
		t.Fatalf("jam lokal harus 08:00, dapat %02d:00", next.Hour())
	}
}

// Uji berkala jatuh pada kelipatan 6 jam dari tengah malam UTC, bukan bergeser mengikuti kapan
// proses dinyalakan.
func TestNextTestRunAligned(t *testing.T) {
	cases := []struct {
		now  time.Time
		want time.Time
	}{
		{time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC), time.Date(2026, 11, 14, 6, 0, 0, 0, time.UTC)},
		{time.Date(2026, 11, 14, 5, 59, 0, 0, time.UTC), time.Date(2026, 11, 14, 6, 0, 0, 0, time.UTC)},
		{time.Date(2026, 11, 14, 6, 0, 0, 0, time.UTC), time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)},
		{time.Date(2026, 11, 14, 23, 30, 0, 0, time.UTC), time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)},
	}
	for _, c := range cases {
		if got := NextTestRun(c.now, 6); !got.Equal(c.want) {
			t.Fatalf("NextTestRun(%s) = %s, mau %s", c.now, got, c.want)
		}
	}
	if !NextTestRun(time.Now(), 0).IsZero() {
		t.Fatal("interval 0 harus berarti mati (zero time)")
	}
}

func TestNormalizeCountryTargets(t *testing.T) {
	s := Default()
	s.CountryTargets = map[string]int{" id ": 5, "sg": -2, "X": 4, "us": 999}
	s.ScheduleTimezone = "bukan/zona"
	s.TestIntervalHours = 99
	s.Normalize()
	if got := s.CountryTargets["ID"]; got != 5 {
		t.Fatalf("ID harus 5 (kode dinormalkan huruf besar), dapat %d", got)
	}
	if got := s.CountryTargets["SG"]; got != 0 {
		t.Fatalf("target negatif dijepit ke 0, dapat %d", got)
	}
	if _, ok := s.CountryTargets["X"]; ok {
		t.Fatal("kode bukan 2 huruf harus dibuang")
	}
	if got := s.CountryTargets["US"]; got != 200 {
		t.Fatalf("target dijepit maksimal 200, dapat %d", got)
	}
	if s.ScheduleTimezone != Default().ScheduleTimezone {
		t.Fatalf("zona tidak dikenal harus kembali ke bawaan, dapat %q", s.ScheduleTimezone)
	}
	if s.TestIntervalHours != 24 {
		t.Fatalf("test_interval_hours dijepit 24, dapat %d", s.TestIntervalHours)
	}
}
