package settings

import "testing"

// Batas push per org/negara: 0 = tanpa batas, nilai negatif (salah ketik) dikembalikan ke
// bawaan supaya tidak berubah arti menjadi "tanpa batas", dan nilai keterlaluan dipotong.
func TestNormalizePushCaps(t *testing.T) {
	def := Default()
	if def.PushOrgCap != 25 || def.PushCountryCap != 60 {
		t.Fatalf("bawaan harus 25 baris per org / 60 per negara, dapat %d/%d", def.PushOrgCap, def.PushCountryCap)
	}
	cases := []struct{ in, wantOrg, wantCountry int }{
		{in: 0, wantOrg: 0, wantCountry: 0},
		{in: 10, wantOrg: 10, wantCountry: 10},
		{in: -1, wantOrg: def.PushOrgCap, wantCountry: def.PushCountryCap},
		{in: -1000, wantOrg: def.PushOrgCap, wantCountry: def.PushCountryCap},
		{in: 50000, wantOrg: maxPushCap, wantCountry: maxPushCap},
	}
	for _, c := range cases {
		s := Settings{PushOrgCap: c.in, PushCountryCap: c.in}
		s.Normalize()
		if s.PushOrgCap != c.wantOrg || s.PushCountryCap != c.wantCountry {
			t.Fatalf("cap=%d → org %d / negara %d, mau %d / %d", c.in, s.PushOrgCap, s.PushCountryCap, c.wantOrg, c.wantCountry)
		}
	}
}
