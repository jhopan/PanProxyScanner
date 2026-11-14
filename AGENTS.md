# AGENTS.md — Pan ProxyScanner

Pengumpul + verifikator proxy publik berbahasa Go (100% Go, tanpa CGO): server web + API,
SQLite pure-Go, 55 sumber daftar proxy, dan sinkronisasi ke Supabase untuk panel CFVPN Hub.

## Aturan kerja (WAJIB)

1. **Setiap perubahan harus di-commit secara lokal.** Jangan tinggalkan perubahan tanpa commit:
   ```bash
   git add -A && git commit -m "<tipe>(<bagian>): <ringkas>"
   ```
   Tipe yang dipakai: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`. Pesan boleh Indonesia.
2. **Push hanya kalau diminta.** Commit lokal selalu; `git push` **hanya** kalau pengguna
   menyuruh. (Rilis = push tag, lihat bagian Rilis.)
3. **Sebelum bilang selesai, jalankan gate:**
   ```bash
   gofmt -l .            # harus kosong
   go vet ./...          # harus bersih
   go test ./... -count=1
   go build -o panproxyscanner.exe .
   ```
   Untuk perubahan perilaku jaringan, uji nyata dulu (lihat "Uji manual").
4. **Jangan commit berkas runtime**: `.env`, `proxies.db*`, `panproxyscanner.exe`, `dist/`
   (sudah ada di `.gitignore`). Jangan tambahkan UUID/kredensial ke berkas yang di-track.
5. **Perubahan yang mengubah nama kolom/tabel Supabase** wajib disertai berkas `.sql`
   (contoh: `docs/supabase-policies.sql`) — dijalankan oleh manusia di Supabase SQL Editor.

## Struktur

| Path | Isi |
|---|---|
| `main.go` | subcommand `serve`/`scrape`/`sources`/`check`/`verify`/`version` + flag CLI |
| `internal/checker/` | verifikasi 3 tahap: trace HTTP :80, TLS :443 terverifikasi, port non-standar |
| `internal/scraper/` | orkestrasi siklus, prioritas kandidat, penjadwal, verifikasi baris Supabase |
| `internal/store/` | SQLite pure-Go + migrasi + filter/statistik/per-negara |
| `internal/sources/` | katalog 55 sumber + parser generik + kode negara |
| `internal/supabase/` | klien REST PostgREST (push/pull-verify/prune) |
| `internal/api/` | server HTTP + UI (HTML/JS di `internal/api/web/`, di-embed) |
| `internal/settings/` | pengaturan JSON (`~/.panproxyscanner/settings.json`) |
| `internal/auth/`, `internal/envfile/` | sesi login + pembaca `.env` |
| `android/` | aplikasi Android (WebView) yang di-build jadi APK di rilis |
| `docs/API.md` | dokumentasi endpoint |
| `.github/workflows/release.yml` | build biner + APK, buat rilis `vX.Y.Z` dan `latest` |

## Perintah penting

```bash
go run . serve                       # panel + API di http://0.0.0.0:8787
go run . scrape                      # satu siklus penuh di terminal
go run . sources                     # uji katalog sumber
go run . check -jobs 8 1.2.3.4:1080  # uji proxy (tanpa simpan)
go run . verify -jobs 8 1.2.3.4:1080 # uji + SIMPAN verdict ke database
go run . version
go build -ldflags "-X main.version=v0.1.0" -o panproxyscanner.exe .
```

## Konvensi

- Komentar, log, pesan UI, dan dokumen dalam **Indonesia**; identifier Go dalam Inggris/camelCase.
- Komentar menjelaskan **sebab** (kenapa), bukan mengulang kode.
- Semua I/O jaringan punya timeout eksplisit; verifikasi selalu lewat tunnel nyata, bukan cek port.
- Flag CLI: `-tls`, `-alt`, `-jobs`, `-cap`, `-refresh-limit`, `-fresh` (lihat `go run . -h`).
- Konfigurasi runtime lewat `.env` (menang) > flag CLI > `settings.json`.
- Database: SQLite satu berkas; migrasi idempoten (`columnExists` + `ALTER TABLE`) — tabel dibuat
  sebelum migrasi kolomnya.
- Verdict kualitas (`tls_ok`, `alt_ok`, `exit_ip`) = hasil **terakhir**, bukan "pernah bisa".
- **Target per negara** (`country_targets`, kode 2 huruf → angka): negara yang stok TLS-nya di
  bawah targetnya didahulukan verifikasinya. Negara tanpa target khusus memakai default 3, dan
  target 0 berarti tidak diprioritaskan. Ini hanya mengatur **urutan** — semua negara tetap
  di-scrape, dan setiap baris tetap dikategorikan lewat kolom `country`.
- **Penanda scrape harian**: tanggal scrape penuh terakhir disimpan di tabel `meta`
  (`last_full_scrape`). Saat proses start, kalau hari ini (zona jadwal) belum ada scrape penuh,
  pipeline langsung dijalankan (catch-up). Penanda **hanya ditulis setelah run penuh sukses**, dan
  kalau gagal (termasuk "pipeline lain sedang berjalan") loop menjadwalkan percobaan ulang tiap 15
  menit — bukan menunggu 24 jam. Catch-up berjalan di goroutine jadwal, jadi `Start()` tidak lagi
  menunggu satu pipeline penuh. Jalur scrape biasa (`Pipeline == nil`) tetap menandai saat memicu,
  karena `runner.Start` asinkron sehingga hasil akhirnya tidak diketahui di sana.
  Penjadwal memakai `apiServer.RunPipelineSync` (pipeline di goroutine pemanggil, error
  langkah gagal dikembalikan), sedangkan endpoint HTTP `/api/pipeline/run` tetap memakai
  `RunPipelineMode` yang melepas pekerjaan ke latar belakang. Sebelum perbaikan ini penjadwal
  memakai versi latar belakang, sehingga `RunPipelineMode` mengembalikan nil seketika dan
  penanda ditulis sebelum run benar-benar selesai (terlihat di host: `last_full_scrape` sama
  dengan detik restart).
- **Push hanya yang berubah**: waktu push terakhir disimpan di `meta.last_push_at`; push dari
  jadwal 6 jam / auto-push hanya mengirim baris yang `last_checked`-nya lebih baru dari titik
  itu (`Filter.Since`). Pipeline harian tetap mengirim menyeluruh (Since kosong).
- **Semua proxy yang bisa dipakai dipublikasikan**: `push_require_tls` kini bawaan **false** —
  baris hidup (TLS maupun non-TLS) naik ke `is_active=true`, dan `tls_ok` hanya menjadi penanda
  kualitas yang dipakai panel/worker untuk memilih. Aturan lama ("hanya TLS yang boleh aktif")
  diganti karena klien bisa memakai config NTLS; bukti `tls_ok/alt_ok/exit_ip` tetap dikirim.
- **Bukti vantage host dikirim ke Supabase**: setiap baris yang dipush membawa
  `tunnel_host_ok` (pemeriksaan nyata lewat proxy itu dari mesin scanner), `tunnel_host_ms`, dan
  `tunnel_host_checked`. Kolom itu milik cfvpn-hub (`sql/2026-11-14-tunnel-host-columns.sql`,
  dijalankan manusia) dan TIDAK boleh tertukar dengan `tunnel_ok`/`tunnel_ms`/`tunnel_checked`
  yang edge-only (diisi cron CFVPN); scanner hanya membaca `tunnel_ok` sebagai penahan
  (`PullEdgeVerdicts`). Kalau kolomnya belum ada, push tetap sukses tanpa kolom itu.
- **Prune butuh service key**: setelah migrasi `sql/2026-11-15-pool-prune-service-key.sql` (repo
  cfvpn-hub) mencabut DELETE anon pada `proxy_pool`, penghapusan baris mati memakai setting
  `supabase_service_key` (service_role; tidak pernah dikembalikan API, hanya
  `supabase_service_key_set: true`). Tanpa key itu prune menjawab `deleted: 0` + catatan, bukan
  error; push/pull/select tetap memakai `supabase_key` (anon).
- **Kegagalan push dibedakan**: hanya HTTP 400 dengan kode PostgREST `PGRST204` / SQLSTATE
  `42703` yang dianggap "kolom belum ada" (lalu kelompok kolom host dilepas, kelompok bukti lama
  dipertahankan). Error lain (5xx, jaringan, 400 kode lain) dikembalikan apa adanya **tanpa**
  mengubah state kolom, supaya gangguan sesaat tidak membuat bukti berhenti terkirim.
- **Dua jadwal**: `schedule_mode=daily` + `daily_at` pada `schedule_timezone` (bawaan 08:00
  Asia/Makassar = 00:00 UTC) menjalankan pipeline penuh (scrape semua sumber → push).
  `test_interval_hours` (bawaan 6) menjalankan **tes berkala** pada kelipatan jam dari 00:00
  UTC: verifikasi ulang baris tersimpan + pull-verify Supabase + push bukti, tanpa scrape baru.

## Aturan kualitas pool (jangan dilonggarkan tanpa alasan)

- `push` hanya memakai verdict **≤90 menit** (`pushMaxAge`), butuh **`ok_count ≥ 2`**, melewati
  proxy dengan `fail_count > ok_count`, dan mendedup **alamat keluar per /24**.
- Hanya proxy dengan bukti TLS 443 yang boleh naik ke `is_active=true`; yang terbukti tidak
  sanggup TLS justru diturunkan.
- `pull-verify` dan cron worker panel **tidak boleh** menyalakan baris (mereka hanya mematikan).

## Uji manual

```bash
# panel
curl -s http://127.0.0.1:8787/api/health                       # {"ok":true,...}
# verifikasi langsung ke worker VLESS ter-deploy (di repo cfvpn-hub)
python ../cfvpn-hub/tests/prod-proxy-check.py <worker>.workers.dev 103.18.78.157:1080
```

## Installer

`install.sh` (Linux/macOS/Termux) dan `install.bat` (Windows) mengunduh dari
`https://github.com/jhopan/PanProxyScanner/releases/download/latest/<nama-stabil>`
(bentuk `…/releases/download/latest/…`; shortcut `…/releases/latest/download/…` terbukti 404
untuk arsip besar).
Workflow rilis **wajib** tetap menerbitkan nama stabil itu di rilis `latest`
(`panproxyscanner-linux-amd64.tar.gz`, `-linux-arm64`, `-macos-amd64`, `-macos-arm64`,
`-windows-amd64.zip`, `panproxyscanner.apk`) — installer tidak boleh bergantung pada nomor
versi. Kalau nama aset diubah, perbarui juga `install.sh`/`install.bat` dan README.

Uji installer sebelum rilis: `sh -n install.sh`, lalu jalankan non-interaktif
(`PORT=8898 SERVICE=no RUN_NOW=yes NONINTERACTIVE=1 sh install.sh`) dan pastikan
`/api/health` menjawab `{"ok":true,...}`.

## Rilis (GitHub Actions)

```bash
# 1. commit semua perubahan, lalu tag
git tag v0.1.1 && git push origin main --tags
```

Workflow `release.yml` akan: menjalankan `gofmt`/`go vet`/`go test`, membangun biner Windows
(amd64), Linux (amd64, arm64), **macOS (amd64, arm64)**, membangun **APK** Android, lalu:

- membuat rilis **`v0.1.1`** (aset: zip/tar.gz per-OS + `panproxyscanner.apk`), dan
- memperbarui tag + rilis **`latest`** ke versi itu,

sehingga tautan `.../releases/latest` selalu menunjuk ke versi terbaru. Detail pemasangan ada
di `README.md`.
