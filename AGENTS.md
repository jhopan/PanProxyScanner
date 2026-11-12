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
`https://github.com/jhopan/PanProxyScanner/releases/latest/download/<nama-stabil>`.
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
