# API — Pan ProxyScanner

Base URL default: `http://<host>:8787` (bind bawaan `0.0.0.0`, jadi bisa dipanggil dari LAN).

Semua respons API memakai header `Access-Control-Allow-Origin: *`.

## Autentikasi

Panel memakai **login** (bawaan `admin` / `admin`, bisa diganti di halaman **Scheduler →
Akun & API**). Setelah `POST /api/login` server menulis cookie sesi `ps_session`
(HttpOnly, SameSite=Lax, umur 24 jam, `Secure` otomatis kalau lewat HTTPS/tunnel).

Untuk **klien mesin** (panel CFVPN Hub, skrip, cron) yang tidak bisa memakai cookie browser,
isi **Token API** di pengaturan lalu kirim:

```
X-Api-Token: <token>       # header, disarankan
?token=<token>             # query, untuk <img>/<a> yang tidak bisa set header
```

Aturan akses:

| Rute | Akses |
|---|---|
| `GET /login`, `POST /api/login` | publik |
| `GET /api/health` | publik (monitoring) |
| `POST /api/pool-purge` | publik, tetapi wajib `{username, hash}` sesi panel dengan role `admin` (lihat bagian *Purge `proxy_pool`*) |
| `POST /api/state-backup` | sesi atau token API seperti rute `/api/*` lain (endpoint ini menulis berkas backup di host scanner) |
| rute lain | cookie sesi **atau** token API (kalau token diatur) |

Tanpa sesi/token, `/api/*` menjawab `401`, sedangkan halaman HTML dialihkan (`302`) ke `/login`.

**Sandi bawaan wajib diganti saat login pertama.** Selama `admin_password_hash` masih hash
bawaan (`admin`), sesi yang dibuat lewat `POST /api/login` ditandai wajib-ganti-sandi: balasan
login dan `GET /api/me` menyertakan `must_change_password: true`. Sesi bertanda itu hanya boleh
menyentuh `/api/login`, `/api/logout`, `/api/me`, `/api/password`, `/api/health`, dan halaman
statis (path di luar `/api/`); endpoint `/api/*` lain dibalas `403` dengan
`{ "error": "ganti sandi dulu", "must_change_password": true }` sampai sandi diganti. Sesi yang
masuk dengan hash non-bawaan (host yang sandinya sudah diganti, atau `PS_ADMIN_PASSWORD` di
`.env`) **tidak** ditandai, jadi alur normal tidak berubah. Klien mesin dengan `X-Api-Token`
tidak terkena aturan ini.

| Endpoint | Guna |
|---|---|
| `POST /api/login` | masuk. Body `{"username","password"}` → `{ "ok": true, "user": "admin", "must_change_password": false }` + cookie |
| `POST /api/logout` | hapus sesi (cookie dibersihkan) |
| `GET /api/me` | `{ "logged_in": true, "user": "admin", "must_change_password": false }` atau `401` |
| `POST /api/password` | ganti sandi sesi ini. Body `{"old_password","new_password"}` → `{ "ok": true }` |

`POST /api/password` memakai sesi (bukan token API: token mesin tidak punya sandi). Urutan
pemeriksaannya: sandi lama harus cocok dengan hash tersimpan (`401` `sandi lama salah`), lalu
sandi baru tidak boleh `admin` (`400`) dan minimal **8 karakter** (`400`). Setelah berhasil,
hash SHA-256 baru disimpan ke `settings.json` dan tanda wajib-ganti-sandi sesi itu dibersihkan;
sandi tidak pernah masuk log.

Aturan kekuatan yang sama (≥8 karakter, bukan `admin`, case-insensitive) juga berlaku untuk
`admin_password` lewat `POST /api/settings`: nilai yang ditolak dibalas `400` dengan alasan
(`sandi baru minimal 8 karakter` atau `sandi baru tidak boleh "admin" (sandi bawaan)`) dan
seluruh patch dibatalkan — hash lama serta field lain tidak berubah.

```bash
# login lalu pakai cookie untuk permintaan berikutnya
curl -c cookie.txt -X POST http://127.0.0.1:8787/api/login \
  -H "Content-Type: application/json" -d '{"username":"admin","password":"<sandi>"}'
curl -b cookie.txt "http://127.0.0.1:8787/api/proxies.txt?limit=5"

# ganti sandi sesi ini (wajib saat masih memakai sandi bawaan)
curl -b cookie.txt -X POST http://127.0.0.1:8787/api/password \
  -H "Content-Type: application/json" -d '{"old_password":"admin","new_password":"sandi-baru-ku"}'

# klien mesin (tanpa login)
curl -H "X-Api-Token: <token>" "http://127.0.0.1:8787/api/proxies.txt?limit=5"
```

Kalau password diubah lewat `POST /api/settings` (`admin_password`), semua sesi lama tetap
berjalan sampai kedaluwarsa; logout manual untuk mengakhiri lebih cepat. Server hanya
menyimpan **SHA-256 hex** sandi (`admin_password_hash` di `settings.json`), dan hash itu tidak
pernah dikirim ke klien (`GET /api/settings` hanya memberi `admin_password_set`).

## Konfigurasi `.env`

Port dan mode listen diatur dari `.env` (atau flag CLI). Nilai di `.env` **menang** atas flag
dan `settings.json`, dan dibaca saat start.

```env
PS_BIND_LAN=true      # true = 0.0.0.0 (LAN bisa akses), false = 127.0.0.1 saja
PS_HOST=0.0.0.0
PS_PORT=8787
# PS_ADDR=0.0.0.0:8787   # alamat lengkap, menang atas PS_HOST/PS_PORT/PS_BIND_LAN
PS_DB=proxies.db       # folder berkas ini juga jadi induk folder backup: <folder>/backups
PS_ADMIN_USER=admin
PS_ADMIN_PASSWORD=admin
# PS_API_TOKEN=token-klien-mesin
PS_TUNNEL_NAME=panproxyscanner
PS_TUNNEL_DOMAIN=
PS_PUBLIC_URL=
```

Alamat yang benar-benar dipakai bisa dilihat di `GET /api/tunnel` pada objek `listen`.

---

## Sinkronisasi Supabase (tabel `proxy_pool`)

Tujuannya: verifikasi tetap dilakukan **di sini** (bukan di browser panel), lalu panel
CFVPN Hub cukup membaca tabel Supabase (`is_active`, `last_checked`, `ping`).

Isi dulu **URL**, **key**, dan **tabel** di menu **Supabase** (atau lewat `POST /api/settings`).

| Endpoint | Kegunaan |
|---|---|
| `GET /api/supabase/status` | konfigurasi, jumlah baris Supabase terakhir yang diketahui, catatan sinkron terakhir. Tambah `?test=1` untuk benar-benar mengecek ke Supabase (lebih lambat) |
| `POST /api/supabase/test` | uji koneksi: `{ "ok": true, "table": "proxy_pool", "total": 2732, "alive": 413 }` |
| `POST /api/supabase/push?limit=500` | kirim proxy **hidup & segar** dari database lokal ke Supabase (upsert by `ip,port`) memakai **service key** bila terisi (kalau kosong: anon + catatan); kolom yang ditulis: `ip`, `port`, `country`, `org`, `is_active`, `ping`, `last_checked`, bukti `tls_ok`/`alt_ok`/`exit_ip`, dan bukti vantage host `tunnel_host_ok`/`tunnel_host_ms`/`tunnel_host_checked`. `org` hanya diisi kalau barisnya pernah terbaca dari tabel Supabase (scanner tidak menebak penyedia). Kolom bukti yang belum ada di tabel dilepas otomatis (hanya untuk error kolom: HTTP 400 `PGRST204`/`42703`), jadi push tidak gagal di project yang SQL-nya belum dijalankan |
| `POST /api/supabase/pull-verify?limit=50&jobs=100` | tarik baris dari Supabase dengan anon key (yang paling lama tidak dicek), verifikasi satu per satu di sini, lalu tulis balik `is_active`/`ping`/`last_checked` dengan **service key** bila terisi — baris yang mati langsung ditandai. Kolom `org` yang dibaca dari tabel disimpan di database lokal (hanya untuk baris yang sudah dikenal) sebagai bahan `push_org_cap` |
| `POST /api/supabase/prune?days=7&limit=2000` | hapus baris **mati** yang terakhir dicek lebih dari N hari. Balasan `{ "deleted": N, "kandidat_sebelumnya": M }` |
| `POST /api/state-backup` | tulis salinan `user_states` + ringkasan `proxy_pool` ke berkas di host scanner (butuh `supabase_service_key`). Detail di bawah |
| `POST /api/pool-purge` | hapus **semua** baris mati (`is_active = false`) atas permintaan panel CFVPN Hub (whoami + role `admin`, DELETE memakai `supabase_service_key`). Detail di bawah |

**Key mana untuk apa:** semua operasi **baca** (`pull`, `select`, hitung baris, RPC `whoami`)
memakai **`supabase_key` (anon)**; semua operasi **tulis** — upsert `push`, `MarkDead`/
`UpdateVerdict` di `pull-verify`, `prune`, RPC retensi `deploy_log_trim`, dan `backup state` —
memakai **`supabase_service_key`** kalau terisi. Panel sudah mencabut hak tulis anon pada
`proxy_pool` (`sql/2026-11-15-pool-write-lockdown.sql`), jadi tanpa key itu tulis memakai anon
dan Supabase akan menolaknya; scanner mencatat keadaan itu sekali di log dan menuliskannya lagi
di ringkasan sinkron (`GET /api/supabase/status` → `last_sync.note`) serta di `write_note`.

**Catatan `prune`:** DELETE ke `proxy_pool` **wajib** `supabase_service_key` (service_role key),
karena panel tidak lagi memberi anon izin hapus. Tanpa key itu `prune` tidak menembak Supabase
sama sekali dan menjawab `200` dengan `deleted: 0` + `note` bahwa prune butuh
`supabase_service_key` (bukan error 5xx) — fallback ke anon tidak dijalankan karena request itu
pasti ditolak (hak tulis anon sudah dicabut). Di sisi panel, migrasi
`sql/2026-11-15-pool-prune-service-key.sql` (policy DELETE untuk `service_role`) dan
`sql/2026-11-15-pool-write-lockdown.sql` (cabut tulis anon) sudah tersedia; kalau `deleted`
tetap 0 padahal ada kandidat, key itu kemungkinan bukan service_role key.

### Retensi `deploy_log` + backup state (langkah housekeeping pipeline harian)

Dua pekerjaan tambahan dijalankan otomatis pada pipeline harian (bukan pada jadwal "cek lokal"):

1. **`deploy_log_trim`** — RPC `POST <supabase_url>/rest/v1/rpc/deploy_log_trim` dengan body
   `{"p_keep_days": 30}` dan **service key**; balasannya jumlah baris terhapus. Fungsi SQL-nya
   di repo panel (`sql/2026-11-15-deploy-log-trim.sql`, `SECURITY DEFINER`, `EXECUTE` hanya
   untuk `service_role`). Langkah ini **best-effort**: kegagalannya (fungsi belum dijalankan,
   key salah, jaringan) hanya muncul sebagai `note` pada langkah
   `housekeeping deploy_log` — pipeline tidak pernah ditandai gagal karenanya.
2. **`backup state`** — `POST /api/state-backup` menulis salinan tabel `user_states` (dibaca
   dengan service key, kolom `username`/`state_data`/`updated_at` saja — tidak pernah `*`, dan
   kolom bernama sandi/hash/kunci dibuang walau ikut terkirim) ke
   `<folder DB>/backups/user_states-YYYY-MM-DD.json` (folder DB = nilai `-db`/`PS_DB`, bawaan
   `proxies.db`), lengkap dengan ringkasan `generated_at`, `user_states_count`, dan
   `proxy_pool_count`. Setelah menulis, hanya **14 berkas terbaru** yang disisakan. Berkas
   ditulis lewat berkas `.tmp` lalu di-rename, jadi backup separuh jalan tidak pernah terlihat
   sebagai berkas jadi.

Rute ini **tidak publik**: gate umum mewajibkan cookie sesi atau header `X-Api-Token`.

```bash
curl -X POST http://127.0.0.1:8787/api/state-backup -H "X-Api-Token: <token>"
```

```json
{
  "path": "C:\\scanner\\backups\\user_states-2026-11-15.json",
  "bytes": 48210,
  "user_states": 7,
  "proxy_pool": 2732,
  "generated_at": "2026-11-15T05:30:12Z",
  "kept": 14,
  "removed": ["user_states-2026-01-01.json", "user_states-2026-01-02.json"]
}
```

| Status | Balasan | Arti |
|---|---|---|
| `200` | ringkasan di atas | berkas ditulis (dan rotasi dijalankan) |
| `405` | `{ "error": "pakai POST" }` | metode bukan POST |
| `502` | `{ "error": "..." }` | baca `user_states`/hitung `proxy_pool`/menulis berkas gagal |
| `503` | `{ "error": "..." }` | `supabase_service_key` belum diset (tabel `user_states` tidak bisa dibaca anon) |

`GET /api/supabase/status` juga melaporkan `write_key` (`service_key`/`anon_key`) dan
`write_note` (kosong kalau service key terisi), supaya key yang dipakai operasi tulis bisa
diperiksa tanpa menjalankan sinkronisasi.

### Purge `proxy_pool` (dipakai panel CFVPN Hub)

`POST /api/pool-purge` menghapus **semua** baris `is_active = false` di tabel Supabase. Rute
ini publik di scanner, karena panel CFVPN Hub (Pages) tidak memegang `supabase_service_key`:
panel mengirim identitas sesinya sendiri dan scanner yang memutuskan.

```json
{ "username": "budi", "hash": "<hash sesi panel>" }
```

Verifikasi: `POST <supabase_url>/rest/v1/rpc/whoami` dengan body `{p_username, p_hash}` dan
**`supabase_key` (anon)**, role hasilnya wajib `admin`. DELETE-nya memakai
**`supabase_service_key`** ke `<supabase_url>/rest/v1/<supabase_table>?is_active=eq.false`
dengan `Prefer: count=exact, return=minimal`; jumlah terhapus dibaca dari `Content-Range`.

| Status | Balasan | Arti |
|---|---|---|
| `200` | `{ "success": true, "deleted": N }` | semua baris mati terhapus |
| `400` | `{ "error": "..." }` | body bukan JSON, atau `username`/`hash` kosong/tidak valid |
| `401` | `{ "error": "sesi tidak valid — login ulang." }` | hash salah atau user tidak ada |
| `403` | `{ "error": "hanya admin yang boleh menghapus baris proxy." }` | hash benar, role bukan `admin` |
| `405` | `{ "error": "pakai POST" }` | metode bukan POST |
| `409` | `{ "error": "... sql/2026-11-14-login-lockout.sql ..." }` | RPC `whoami` belum ada (404/`PGRST202`/`42883`/`42P01`) |
| `429` | `{ "error": "..." }` + `Retry-After` | limiter per IP (6 permintaan / 10 menit) |
| `502` | `{ "error": "..." }` | `whoami` gagal selain "belum ada", atau DELETE ditolak Supabase |
| `503` | `{ "error": "..." }` | `supabase_service_key` (atau `supabase_url`/`supabase_key`) belum diset |

Gerbangnya **fail-closed**: tanpa service key tidak ada satu pun request ke Supabase, dan anon
key tidak pernah dipakai untuk DELETE. Endpoint ini juga tidak pernah melaporkan `success`
kalau DELETE gagal. Di panel, URL scanner ada di konstanta `SCANNER_URL`
(`frontend/app.js`), jadi pindah host cukup mengubah satu baris.

Dua hal yang ikut menentukan kualitas push:

- **Urut skor**: `push` mengurutkan proxy dengan skor (`ok_count*100 - fail_count*25 - ping/10`)
  dan melewati proxy yang `fail_count > ok_count`.
- **Saringan tambahan** (berlaku di `POST /api/supabase/push`):
  - verdict harus segar ≤ **90 menit** (`pushMaxAge`), walaupun `fresh_minutes` 360;
  - perlu **`ok_count ≥ 2`** sebelum naik (proxy yang baru sekali berhasil ditahan, dicatat sebagai
    `N ditahan (baru 1× berhasil)` di status sinkron);
  - dedup alamat keluar per **/24** (`N dilewati (exit IP sama)`);
  - batas baris per **penyedia (`org`)** dan per **negara**: `push_org_cap` (bawaan 25) dan
    `push_country_cap` (bawaan 60). Karena seleksi sudah urut skor, yang dipertahankan adalah
    baris paling tinggi skornya (ping terkecil) dan sisanya ditahan sampai push berikutnya —
    dicatat sebagai `N ditahan (batas org/negara)`. Setel `0` untuk mematikan batasnya, dan
    `-1`/negatif ditolak (kembali ke bawaan, bukan dianggap "tanpa batas").
    Baris tanpa `org` (penyedianya belum diketahui — scanner hanya membaca `org` dari tabel
    Supabase, tidak menebaknya) tidak dihitung sebagai satu org yang sama, jadi tidak ditahan
    hanya karena cap; hal yang sama berlaku untuk baris tanpa negara.
- **Uji TLS 443 & port lain**: setiap proxy hidup diuji `GET https://cp.cloudflare.com/cdn-cgi/trace`
  di dalam tunnel TLS dengan **verifikasi sertifikat**, lalu (kalau `probe_alt` aktif) trace
  yang sama lewat port 8443/2053. Bukti yang diterima bukan cuma status HTTP: harus ada header
  `cf-ray` dan isi trace `h=cp.cloudflare.com`, sehingga proxy yang membajak HTTP atau
  melakukan MITM gugur. Hasilnya: `tls_ok` (syarat VLESS), `alt_ok`, plus `exit_ip` dan negara
  asli dari `ip=`/`loc=`.
- Filter API: `?tls=true` (wajib sanggup TLS 443), `?alt=true` (wajib sanggup port non-standar),
  `?country=SG,ID`, `?order=score|ping|sources|last_checked`.
- `ping_ms` = latensi **satu** permintaan yang berhasil (hasil tunnel sampai trace dibaca),
  dihaluskan dengan EWMA `0,6 × lama + 0,4 × baru` — bukan total verifikasi. Pada pool gratis
  ini jangkanya ~0,5-3 detik; default filter UI 8000 ms. Proxy mati mengosongkan `ping_ms`.

```bash
# uji koneksi
curl -X POST http://127.0.0.1:8787/api/supabase/test -H "X-Api-Token: <token>"

# kirim 200 proxy hidup terbaik ke Supabase
curl -X POST "http://127.0.0.1:8787/api/supabase/push?limit=200" -H "X-Api-Token: <token>"

# tarik 100 baris terlama dari Supabase dan cek satu per satu
curl -X POST "http://127.0.0.1:8787/api/supabase/pull-verify?limit=100&jobs=100" -H "X-Api-Token: <token>"
```

Balasan `pull-verify`:

```json
{ "checked": 25, "alive": 0, "dead": 25, "errors": 0 }
```

Supaya berjalan sendiri tanpa perintah manual, aktifkan di **Scheduler → Supabase** (atau
`POST /api/settings`):

| Field | Arti |
|---|---|
| `supabase_url` | URL project (mis. `https://xxxx.supabase.co`) |
| `supabase_key` | anon key untuk semua operasi **baca** (pull, select, hitung baris, RPC `whoami`); tidak pernah dikirim balik ke klien (hanya `supabase_key_set: true`) |
| `supabase_service_key` | service_role key untuk **semua operasi tulis** (`push`, `MarkDead`/`UpdateVerdict`, `prune`, RPC `deploy_log_trim`, `state-backup`) dan untuk membaca `user_states`; dipakai kalau terisi, kalau kosong tulis jatuh ke anon + catatan. Tidak pernah dikirim balik ke klien (hanya `supabase_service_key_set: true`); tanpa key ini prune menjawab `deleted: 0` + `note`, dan `POST /api/state-backup` menjawab `503` |
| `supabase_table` | nama tabel, default `proxy_pool` |
| `supabase_auto_push` | `true` = setiap siklus selesai, proxy hidup otomatis dikirim ke Supabase |
| `supabase_pull_limit` | jumlah baris Supabase yang ditarik & diverifikasi tiap siklus (`0` = tidak) |
| `push_org_cap` | maksimum baris **per `org`/penyedia** dalam satu push (bawaan `25`; `0` = tanpa batas; nilai negatif ditolak → kembali ke bawaan). Baris tanpa `org` tidak dihitung |
| `push_country_cap` | maksimum baris **per negara** dalam satu push (bawaan `60`; `0` = tanpa batas; nilai negatif ditolak → kembali ke bawaan). Baris tanpa negara tidak dihitung |
| `push delta` | tidak ada setelan terpisah: waktu push terakhir disimpan di tabel `meta` (`last_push_at`), dan push dari jadwal 6 jam / auto-push hanya mengirim baris yang `last_checked`-nya lebih baru. Kalau tidak ada yang berubah, status sinkron menulis `push delta: tidak ada perubahan` (bukan kegagalan) |
| `probe_tls` | uji tunnel 443 dengan TLS terverifikasi (root CA sistem) untuk setiap proxy hidup |
| `probe_alt` | uji tambahan port non-standar 8443/2053 (`alt_ok`) |
| `push_require_tls` | `false` (default) = semua proxy yang bisa dipakai (TLS maupun HTTP/non-TLS) dikirim dengan `is_active=true` dan `tls_ok` sebagai penanda kualitas; `true` = hanya yang sanggup TLS 443 |
| `supabase_prune_days` | hapus baris mati di Supabase yang lebih tua dari N hari saat pipeline (`0` = tidak) |
| `auto_disable_sources` | matikan sumber yang berkali-kali tidak menghasilkan proxy |
| `zero_streak_limit` | berapa kali kosong berturut-turut sebelum sumber dimatikan otomatis |

Satu operasi sinkronisasi berjalan pada satu waktu; perintah manual akan menjawab `409`
kalau sinkronisasi lain masih jalan. `push` meng-upsert berdasarkan pasangan `(ip, port)`,
jadi aman diulang dan tidak menggandakan baris.

---

## Pipeline harian

Pipeline penuh (harian) berjalan berurutan:

1. **tarik dari Supabase** baris yang paling lama tidak dicek
2. **uji satu per satu** di sini (handshake nyata) lalu tulis verdict kembali ke Supabase
3. **scrape semua sumber** → verifikasi → simpan ke database lokal
4. **push** proxy hidup & segar ke Supabase (harian: menyeluruh; jadwal 6 jam: hanya yang berubah)
5. **prune** baris mati lama (butuh service key)
6. **housekeeping `deploy_log`** — RPC `deploy_log_trim(p_keep_days=30)`, best-effort (kegagalan
   hanya jadi `note` langkah, bukan kegagalan pipeline)
7. **backup state** — salinan `user_states` + ringkasan `proxy_pool` ke `<folder DB>/backups`
   (rotasi 14 berkas)
8. **housekeeping database lokal** — buang kandidat gagal berulang

Jadwal "cek lokal" (bawaan tiap 6 jam) tidak menjalankan langkah 1, 5, 6, dan 7; langkahnya
muncul di status dengan catatan `dilewati`. Kegagalan langkah 6 tidak pernah mengisi
`pipeline.error`, sedangkan kegagalan langkah 7 **dianggap gagal** (backup adalah jaring
pengaman data pengguna, jadi tidak boleh lewat diam-diam).

| Endpoint | Kegunaan |
|---|---|
| `GET /api/pipeline` | status pipeline terakhir (langkah + hasil) beserta konfigurasi jadwal |
| `POST /api/pipeline/run` | jalankan pipeline sekarang (di latar belakang, `202`); `409` kalau sudah ada yang berjalan |

```bash
curl -X POST http://127.0.0.1:8787/api/pipeline/run -H "X-Api-Token: <token>"
curl http://127.0.0.1:8787/api/pipeline -H "X-Api-Token: <token>"
```

Contoh balasan `GET /api/pipeline`:

```json
{
  "pipeline": {
    "running": false,
    "started_at": "2026-11-12T14:13:00+08:00",
    "ended_at": "2026-11-12T14:14:20+08:00",
    "steps": [
      { "name": "tarik & verifikasi Supabase", "ok": true, "note": "30 baris diuji, 0 masih hidup", "counts": { "dicek": 30, "hidup": 0, "mati": 30, "error": 0 }, "ms": 10727 },
      { "name": "scrape sumber & verifikasi", "ok": true, "note": "220 kandidat diperiksa, 14 hidup", "counts": { "kandidat": 200, "dicek": 220, "hidup": 14, "mati": 206 }, "ms": 30004 },
      { "name": "push ke Supabase", "ok": true, "note": "97 proxy dikirim", "counts": { "dikirim": 97 }, "ms": 587 },
      { "name": "prune baris mati Supabase", "ok": true, "note": "12 baris mati (> 7 hari) dihapus", "counts": { "dihapus": 12, "hari": 7 }, "ms": 2204 },
      { "name": "housekeeping deploy_log", "ok": true, "note": "31 baris riwayat deploy (> 30 hari) dihapus", "counts": { "dihapus": 31, "hari": 30 }, "ms": 410 },
      { "name": "backup state user_states", "ok": true, "note": "7 baris user_states + 2732 baris proxy_pool → /srv/scanner/backups/user_states-2026-11-15.json (48210 B, 14 berkas disimpan)", "counts": { "user_states": 7, "proxy_pool": 2732, "disimpan": 14, "dihapus": 0 }, "ms": 880 }
    ]
  },
  "config": {
    "schedule_mode": "daily", "daily_at": "05:30", "interval_minutes": 30,
    "pipeline_enabled": true, "pipeline_pull_limit": 500, "pipeline_push_limit": 1000,
    "auto": true, "scheduler_running": true, "next_run": "2026-11-13 05:30"
  }
}
```

Field pengaturan terkait:

| Field | Arti |
|---|---|
| `schedule_mode` | `daily` (jam tetap) atau `interval` (tiap N menit) |
| `daily_at` | jam jalan untuk mode `daily`, format `HH:MM` waktu lokal |
| `interval_minutes` | jarak antar jadwal untuk mode `interval` |
| `auto` | penjadwal aktif/mati |
| `pipeline_enabled` | `true` = jadwal menjalankan pipeline penuh; `false` = hanya siklus scraping |
| `pipeline_pull_limit` | baris Supabase yang diuji di langkah 1 (0 = pakai `supabase_pull_limit`) |
| `pipeline_push_limit` | berapa proxy dikirim ke Supabase di langkah 4 (0 = tidak push) |

Catatan penulisan verdict: hasil verifikasi dikirim **per batch** — baris yang hidup lewat
satu upsert (kolom `ping` berbeda per baris), baris yang mati lewat satu `PATCH id=in.(...)`
per 200 id. Jadi menguji 500 baris ≈ 3-4 request, bukan 500. Kedua jalur itu operasi **tulis**,
jadi memakai `supabase_service_key` bila terisi (barisnya ditarik dengan anon key).

### Notifikasi Telegram

Scanner mengirim notifikasi langsung ke HTTP API Telegram (`POST
https://api.telegram.org/bot<token>/sendMessage`, `net/http` biasa — tanpa dependensi baru).
Notifikasi **aktif hanya kalau `telegram_bot_token` DAN `telegram_chat_id` keduanya terisi**;
setelannya bisa diisi di **Scheduler → Notifikasi Telegram** atau lewat `POST /api/settings`.
Token tidak pernah dikembalikan ke klien (balasan hanya memuat `telegram_bot_token_set: true`),
chat id boleh dibaca.

Hanya dua momen yang dinotifikasi:

1. **Setelah langkah scrape penuh selesai** (run harian maupun catch-up; bukan cek lokal),
   dikirim setelah langkah push supaya jumlah baris ke pool ikut terlaporkan:

   ```
   Pan-VlessCloudflare - scrape harian
   Kandidat ditarik: 1200
   Hidup: 140
   Mati: 1060
   Dikirim ke pool: 97
   Waktu: selesai dalam 34 menit
   ```

   Angka diambil dari `counts` langkah `scrape sumber & verifikasi`; `Dikirim ke pool` dari
   `counts.dikirim` langkah `push ke Supabase`, dan ditulis `-` kalau langkah push dilewati.

2. **Setelah langkah cek lokal 6 jam selesai**:

   ```
   Pan-VlessCloudflare - cek 6 jam
   Diperiksa ulang: 300
   Masih hidup: 88
   Sudah mati: 212
   ```

   `Sudah mati` diambil dari `counts.mati` langkah, atau dihitung `diperiksa - hidup` kalau
   langkah itu tidak melaporkannya.

| Kejadian | Perilaku |
|---|---|
| Setelan belum lengkap | tidak ada pesan, tidak ada request (bukan kesalahan) |
| Kirim gagal (jaringan atau Telegram menolak) | satu baris log + catatan `notifikasi gagal: ...` pada langkah pipeline terkait; **pipeline tetap sukses** |
| Timeout satu kirim | 15 detik |

`POST /api/notify-test` mengirim pesan uji memakai setelan tersimpan dan meneruskan hasil
balasan Telegram apa adanya. Rute ini butuh sesi atau token API seperti rute `/api/*` lain.

| Status | Balasan | Arti |
|---|---|---|
| `200` | `{ "ok": true, "telegram_status": 200 }` | Telegram menerima pesan |
| `400` | `{ "ok": false, "error": "...", "missing": ["telegram_chat_id"] }` | token ada tapi chat id belum diisi |
| `503` | `{ "ok": false, "error": "...", "missing": ["telegram_bot_token", "telegram_chat_id"] }` | setelan Telegram belum diisi sama sekali |
| `502` | `{ "ok": false, "error": "...", "telegram_status": 400 }` | Telegram menolak (`telegram_status` = balasan Telegram) atau request tidak sampai (`0`) |

```bash
curl -X POST http://127.0.0.1:8787/api/notify-test -H "X-Api-Token: <token>"
```

---

## Format data

Objek proxy (dipakai di semua respons daftar):

```json
{
  "ip": "45.194.33.12",
  "port": 30001,
  "protocol": "socks5",         // "socks5" | "http" | ""
  "country": "HK",              // kode 2 huruf bila sumber menyediakan, "" kalau tidak
  "alive": true,
  "ping_ms": 838,               // hasil ukur verifikasi terakhir (ms)
  "last_checked": 1794451751,   // unix detik
  "first_seen": 1794400000,
  "ok_count": 3,                // berapa kali berturut-turut pernah hidup
  "fail_count": 1,
  "sources": 4                  // muncul di berapa sumber (sinyal kualitas)
}
```

Semua error berbentuk `{ "error": "<pesan>" }` dengan status HTTP yang sesuai
(`400` input salah, `401` token, `404` tidak ada data, `405` metode salah, `409` siklus
lain masih berjalan, `500` kesalahan internal).

---

## Daftar proxy

### `GET /api/proxies`

Query parameter (semua opsional):

| Parameter | Arti | Default |
|---|---|---|
| `alive` | `true` / `false` / kosong (semua) | semua |
| `protocol` | `socks5` atau `http` | semua |
| `country` | kode negara, bisa beberapa: `SG,ID,HK` | semua |
| `tls` | `true` = hanya yang terbukti sanggup tunnel TLS 443 (syarat VLESS) | semua |
| `alt` | `true` = hanya yang sanggup port non-standar 8443/2053 | semua |
| `max_ping` | hanya ping ≤ N ms | semua |
| `min_sources` | hanya yang muncul di ≥ N sumber | `0` |
| `fresh_minutes` | hanya yang dicek dalam N menit terakhir | semua |
| `limit` | jumlah baris (maks 5000) | `200` |
| `offset` | lewati N baris (paging) | `0` |
| `order` | `score` \| `ping` (tercepat dulu) \| `last_checked` \| `sources` | `ping` |

```bash
curl "http://127.0.0.1:8787/api/proxies?alive=true&country=SG,ID&max_ping=1500&min_sources=2&limit=20"
```

```json
{
  "count": 2,
  "proxies": [
    { "ip": "8.219.122.8", "port": 9999, "protocol": "socks5", "country": "SG", "alive": true, "ping_ms": 655, "last_checked": 1794451751, "first_seen": 1794400000, "ok_count": 4, "fail_count": 0, "sources": 3 },
    { "ip": "128.199.202.122", "port": 1080, "protocol": "socks5", "country": "SG", "alive": true, "ping_ms": 992, "last_checked": 1794451751, "first_seen": 1794400000, "ok_count": 2, "fail_count": 1, "sources": 2 }
  ]
}
```

### `GET /api/proxies.txt`

Parameter sama dengan `/api/proxies`. Balasan `text/plain`, satu `ip:port` per baris —
format yang dipakai generator VLESS.

```bash
curl "http://127.0.0.1:8787/api/proxies.txt?max_ping=2000&fresh_minutes=120&limit=5"
```

```
8.219.122.8:9999
128.199.202.122:1080
45.194.33.12:30001
68.71.240.210:4145
216.68.128.121:4145
```

### `GET /api/countries`

| Parameter | Arti | Default |
|---|---|---|
| `all` | `true` = hitung semua baris, bukan hanya yang hidup | `false` |

```json
{
  "count": 3,
  "countries": [
    { "country": "US", "alive": 22, "total": 210 },
    { "country": "SG", "alive": 5, "total": 30 },
    { "country": "ID", "alive": 2, "total": 12 }
  ]
}
```

---

## Statistik & status

### `GET /api/stats`

```json
{
  "stats": {
    "total": 900, "alive": 65, "alive_fresh": 65,
    "alive_socks5": 65, "alive_http": 0,
    "avg_ping_ms": 2801, "last_checked": 1794451751, "fresh_minutes": 360
  },
  "progress": {
    "running": false, "phase": "selesai",
    "sources": [ { "name": "proxifly-all", "kind": "json", "count": 10815, "bytes": 3020344, "ms": 1286, "status": 200, "optional": false } ],
    "candidates": 500, "checked": 550, "alive": 52, "dead": 498, "refreshed": 36,
    "started_at": "2026-11-12T11:00:00Z", "ended_at": "2026-11-12T11:00:41Z"
  },
  "auto": { "running": true, "last_run": "2026-11-12T11:00:00Z", "interval_minutes": 30 }
}
```

Field `progress.phase`: `fetch` → `verify` → `refresh` → `selesai`.

### `GET /api/sources`

Katalog sumber **dari database** (termasuk yang ditambahkan lewat website) + hasil siklus
terakhir per sumber.

```json
{
  "count": 56, "enabled": 55, "running": true,
  "sources": [
    {
      "name": "proxifly-all", "url": "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.json",
      "kind": "json", "optional": false, "enabled": true,
      "status": 200, "count": 10815, "bytes": 3020344, "ms": 1286
    },
    { "name": "tambahan-saya", "url": "https://contoh.com/list.txt", "kind": "txt", "optional": false, "enabled": false }
  ]
}
```

### `POST /api/sources`

Tambah sumber baru. Body: `{"url": "...", "name": "opsional", "kind": "txt|json|api|html", "optional": false}`.
`url` wajib `http://`/`https://`; `name` kosong → dibuat otomatis dari URL.

```bash
curl -X POST http://127.0.0.1:8787/api/sources -H "Content-Type: application/json" \
  -d '{"url":"https://contoh.com/socks5.txt","name":"sumber-baru"}'
```

`201` → `{ "added": "sumber-baru" }` · `400` URL tidak valid · `409` nama sudah dipakai.

### `DELETE /api/sources?name=`

Hapus sumber (bawaan maupun tambahan).

### `POST /api/sources/toggle?name=&enabled=true|false`

Menyalakan/mematikan sumber tanpa menghapusnya. Sumber yang dimatikan tidak diambil pada
siklus berikutnya, tetapi url dan catatannya tetap ada.

Sumber yang baru ditambahkan langsung ikut pada siklus berikutnya (`POST /api/now` untuk
menjalankannya segera); parser-nya generik — apa pun yang memuat pola `ip:port` (teks, JSON,
tabel HTML) akan terbaca.

### `GET /api/health`

```json
{ "ok": true, "time": "2026-11-12T11:00:00Z" }
```

### `GET /api/tunnel`

Membantu menyiapkan Cloudflare Tunnel dan melaporkan apakah permintaan saat ini datang lewat
Cloudflare.

```json
{
  "listen": { "addr": "0.0.0.0:8787", "host": "0.0.0.0", "port": "8787", "bind_lan": true, "note": "listen di semua interface (LAN bisa akses)" },
  "local_urls": ["http://localhost:8787", "http://192.168.42.124:8787"],
  "env_file": ".env",
  "settings_file": "C:\\Users\\ACER\\.panproxyscanner\\settings.json",
  "public_url": "https://scraping.example.com",
  "tunnel_name": "panproxyscanner",
  "tunnel_domain": "scraping.example.com",
  "cf_ray": "8f2c1a...-CGK",
  "cf_connecting_ip": "103.x.x.x",
  "forwarded_host": "scraping.example.com",
  "via_cloudflare": true,
  "state": {
    "service": { "unit": "cloudflared-panproxyscanner.service", "available": true, "load": "loaded", "active": "active", "sub": "running", "enabled": "enabled", "since": "Thu 2026-11-12 23:00:28 WIB", "restarts": "0" },
    "public": { "url": "https://scraping.example.com/api/health", "ok": true, "status": 200, "ms": 231, "checked_at": "2026-11-13T07:20:11+08:00" },
    "checked_at": "2026-11-13T07:20:11+08:00"
  },
  "quick_tunnel_command": "cloudflared tunnel --url http://localhost:8787",
  "named_tunnel_steps": [
    "cloudflared tunnel login",
    "cloudflared tunnel create panproxyscanner",
    "cloudflared tunnel route dns panproxyscanner scraping.example.com",
    "cloudflared tunnel run --url http://localhost:8787 panproxyscanner"
  ],
  "config_yaml": "tunnel: panproxyscanner\ncredentials-file: <path>\n\ningress:\n  - hostname: scraping.example.com\n    service: http://localhost:8787\n    originRequest:\n      noTLSVerify: true\n  - service: http_status:404\n",
  "notes": ["Panel ini bind 0.0.0.0:8787 — cloudflared dijalankan di mesin yang sama, mengarah ke http://localhost:8787.", "..."]
}
```

Cara pakai dengan domain sendiri:

1. `cloudflared tunnel login` → `cloudflared tunnel create <tunnel_name>`
2. `cloudflared tunnel route dns <tunnel_name> <tunnel_domain>`
3. Isi **Public URL** dan **Domain tunnel** di menu *Scheduler → Akun & API* (atau lewat `POST /api/settings`)
4. `cloudflared tunnel run --url <local_url> <tunnel_name>` (atau pakai `config_yaml` di atas sebagai service)

Port yang dipakai tunnel selalu port panel (`listen.port`), dan `listen.host` menentukan
apakah LAN ikut bisa mengakses (`0.0.0.0`) atau hanya mesin lokal (`127.0.0.1`).

Kolom `via_cloudflare` bernilai `true` bila permintaan memuat header `Cf-Ray`, yaitu saat
diakses lewat tunnel Cloudflare.

#### Kondisi tunnel (`state`)

Objek `state` dipakai halaman **Tunnel CF** untuk menampilkan kondisi nyata, bukan cuma
konfigurasi:

- `state.service` — status unit systemd `cloudflared-<tunnel_name>.service` (nama yang dipasang
  installer): `available` (unit dikenal), `active`/`sub` (mis. `active`/`running`), `enabled`,
  `since`, `restarts`. Di sistem tanpa systemd (Windows, Termux) nilainya `available:false`
  plus `error`, bukan galat HTTP.
- `state.public` — hasil panel memanggil `/api/health` **miliknya sendiri lewat Public URL**,
  jadi `ok:true` berarti jalur Cloudflare benar-benar tembus dari luar sampai ke panel
  (`status`, `ms`, `checked_at`).

Uji jalur publik disegarkan di latar belakang (paling cepat tiap 30 detik) supaya
`GET /api/tunnel` tidak ikut menunggu jaringan. Pakai `?probe=1` untuk memaksa uji baru dan
menunggu hasilnya — itu yang dipanggil tombol **Uji ulang**:

```bash
curl "http://127.0.0.1:8787/api/tunnel?probe=1" -H "X-Api-Token: <token>"
```

---

## Mengatur siklus

### `POST /api/now`

Menjalankan satu siklus sekarang memakai pengaturan tersimpan (slice otomatis digeser kalau
`auto_rotate` aktif).

```bash
curl -X POST http://127.0.0.1:8787/api/now
```

`202` → `{ "started": true, "settings": { ... } }`, `409` kalau siklus lain masih berjalan.

### `POST /api/scrape`

Siklus dengan opsi eksplisit, tidak mengubah pengaturan.

| Parameter | Arti |
|---|---|
| `cap` | batas kandidat baru |
| `offset` | geser slice kandidat |
| `jobs` | verifikasi paralel (maks 2000) |
| `min_sources` | hanya kandidat yang muncul di ≥ N sumber |

```bash
curl -X POST "http://127.0.0.1:8787/api/scrape?cap=500&jobs=150"
```

### `POST /api/stop`

Membatalkan siklus yang sedang berjalan.

### `POST /api/auto/start` · `POST /api/auto/stop`

Menyalakan/mematikan penjadwal. Saat dinyalakan, satu siklus langsung dijalankan, lalu
mengikuti `interval_minutes` dari pengaturan.

### `POST /api/purge`

Housekeeping database lokal. Dua aturan:

| Parameter | Arti | Default |
|---|---|---|
| `hours` | hapus catatan **mati** yang terakhir dicek lebih tua dari N jam | `24` |

Selain itu, kandidat yang sudah **≥2 kali diuji dan belum pernah berhasil** selalu dihapus
(daftar mass-scan menyumbang mayoritas baris seperti ini). Balasannya:

```json
{ "removed": 0, "older_than_hours": 168, "gagal_berulang": 1108,
  "note": "gagal_berulang = baris yang sudah >=2 kali diuji dan belum pernah hidup" }
```

Aturan yang sama dijalankan sebagai langkah terakhir pipeline harian, jadi database tetap
ramping tanpa perlu tombol manual.

---

## Pengaturan

### `GET /api/settings`

```json
{
  "settings": {
    "auto": true, "interval_minutes": 30, "auto_rotate": true,
    "cap": 20000, "jobs": 150, "min_sources": 0,
    "refresh_limit": 300, "refresh_after_minutes": 30,
    "fresh_minutes": 360, "purge_dead_hours": 0
  },
  "runtime": {
    "addr": "0.0.0.0:8787",
    "lan_urls": ["http://192.168.1.10:8787"],
    "db": "C:\\Users\\ACER\\.panproxyscanner\\settings.json",
    "auto_running": true,
    "api_token_required": false,
    "sources": 55
  }
}
```

### `POST /api/settings`

Body JSON boleh sebagian (field yang tidak dikirim tidak berubah). Semua nilai
dinormalisasi (mis. `jobs` dijepit 1–2000, `interval_minutes` 1–1440). Perubahan langsung
disimpan ke `settings.json` dan diterapkan ke penjadwal.

| Field | Arti | Rentang |
|---|---|---|
| `auto` | penjadwal aktif | bool |
| `interval_minutes` | jarak antar siklus | 1–1440 |
| `auto_rotate` | geser slice kandidat tiap siklus | bool |
| `cap` | kandidat baru per siklus | 50–500000 |
| `jobs` | verifikasi paralel | 1–2000 |
| `min_sources` | hanya kandidat dari ≥ N sumber | ≥0 |
| `refresh_limit` | baris lama yang diverifikasi ulang per siklus | 0–50000 |
| `refresh_after_minutes` | umur catatan sebelum diverifikasi ulang | ≥1 |
| `fresh_minutes` | jendela "hidup & segar" di statistik | ≥1 |
| `purge_dead_hours` | housekeeping otomatis (0 = tidak) | ≥0 |
| `admin_user` | username login website | string non-kosong |
| `admin_password` | sandi login (ditulis sebagai hash; kosong = tidak diganti). Wajib ≥8 karakter dan bukan `admin` (case-insensitive) — aturan yang sama dengan `POST /api/password`; nilai yang ditolak dibalas `400` tanpa mengubah settings | string |
| `public_url` | URL publik hasil tunnel (informasi di halaman Tunnel CF) | string/URL |
| `tunnel_name` | nama tunnel Cloudflare (dipakai di perintah & config.yml) | string |
| `tunnel_domain` | domain yang diarahkan ke panel | hostname |
| `supabase_url` | URL project Supabase tujuan sinkron | URL |
| `supabase_key` | anon/service key Supabase (hanya ditulis, tidak pernah dikembalikan) | string |
| `supabase_table` | tabel tujuan, default `proxy_pool` | string |
| `supabase_auto_push` | dorong hasil siklus ke Supabase otomatis | bool |
| `supabase_pull_limit` | baris Supabase yang diverifikasi ulang tiap siklus | 0–20000 |
| `push_org_cap` | maksimum baris per `org`/penyedia dalam satu push (0 = tanpa batas) | 0–10000 (negatif → bawaan) |
| `push_country_cap` | maksimum baris per negara dalam satu push (0 = tanpa batas) | 0–10000 (negatif → bawaan) |
| `token` | token API untuk klien mesin (kosong = `/api/*` hanya lewat sesi) | string |
| `telegram_bot_token` | token bot Telegram dari BotFather (hanya ditulis, tidak pernah dikembalikan — balasan hanya memuat `telegram_bot_token_set`) | string |
| `telegram_chat_id` | tujuan notifikasi (chat/grup id, boleh dikosongkan untuk mematikan) | string |

```bash
curl -X POST http://127.0.0.1:8787/api/settings \
  -H "Content-Type: application/json" \
  -d '{"auto": true, "interval_minutes": 15, "cap": 30000, "jobs": 200}'
```

---

## Integrasi VLESS

Worker VLESS menerima target proxy dari path WebSocket `/<ip>-<port>`, jadi kedua endpoint
berikut langsung bisa dipakai di klien.

### `GET /api/vless/clash`

| Parameter | Wajib | Arti |
|---|---|---|
| `host` | ya | hostname worker (mis. `vpn-worker-prem.adelia140845.workers.dev`) |
| `uuid` | ya | UUID VLESS yang dipakai worker |
| `port` | tidak | port worker, default `443` (= TLS) |
| filter proxy | tidak | `alive`, `country`, `tls`, `alt`, `max_ping`, `min_sources`, `fresh_minutes`, `limit`, `order` |

Balasan `text/yaml` berisi `proxies` + `proxy-groups` `url-test` + `rules`.

```bash
curl -o vless.yaml "http://127.0.0.1:8787/api/vless/clash?host=<worker>.workers.dev&uuid=<uuid>&max_ping=2000&limit=100"
```

```yaml
proxies:
  - name: "1 45.194.33.12:30001 838ms"
    type: vless
    server: <worker>.workers.dev
    port: 443
    uuid: <uuid>
    udp: true
    tls: true
    network: ws
    servername: <worker>.workers.dev
    ws-opts:
      path: "/45.194.33.12-30001"
      headers:
        Host: <worker>.workers.dev
```

### `GET /api/vless/raw`

Parameter sama; balasan `text/plain` berisi URI `vless://` per proxy.

```bash
curl "http://127.0.0.1:8787/api/vless/raw?host=<worker>.workers.dev&uuid=<uuid>&limit=2"
```

```
vless://<uuid>@<worker>.workers.dev:443?encryption=none&type=ws&host=<worker>.workers.dev&security=tls&sni=<worker>.workers.dev&path=%2F45.194.33.12-30001#45.194.33.12%3A30001%20838ms
```

---

## Contoh pemakaian dari panel CFVPN Hub

```js
// isi proxy untuk generator lokal (ganti sumber proxy_pool Supabase)
const list = await fetch("http://127.0.0.1:8787/api/proxies.txt?max_ping=2000&fresh_minutes=120&limit=200")
  .then((r) => r.text());
const proxies = list.trim().split("\n").map((line) => {
  const [ip, port] = line.split(":");
  return { prxIP: ip, prxPort: port };
});
```

Semantik yang perlu diperhatikan:

- `fresh_minutes` mengikuti jendela kesegaran yang diatur di sini (`fresh_minutes` pada
  pengaturan); panel sebaiknya memakai nilai ≤ jendela itu.
- `ping_ms` adalah hasil ukur nyata (handshake SOCKS5/HTTP CONNECT + `GET cp.cloudflare.com/generate_204`),
  bukan ping ICMP.
- `sources` = berapa sumber independen mencantumkan proxy itu; nilai tinggi = lebih stabil.
