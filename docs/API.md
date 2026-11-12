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
| rute lain | cookie sesi **atau** token API (kalau token diatur) |

Tanpa sesi/token, `/api/*` menjawab `401`, sedangkan halaman HTML dialihkan (`302`) ke `/login`.

| Endpoint | Guna |
|---|---|
| `POST /api/login` | masuk. Body `{"username","password"}` → `{ "ok": true, "user": "admin" }` + cookie |
| `POST /api/logout` | hapus sesi (cookie dibersihkan) |
| `GET /api/me` | `{ "logged_in": true, "user": "admin" }` atau `401` |

```bash
# login lalu pakai cookie untuk permintaan berikutnya
curl -c cookie.txt -X POST http://127.0.0.1:8787/api/login \
  -H "Content-Type: application/json" -d '{"username":"admin","password":"admin"}'
curl -b cookie.txt "http://127.0.0.1:8787/api/proxies.txt?limit=5"

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
PS_DB=proxies.db
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
| `POST /api/supabase/push?limit=500` | kirim proxy **hidup & segar** dari database lokal ke Supabase (upsert by `ip,port`); kolom yang ditulis: `ip`, `port`, `country`, `is_active=true`, `ping`, `last_checked` |
| `POST /api/supabase/pull-verify?limit=50&jobs=100` | tarik baris dari Supabase (yang paling lama tidak dicek), verifikasi satu per satu di sini, lalu tulis balik `is_active`/`ping`/`last_checked` — baris yang mati langsung ditandai |
| `POST /api/supabase/prune?days=7&limit=2000` | hapus baris **mati** yang terakhir dicek lebih dari N hari. Balasan `{ "deleted": N, "kandidat_sebelumnya": M }` |

**Catatan `prune`:** aplikasi memakai anon key, jadi Supabase hanya mengizinkan hapus kalau
ada policy `for delete to anon`. Jalankan **`docs/supabase-policies.sql`** sekali di Supabase
SQL Editor; tanpa policy itu `prune` menjawab `deleted: 0` beserta petunjuk (bukan error),
sedangkan `push`/`pull-verify` tetap normal.

Dua hal yang ikut menentukan kualitas push:

- **Urut skor**: `push` mengurutkan proxy dengan skor (`ok_count*100 - fail_count*25 - ping/10`)
  dan melewati proxy yang `fail_count > ok_count`.
- **Saringan tambahan** (berlaku di `POST /api/supabase/push`):
  - verdict harus segar ≤ **90 menit** (`pushMaxAge`), walaupun `fresh_minutes` 360;
  - perlu **`ok_count ≥ 2`** sebelum naik (proxy yang baru sekali berhasil ditahan, dicatat sebagai
    `N ditahan (baru 1× berhasil)` di status sinkron);
  - dedup alamat keluar per **/24** (`N dilewati (exit IP sama)`).
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
| `supabase_key` | anon atau service key (tidak pernah dikirim balik ke klien; hanya `supabase_key_set: true`) |
| `supabase_table` | nama tabel, default `proxy_pool` |
| `supabase_auto_push` | `true` = setiap siklus selesai, proxy hidup otomatis dikirim ke Supabase |
| `supabase_pull_limit` | jumlah baris Supabase yang ditarik & diverifikasi tiap siklus (`0` = tidak) |
| `probe_tls` | uji tunnel 443 dengan TLS terverifikasi (root CA sistem) untuk setiap proxy hidup |
| `probe_alt` | uji tambahan port non-standar 8443/2053 (`alt_ok`) |
| `push_require_tls` | `true` (default) = hanya proxy yang sanggup TLS 443 dikirim ke Supabase |
| `supabase_prune_days` | hapus baris mati di Supabase yang lebih tua dari N hari saat pipeline (`0` = tidak) |
| `auto_disable_sources` | matikan sumber yang berkali-kali tidak menghasilkan proxy |
| `zero_streak_limit` | berapa kali kosong berturut-turut sebelum sumber dimatikan otomatis |

Satu operasi sinkronisasi berjalan pada satu waktu; perintah manual akan menjawab `409`
kalau sinkronisasi lain masih jalan. `push` meng-upsert berdasarkan pasangan `(ip, port)`,
jadi aman diulang dan tidak menggandakan baris.

---

## Pipeline harian

Satu pipeline = empat pekerjaan berurutan:

1. **tarik dari Supabase** baris yang paling lama tidak dicek
2. **uji satu per satu** di sini (handshake nyata) lalu tulis verdict kembali ke Supabase
3. **scrape semua sumber** → verifikasi → simpan ke database lokal
4. **push** proxy hidup & segar ke Supabase

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
      { "name": "push ke Supabase", "ok": true, "note": "97 proxy dikirim", "counts": { "dikirim": 97 }, "ms": 587 }
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
per 200 id. Jadi menguji 500 baris ≈ 3-4 request, bukan 500.

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
| `admin_password` | sandi login (ditulis sebagai hash; kosong = tidak diganti) | string |
| `public_url` | URL publik hasil tunnel (informasi di halaman Tunnel CF) | string/URL |
| `tunnel_name` | nama tunnel Cloudflare (dipakai di perintah & config.yml) | string |
| `tunnel_domain` | domain yang diarahkan ke panel | hostname |
| `supabase_url` | URL project Supabase tujuan sinkron | URL |
| `supabase_key` | anon/service key Supabase (hanya ditulis, tidak pernah dikembalikan) | string |
| `supabase_table` | tabel tujuan, default `proxy_pool` | string |
| `supabase_auto_push` | dorong hasil siklus ke Supabase otomatis | bool |
| `supabase_pull_limit` | baris Supabase yang diverifikasi ulang tiap siklus | 0–20000 |
| `token` | token API untuk klien mesin (kosong = `/api/*` hanya lewat sesi) | string |

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
