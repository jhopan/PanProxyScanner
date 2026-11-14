// Docs: mekanisme baru (penanda harian, cek lokal 6 jam, push delta, semua proxy dipakai).
import fs from "node:fs";

let a = fs.readFileSync("AGENTS.md", "utf8");
const anchor = "- **Dua jadwal**: `schedule_mode=daily`";
if (!a.includes(anchor)) { console.error("anchor AGENTS tidak ada"); process.exit(1); }
a = a.replace(anchor, [
  "- **Penanda scrape harian**: tanggal scrape penuh terakhir disimpan di tabel `meta`",
  "  (`last_full_scrape`). Saat proses start, kalau hari ini (zona jadwal) belum ada scrape penuh,",
  "  pipeline langsung dijalankan (catch-up) lalu penandanya diperbarui. Jadi minimal satu scrape",
  "  penuh per hari tetap terjadi walau service sempat mati.",
  "- **Push hanya yang berubah**: waktu push terakhir disimpan di `meta.last_push_at`; push dari",
  "  jadwal 6 jam / auto-push hanya mengirim baris yang `last_checked`-nya lebih baru dari titik",
  "  itu (`Filter.Since`). Pipeline harian tetap mengirim menyeluruh (Since kosong).",
  "- **Semua proxy yang bisa dipakai dipublikasikan**: `push_require_tls` kini bawaan **false** —",
  "  baris hidup (TLS maupun non-TLS) naik ke `is_active=true`, dan `tls_ok` hanya menjadi penanda",
  "  kualitas yang dipakai panel/worker untuk memilih. Aturan lama (\"hanya TLS yang boleh aktif\")",
  "  diganti karena klien bisa memakai config NTLS; bukti `tls_ok/alt_ok/exit_ip` tetap dikirim.",
  anchor,
].join("\n"));
a = a.replace("- **Dua jadwal**: `schedule_mode=daily` + `daily_at` pada `schedule_timezone` (bawaan 08:00\n  Asia/Makassar = 00:00 UTC) menjalankan pipeline penuh (scrape semua sumber → push), sedangkan\n  `test_interval_hours` (bawaan 6) menjalankan **tes berkala** pada kelipatan jam dari 00:00 UTC:\n  verifikasi ulang baris tersimpan + pull-verify Supabase + push bukti, tanpa scrape baru.",
  "- **Dua jadwal**: `schedule_mode=daily` + `daily_at` pada `schedule_timezone` (bawaan 08:00\n  Asia/Makassar = 00:00 UTC) menjalankan pipeline penuh (pull-verify → scrape semua sumber → push\n  menyeluruh → prune). `test_interval_hours` (bawaan 6, kelipatan dari 00:00 UTC) menjalankan\n  **cek lokal murni**: refresh baris tersimpan + pass target negara (tanpa ambil sumber, tanpa\n  pull Supabase, tanpa prune), lalu push delta.");
fs.writeFileSync("AGENTS.md", a);

let r = fs.readFileSync("README.md", "utf8");
const rAnchor = "- **Tes berkala** tiap `test_interval_hours` jam (bawaan 6, kelipatan dari 00:00 UTC): hanya\n  menguji ulang baris tersimpan + pull-verify + push, supaya pool terjaga tanpa memakan sumber.";
if (!r.includes(rAnchor)) { console.error("anchor README tidak ada"); process.exit(1); }
r = r.replace(rAnchor, [
  "- **Penanda harian**: kalau hari ini scrape penuh belum jalan (mis. server mati saat 08:00),",
  "  pipeline menjalankannya begitu service hidup (catch-up, disimpan di tabel `meta`).",
  "- **Cek lokal** tiap `test_interval_hours` jam (bawaan 6, kelipatan dari 00:00 UTC): hanya",
  "  refresh baris tersimpan + pass target negara, tanpa mengambil sumber, lalu **push delta**",
  "  (hanya baris yang berubah sejak push terakhir) ke Supabase.",
  "- **Yang dikirim ke Supabase**: semua proxy yang bisa dipakai (TLS maupun non-TLS), dengan",
  "  `tls_ok`/`alt_ok`/`exit_ip` sebagai penanda kualitas; baris yang di edge terbukti mati tetap",
  "  ditahan.",
].join("\n"));
fs.writeFileSync("README.md", r);
console.log("AGENTS.md + README.md: mekanisme baru didokumentasikan");