-- Skema + policy untuk tabel proxy_pool (dipakai `pan-proxy-scanner` dan panel CFVPN Hub)
--
-- CATATAN (12 Nov 2026): project panel sudah pindah ke project baru `lumbbmhhmkcnxunuryjr`
-- (akun adelia140845@gmail.com, org "Adelia Hub") dan project itu **sudah lengkap** dari
-- `cfvpn-hub/supabase/schema.sql`. File ini disimpan untuk project LAMA
-- `cgflrpjavyotvolnvcnl` (cadangan); menjalankannya di project baru tetap aman (idempoten).
--
-- Jalankan SEKALI di Supabase → SQL Editor. Idempoten: aman diulang.
--
-- Isi:
--   1. Kolom bukti verifikasi (diisi aplikasi `pan-proxy-scanner`, bukan oleh panel):
--        tls_ok   = terbukti bisa tunnel TLS 443 (syarat VLESS/WebSocket ke worker)
--        alt_ok   = terbukti bisa tunnel port non-standar (8443/2053)
--        exit_ip  = alamat keluar sebenarnya (dibaca dari trace Cloudflare)
--      `is_active` saja TIDAK cukup: cek HTTP dangkal bisa lolos di proxy yang tidak
--      sanggup TLS, dan konfigurasi VLESS hasilnya mati di sisi pengguna.
--   2. Policy RLS. Aplikasi dan panel memakai anon key, jadi:
--        - SELECT/INSERT/UPDATE: sudah berjalan (policy di bawah memastikan tetap begitu)
--        - DELETE: belum ada → fitur "Prune baris mati" selalu melaporkan 0 baris terhapus.
--
-- PERINGATAN: policy di bawah memberi anon akses penuh ke tabel ini. Siapa pun yang tahu
-- anon key (kunci itu ikut terkirim ke browser panel) bisa membaca, menulis, dan menghapus
-- baris. Itu sudah menjadi desain sekarang; kalau mau lebih ketat, ganti `to anon` menjadi
-- `to authenticated` dan pindahkan penulisan ke sisi server memakai service key.

-- 1. kolom bukti verifikasi ------------------------------------------------------------
alter table if exists public.proxy_pool
  add column if not exists tls_ok  boolean not null default false;
alter table if exists public.proxy_pool
  add column if not exists alt_ok  boolean not null default false;
alter table if exists public.proxy_pool
  add column if not exists exit_ip text;

-- indeks untuk panel: hanya baca baris aktif yang sudah terbukti TLS
create index if not exists proxy_pool_active_tls_idx
  on public.proxy_pool (is_active, tls_ok, last_checked desc);

-- 2. policy RLS ------------------------------------------------------------------------
alter table if exists public.proxy_pool enable row level security;

drop policy if exists proxy_pool_select_anon on public.proxy_pool;
create policy proxy_pool_select_anon
  on public.proxy_pool for select
  to anon, authenticated
  using (true);

drop policy if exists proxy_pool_insert_anon on public.proxy_pool;
create policy proxy_pool_insert_anon
  on public.proxy_pool for insert
  to anon, authenticated
  with check (true);

drop policy if exists proxy_pool_update_anon on public.proxy_pool;
create policy proxy_pool_update_anon
  on public.proxy_pool for update
  to anon, authenticated
  using (true)
  with check (true);

drop policy if exists proxy_pool_delete_anon on public.proxy_pool;
create policy proxy_pool_delete_anon
  on public.proxy_pool for delete
  to anon, authenticated
  using (true);

-- Setelah dijalankan:
--   1. POST /api/supabase/push?limit=500   (di pan-proxy-scanner) → mulai mengisi tls_ok/alt_ok/exit_ip
--   2. POST /api/supabase/prune?days=1     → baru benar-benar menghapus baris mati
--   3. Panel CFVPN Hub sebaiknya membaca: is_active = true AND tls_ok = true