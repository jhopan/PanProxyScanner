#!/bin/sh
# Installer Pan ProxyScanner untuk Linux, macOS, dan Termux.
#
#   curl -fsSL https://raw.githubusercontent.com/jhopan/PanProxyScanner/main/install.sh | sh
#
# Skrip ini: mendeteksi OS/arsitektur, mengunduh rilis TERBARU dari GitHub, memasang biner,
# lalu (opsional) menjadikannya layanan otomatis (systemd / launchd / autostart Termux).
#
# Bisa dijalankan non-interaktif lewat variabel lingkungan:
#   PORT=8787 SERVICE=yes RUN_NOW=yes NONINTERACTIVE=1 sh install.sh
set -eu

REPO="jhopan/PanProxyScanner"
STABLE_BASE="https://github.com/${REPO}/releases/download/latest"
API_LATEST="https://api.github.com/repos/${REPO}/releases/latest"
SERVICE_NAME="panproxyscanner"

INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/share/panproxyscanner}"
BIN_DIR="$INSTALL_DIR/bin"
BIN="$BIN_DIR/panproxyscanner"
PORT="${PORT:-}"
SERVICE="${SERVICE:-}"
RUN_NOW="${RUN_NOW:-}"
NONINTERACTIVE="${NONINTERACTIVE:-0}"

# ---------------------------------------------------------------- util ---------------
say() { printf '%s\n' "$*"; }
info() { printf '  %s\n' "$*"; }
warn() { printf 'PERINGATAN: %s\n' "$*" >&2; }
die() { printf 'GAGAL: %s\n' "$*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

# Membaca jawaban dari /dev/tty supaya tetap interaktif walau dipanggil lewat `curl | sh`.
ask() { # ask <pertanyaan> <default>
  _prompt="$1"; _def="$2"
  if [ "$NONINTERACTIVE" = "1" ] || [ ! -r /dev/tty ]; then
    printf '%s\n' "$_def"
    return 0
  fi
  printf '%s [%s]: ' "$_prompt" "$_def" >/dev/tty
  _ans=""
  IFS= read -r _ans </dev/tty || _ans=""
  if [ -n "$_ans" ]; then printf '%s\n' "$_ans"; else printf '%s\n' "$_def"; fi
}

ask_port() {
  _def="$1"
  while :; do
    _p="$(ask "Port panel yang dipakai" "$_def")"
    case "$_p" in
      ''|*[!0-9]*) warn "port harus angka"; continue ;;
    esac
    if [ "$_p" -lt 1 ] || [ "$_p" -gt 65535 ]; then warn "port harus 1-65535"; continue; fi
    if port_in_use "$_p"; then
      warn "port $_p sudah dipakai proses lain — pilih port lain"
      [ "$NONINTERACTIVE" = "1" ] && printf '%s\n' "$_p" && return 0
      continue
    fi
    printf '%s\n' "$_p"; return 0
  done
}

# port_in_use: benar kalau ada yang menjawab di 127.0.0.1:<port>.
port_in_use() {
  if have curl; then
    curl -sS -m 2 -o /dev/null "http://127.0.0.1:$1/" >/dev/null 2>&1 && return 0
    return 1
  fi
  return 1
}

detect_os() {
  # Termux: env khas Termux atau struktur folder paketnya. Perhatikan: `A || B && C` di POSIX
  # shell berarti `(A || B) && C`, jadi tiap sinyal harus dipisah sendiri-sendiri.
  if [ -n "${TERMUX_VERSION:-}" ] || [ -n "${TERMUX_PREFIX:-}" ] || [ -d "/data/data/com.termux/files/usr" ]; then
    echo "termux"; return 0
  fi
  case "$(uname -s 2>/dev/null || echo unknown)" in
    Linux*) echo "linux" ;;
    Darwin*) echo "macos" ;;
    MINGW*|MSYS*|CYGWIN*) echo "windows-shell" ;;
    *) echo "unknown" ;;
  esac
}

detect_arch() {
  case "$(uname -m 2>/dev/null || echo unknown)" in
    x86_64|amd64) echo "amd64" ;;
    aarch64|arm64) echo "arm64" ;;
    *) uname -m 2>/dev/null || echo unknown ;;
  esac
}

download() { # download <url> <dest>
  if have curl; then
    curl -fsSL --retry 3 -o "$2" "$1"
  elif have wget; then
    wget -q -O "$2" "$1"
  else
    die "butuh 'curl' atau 'wget' untuk mengunduh"
  fi
}

# Nama berkas stabil (mis. panproxyscanner-linux-amd64.tar.gz); kalau rilis belum punya,
# cari nama berversi lewat API GitHub.
resolve_asset() { # resolve_asset <asset-stabil> <pola-versi>
  _stable="$1"; _pattern="$2"
  _url="$STABLE_BASE/$_stable"
  if have curl; then
    if curl -fsIL -o /dev/null "$_url" 2>/dev/null; then printf '%s\n' "$_url"; return 0; fi
    _json="$(curl -fsSL "$API_LATEST" 2>/dev/null || true)"
  else
    _json="$(wget -qO- "$API_LATEST" 2>/dev/null || true)"
  fi
  if [ -n "$_json" ]; then
    _hit="$(printf '%s\n' "$_json" | tr ',' '\n' | grep -o "https://[^\"]*${_pattern}" | head -1 || true)"
    if [ -n "$_hit" ]; then printf '%s\n' "$_hit"; return 0; fi
  fi
  return 1
}

# ---------------------------------------------------------------- deteksi ------------
OS="$(detect_os)"
ARCH="$(detect_arch)"

say ""
say "Pan ProxyScanner — installer"
say "============================"
info "sistem   : $OS"
info "arsitektur: $ARCH"
info "tujuan   : $INSTALL_DIR"
say ""

if [ "$OS" = "windows-shell" ]; then
  say "Lingkungan ini Windows (Git Bash/MSYS/Cygwin)."
  say "Jalankan installer Windows, bukan skrip ini:"
  say ""
  say "  install.bat"
  say ""
  say "atau unduh manual: $STABLE_BASE/panproxyscanner-windows-amd64.zip"
  exit 0
fi
[ "$OS" = "unknown" ] && die "sistem operasi tidak dikenali"

case "$OS-$ARCH" in
  linux-amd64|termux-amd64)   ASSET="panproxyscanner-linux-amd64.tar.gz";   PATTERN="panproxyscanner-.*-linux-amd64.tar.gz" ;;
  linux-arm64|termux-arm64)   ASSET="panproxyscanner-linux-arm64.tar.gz";   PATTERN="panproxyscanner-.*-linux-arm64.tar.gz" ;;
  macos-amd64)                ASSET="panproxyscanner-macos-amd64.tar.gz";   PATTERN="panproxyscanner-.*-macos-amd64.tar.gz" ;;
  macos-arm64)                ASSET="panproxyscanner-macos-arm64.tar.gz";   PATTERN="panproxyscanner-.*-macos-arm64.tar.gz" ;;
  *) die "belum ada build untuk $OS/$ARCH (tersedia: linux, macos, windows, dan Termux via linux-arm64)" ;;
esac
info "berkas   : $ASSET"
say ""

# ---------------------------------------------------------------- tanya jawab --------
DEFAULT_PORT="8787"
if [ -z "$PORT" ]; then PORT="$(ask_port "$DEFAULT_PORT")"; else info "port (dari lingkungan): $PORT"; fi
if port_in_use "$PORT"; then
  warn "port $PORT sepertinya sudah dipakai proses lain — scanner bisa gagal bind"
fi

DEFAULT_SERVICE="no"
[ "$OS" = "linux" ] && have systemctl && DEFAULT_SERVICE="yes"
[ "$OS" = "macos" ] && have launchctl && DEFAULT_SERVICE="yes"
[ "$OS" = "termux" ] && DEFAULT_SERVICE="yes"
if [ -z "$SERVICE" ]; then SERVICE="$(ask "Jadikan layanan otomatis (start sendiri)? (yes/no)" "$DEFAULT_SERVICE")"; fi
case "$SERVICE" in y|Y|yes|YES|true|1) SERVICE="yes" ;; *) SERVICE="no" ;; esac

if [ -z "$RUN_NOW" ]; then RUN_NOW="$(ask "Jalankan sekarang? (yes/no)" "yes")"; fi
case "$RUN_NOW" in y|Y|yes|YES|true|1) RUN_NOW="yes" ;; *) RUN_NOW="no" ;; esac
say ""

# ---------------------------------------------------------------- unduh & pasang ----
TMP_DIR="$(mktemp -d 2>/dev/null || echo "/tmp/panproxyscanner.$$")"
mkdir -p "$TMP_DIR"
trap 'rm -rf "$TMP_DIR"' EXIT INT TERM

URL="$(resolve_asset "$ASSET" "$PATTERN" || true)"
[ -z "$URL" ] && die "tidak menemukan aset rilis ($ASSET). Cek https://github.com/$REPO/releases"
info "mengunduh $URL"
download "$URL" "$TMP_DIR/$ASSET" || die "unduhan gagal"

mkdir -p "$BIN_DIR"
info "memasang ke $BIN_DIR"
tar -xzf "$TMP_DIR/$ASSET" -C "$BIN_DIR" 2>/dev/null || die "gagal mengekstrak arsip"
[ -f "$BIN" ] || die "biner tidak ditemukan setelah ekstrak: $BIN"
chmod +x "$BIN"

# symlink supaya bisa dipanggil sebagai `panproxyscanner`
mkdir -p "$HOME/.local/bin"
ln -sf "$BIN" "$HOME/.local/bin/$SERVICE_NAME" 2>/dev/null || true
if [ "$OS" = "termux" ] && [ -n "${PREFIX:-}" ] && [ -d "$PREFIX/bin" ]; then
  ln -sf "$BIN" "$PREFIX/bin/$SERVICE_NAME" 2>/dev/null || true
fi

# tulis .env untuk pemakaian manual (port & bind)
printf 'PS_BIND_LAN=true\nPS_PORT=%s\n' "$PORT" > "$INSTALL_DIR/.env"

VERSION_OUT="$("$BIN" version 2>/dev/null || echo "panproxyscanner")"
info "terpasang: $VERSION_OUT"
say ""

# ---------------------------------------------------------------- layanan -----------
service_linux() {
  if ! have systemctl; then
    warn "systemd tidak ada — jalankan manual: $BIN serve -addr 0.0.0.0:$PORT"
    return 0
  fi
  SUDO=""
  [ "$(id -u)" -ne 0 ] && SUDO="sudo"
  if [ -n "$SUDO" ] && ! $SUDO -n true 2>/dev/null; then
    say "Menulis layanan systemd butuh hak admin (sudo)."
  fi
  UNIT="/etc/systemd/system/${SERVICE_NAME}.service"
  $SUDO tee "$UNIT" >/dev/null <<EOF
[Unit]
Description=Pan ProxyScanner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$(id -un)
WorkingDirectory=$INSTALL_DIR
ExecStart=$BIN serve -addr 0.0.0.0:$PORT
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
  $SUDO systemctl daemon-reload
  $SUDO systemctl enable --now "$SERVICE_NAME" >/dev/null 2>&1 || $SUDO systemctl restart "$SERVICE_NAME"
  info "layanan systemd aktif: $UNIT"
  info "status: systemctl status $SERVICE_NAME · log: journalctl -u $SERVICE_NAME -f"
}

service_macos() {
  PLIST="$HOME/Library/LaunchAgents/id.${SERVICE_NAME}.plist"
  mkdir -p "$HOME/Library/LaunchAgents"
  cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>id.${SERVICE_NAME}</string>
  <key>ProgramArguments</key>
  <array>
    <string>$BIN</string>
    <string>serve</string>
    <string>-addr</string>
    <string>0.0.0.0:$PORT</string>
  </array>
  <key>WorkingDirectory</key><string>$INSTALL_DIR</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardOutPath</key><string>$INSTALL_DIR/${SERVICE_NAME}.log</string>
  <key>StandardErrorPath</key><string>$INSTALL_DIR/${SERVICE_NAME}.err.log</string>
</dict>
</plist>
EOF
  launchctl unload "$PLIST" >/dev/null 2>&1 || true
  launchctl load -w "$PLIST" >/dev/null 2>&1 || warn "launchctl load gagal, cek $PLIST"
  info "agen launchd aktif: $PLIST"
  info "log: $INSTALL_DIR/${SERVICE_NAME}.log"
}

service_termux() {
  RC="$HOME/.bashrc"
  touch "$RC"
  if grep -q "PANPROXYSCANNER AUTOSTART" "$RC" 2>/dev/null; then
    info "autostart Termux sudah ada di $RC"
  else
    cat >> "$RC" <<EOF

# --- PANPROXYSCANNER AUTOSTART ---
# Jalankan scanner saat Termux dibuka (hanya kalau belum jalan).
if command -v curl >/dev/null 2>&1 && ! curl -fsS "http://127.0.0.1:$PORT/api/health" >/dev/null 2>&1; then
  ( termux-wake-lock >/dev/null 2>&1; setsid "$BIN" serve -addr 0.0.0.0:$PORT >/dev/null 2>&1 & )
fi
# --- /PANPROXYSCANNER AUTOSTART ---
EOF
    info "autostart ditambahkan ke $RC"
  fi
  have termux-wake-lock && termux-wake-lock >/dev/null 2>&1 || true
  info "catatan: Termux bisa dimatikan Android; aktifkan 'Acquire wakelock' agar tetap hidup."
}

if [ "$SERVICE" = "yes" ]; then
  case "$OS" in
    linux)  service_linux ;;
    macos)  service_macos ;;
    termux) service_termux ;;
  esac
else
  info "layanan otomatis dilewati (SERVICE=no)"
fi

# ---------------------------------------------------------------- jalankan ----------
if [ "$RUN_NOW" = "yes" ]; then
  if have curl && curl -fsS "http://127.0.0.1:$PORT/api/health" >/dev/null 2>&1; then
    info "scanner sudah berjalan di port $PORT"
  else
    if have setsid; then
      ( cd "$INSTALL_DIR" && setsid "$BIN" serve -addr "0.0.0.0:$PORT" >"$INSTALL_DIR/${SERVICE_NAME}.out.log" 2>&1 & )
    else
      ( cd "$INSTALL_DIR" && nohup "$BIN" serve -addr "0.0.0.0:$PORT" >"$INSTALL_DIR/${SERVICE_NAME}.out.log" 2>&1 & )
    fi
    # beri waktu: proses baru perlu membuat database + pengaturan, dan antivirus bisa
    # memperlambat start pertama (sampai ~20 detik pada Windows).
    _i=0
    _ok="no"
    while [ "$_i" -lt 15 ]; do
      sleep 1
      if have curl && curl -fsS "http://127.0.0.1:$PORT/api/health" >/dev/null 2>&1; then _ok="yes"; break; fi
      _i=$((_i + 1))
    done
    if [ "$_ok" = "yes" ]; then
      info "scanner berjalan (log: $INSTALL_DIR/${SERVICE_NAME}.out.log)"
    else
      warn "health check gagal setelah 15 detik — cek log $INSTALL_DIR/${SERVICE_NAME}.out.log"
    fi
  fi
fi

say ""
say "Selesai."
say "  panel   : http://localhost:$PORT (dan http://<ip-lan>:$PORT dari HP)"
say "  biner   : $BIN"
say "  data    : $INSTALL_DIR (database proxies.db + pengaturan ~/.${SERVICE_NAME}/settings.json)"
say "  update  : jalankan ulang installer ini (selalu mengambil rilis terbaru)"
say "  hapus   : rm -rf $INSTALL_DIR ~/.${SERVICE_NAME} ~/.local/bin/$SERVICE_NAME"
case "$SERVICE" in
  yes)
    case "$OS" in
      linux)  say "            sudo systemctl disable --now $SERVICE_NAME && sudo rm /etc/systemd/system/$SERVICE_NAME.service" ;;
      macos)  say "            launchctl unload ~/Library/LaunchAgents/id.$SERVICE_NAME.plist && rm ~/Library/LaunchAgents/id.$SERVICE_NAME.plist" ;;
      termux) say "            hapus blok PANPROXYSCANNER AUTOSTART di ~/.bashrc" ;;
    esac ;;
esac
say ""
say "login panel bawaan: admin / admin (ganti di menu Scheduler → Akun & API)."
say ""
