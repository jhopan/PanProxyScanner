// Package checker memverifikasi proxy dengan bukti end-to-end, bukan sekadar "port terbuka".
//
// Tiga tahap, semuanya lewat tunnel proxy (SOCKS5 dulu, lalu HTTP CONNECT):
//
//  1. HTTP port 80  → GET http://cp.cloudflare.com/cdn-cgi/trace. Proxy hanya dianggap hidup
//     kalau jawabannya benar-benar berasal dari edge Cloudflare: status 200, header
//     `cf-ray`/`Server: cloudflare`, dan isi trace dengan `h=cp.cloudflare.com`.
//     Proxy yang membajak HTTP (portal, 204 palsu, iklan) gugur di sini.
//  2. TLS port 443  → handshake TLS **dengan verifikasi sertifikat** (root CA sistem) lalu GET
//     trace yang sama di dalam tunnel. Ini membuktikan proxy meneruskan byte mentah dua arah
//     tanpa MITM — syarat VLESS/TCP arbitrer, bukan cuma HTTP.
//  3. TLS port 8443/2053 → bukti proxy mau meneruskan port non-standar (VLESS sering di situ).
//
// Dari isi trace diambil `ip=` (exit IP proxy yang sebenarnya) dan `loc=` (negara),
// jadi data negara tidak lagi bergantung pada metadata sumber.
package checker

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

const (
	// Endpoint trace milik Cloudflare: murah, tidak rate-limit, dan isinya membawa bukti
	// identitas edge (h=, colo=, ip=, loc=) yang tidak bisa dipalsukan tanpa MITM.
	traceHost = "cp.cloudflare.com"
	altHost   = "www.cloudflare.com"
	tracePath = "/cdn-cgi/trace"

	httpPort = 80
	tlsPort  = 443

	maxBody   = 8 << 10
	maxHeader = 16 << 10
	maxStatus = 1024
)

// Result adalah hasil satu verifikasi.
type Result struct {
	Alive    bool
	Protocol string // "socks5" | "http"
	MS       int    // latensi satu permintaan yang berhasil (angka yang dibandingkan antar proxy)
	TotalMS  int    // seluruh proses verifikasi (semua tahap + ulangan) — untuk diagnosa
	Detail   string // rangkuman tiap tahap untuk log/UI
	TLS      bool   // tunnel ke 443 dengan TLS terverifikasi (dibutuhkan VLESS)
	TLSNote  string
	AltOK    bool // tunnel ke port non-standar (8443/2053) juga jalan
	AltNote  string
	ExitIP   string // ip= dari trace: alamat keluar proxy yang sebenarnya
	Country  string // loc= dari trace (ISO-3166 alpha-2), kosong kalau tidak diketahui
}

// Options mengatur batas waktu tiap tahap.
type Options struct {
	DialTimeout  time.Duration // koneksi ke proxy
	StageTimeout time.Duration // greeting/handshake proxy
	ProbeTimeout time.Duration // permintaan trace lewat tunnel
	Budget       time.Duration // batas total per proxy (semua tahap)
	ProbeTLS     bool          // uji TLS 443 (verifikasi sertifikat)
	ProbeAlt     bool          // uji port non-standar
	AltPorts     []int         // daftar port non-standar yang diuji
}

// DefaultOptions dipakai kalau Options kosong. Nilai ini hasil pengukuran pada proxy publik
// nyata: proxy sehat sering butuh 7-12 detik untuk satu permintaan penuh (bukan 2 detik),
// jadi timeout ketat justru menandai proxy baik sebagai mati. Batas tetap ada supaya
// proxy mati tidak menahan antrean.
func DefaultOptions() Options {
	return Options{
		DialTimeout:  6 * time.Second,
		StageTimeout: 6 * time.Second,
		ProbeTimeout: 12 * time.Second,
		Budget:       40 * time.Second,
		ProbeTLS:     true,
		ProbeAlt:     true,
		AltPorts:     []int{8443, 2053},
	}
}

func (o Options) withDefaults() Options {
	d := DefaultOptions()
	if o.DialTimeout <= 0 {
		o.DialTimeout = d.DialTimeout
	}
	if o.StageTimeout <= 0 {
		o.StageTimeout = d.StageTimeout
	}
	if o.ProbeTimeout <= 0 {
		o.ProbeTimeout = d.ProbeTimeout
	}
	if o.Budget <= 0 {
		o.Budget = d.Budget
	}
	if len(o.AltPorts) == 0 {
		o.AltPorts = d.AltPorts
	}
	return o
}

// traceInfo adalah isi balasan /cdn-cgi/trace beserta bukti identitas edge-nya.
type traceInfo struct {
	Status   string
	CFRay    string
	ExitIP   string
	Country  string
	Colo     string
	TLSVer   string
	Hostname string
}

func (t traceInfo) summary() string {
	parts := []string{t.Status}
	if t.Hostname != "" {
		parts = append(parts, "h="+t.Hostname)
	}
	if t.ExitIP != "" {
		parts = append(parts, "ip="+t.ExitIP)
	}
	if t.Colo != "" {
		parts = append(parts, "colo="+t.Colo)
	}
	if t.TLSVer != "" {
		parts = append(parts, "tls="+t.TLSVer)
	}
	return strings.Join(parts, " ")
}

// Check memverifikasi satu proxy: port 80 (HTTP) dan/atau 443 (TLS terverifikasi) harus
// menjawab trace Cloudflare yang asli, lalu port non-standar diuji sebagai nilai tambah.
// `addr` berbentuk "ip:port" alamat proxy.
//
// Populasi proxy publik latensinya liar (terukur 0,8-14 detik untuk permintaan yang sama),
// jadi satu kali timeout belum berarti proxy itu mati. Kalau hasilnya masih "mati" atau
// TLS-nya belum terbukti, seluruh tahap diulang sekali lagi; kesalahan protokol/autentikasi
// tidak diulang karena pasti berulang.
func Check(ctx context.Context, addr string, opts Options) Result {
	opts = opts.withDefaults()
	if opts.Budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Budget)
		defer cancel()
	}
	started := time.Now()
	result := runStages(ctx, addr, opts)
	if !result.Alive || (opts.ProbeTLS && !result.TLS) {
		if retry := runStages(ctx, addr, opts); retry.Alive || retry.TLS {
			result = mergeResults(result, retry)
		}
	}
	result.TotalMS = int(time.Since(started).Milliseconds())
	if result.MS == 0 {
		// tidak ada tahap yang berhasil: pakai total supaya tetap ada angka untuk log
		result.MS = result.TotalMS
	}
	return result
}

// mergeResults menggabungkan percobaan pertama dan ulangan: yang menang adalah bukti terbaik,
// bukan percobaan terakhir.
func mergeResults(first, retry Result) Result {
	out := first
	if !out.Alive && retry.Alive {
		out.Alive, out.Protocol = true, retry.Protocol
	}
	if !out.TLS && retry.TLS {
		out.TLS, out.TLSNote = true, retry.TLSNote
	}
	if !out.AltOK && retry.AltOK {
		out.AltOK, out.AltNote = true, retry.AltNote
	}
	if retry.ExitIP != "" {
		out.ExitIP = retry.ExitIP
	}
	if retry.Country != "" {
		out.Country = retry.Country
	}
	if out.MS == 0 {
		out.MS = retry.MS
	}
	if retry.MS > 0 && retry.MS < out.MS {
		// ulangan yang lebih cepat lebih mewakili latensi asli proxy
		out.MS = retry.MS
	}
	out.Detail = first.Detail + " | ulang: " + retry.Detail
	return out
}

// runStages menjalankan seluruh tahap sekali untuk satu proxy.
func runStages(ctx context.Context, addr string, opts Options) Result {
	result := Result{}
	notes := make([]string, 0, 3)
	var best traceInfo

	// Tahap 1: HTTP port 80 — paling murah dan paling sering didukung.
	prefer := ""
	if info, protocol, ms, err := tryTunnel(ctx, addr, httpPort, false, "", opts); err == nil {
		result.Alive, result.Protocol, result.MS = true, protocol, ms
		notes = append(notes, protocol+" 80: "+info.summary())
		best, prefer = info, protocol
	} else {
		notes = append(notes, "80: "+trimErr(err))
	}

	// Tahap 2: TLS 443 dengan verifikasi sertifikat.
	if opts.ProbeTLS {
		info, protocol, ms, err := tryTunnel(ctx, addr, tlsPort, true, prefer, opts)
		if err == nil {
			result.TLS = true
			result.TLSNote = info.summary()
			if !result.Alive {
				// proxy yang hanya bisa 443: latensinya dari tahap ini
				result.Alive, result.Protocol, result.MS = true, protocol, ms
			} else if ms > 0 {
				// Latensi yang dibandingkan antar proxy adalah latensi jalur yang benar-benar dipakai
				// klien VLESS (443 + TLS), bukan jalur 80 yang hanya dipakai verifikasi. Dulu diambil
				// yang tercepat, sehingga proxy yang cepat di 80 tapi lambat di 443 terlihat cepat di
				// panel lalu mengecewakan saat dipakai.
				result.MS = ms
			}
			notes = append(notes, protocol+" 443: "+info.summary())
			best, prefer = info, protocol // data trace dari tunnel TLS lebih dipercaya
		} else {
			result.TLSNote = trimErr(err)
			notes = append(notes, "443: "+result.TLSNote)
		}
	}

	// Tahap 3: port non-standar — hanya berguna kalau TLS 443 sudah lolos.
	if opts.ProbeAlt && result.TLS {
		for _, port := range opts.AltPorts {
			if info, _, _, err := tryTunnel(ctx, addr, port, true, prefer, opts); err == nil {
				result.AltOK = true
				result.AltNote = fmt.Sprintf("port %d ok (%s)", port, info.summary())
				break
			}
		}
		if !result.AltOK {
			result.AltNote = "port non-standar gagal"
		}
	}

	result.Detail = strings.Join(notes, " | ")
	if result.Alive {
		result.ExitIP = best.ExitIP
		result.Country = best.Country
	}
	return result
}

// tryTunnel mencoba SOCKS5 lalu HTTP CONNECT untuk satu port. `prefer` adalah protokol yang
// sudah terbukti jalan pada tahap sebelumnya (dicoba lebih dulu supaya tidak ada percobaan
// mubazir). Kalau handshake-nya sudah berhasil tetapi relay-nya gagal, protokol lain tidak
// dicoba: proxy itu memang tidak meneruskan permintaan, bukan salah protokol.
//
// Nilai `ms` adalah latensi percobaan yang berhasil saja (bukan seluruh proses verifikasi),
// supaya bisa dibandingkan antar proxy dan dipakai mengurutkan yang tercepat.
func tryTunnel(ctx context.Context, addr string, port int, useTLS bool, prefer string, opts Options) (traceInfo, string, int, error) {
	protocols := []string{"socks5", "http"}
	if prefer == "http" {
		protocols = []string{"http", "socks5"}
	}
	var lastErr error
	for _, protocol := range protocols {
		started := time.Now()
		info, handshakeOK, err := tunnelOnce(ctx, addr, protocol, port, useTLS, opts)
		if err == nil {
			return info, protocol, int(time.Since(started).Milliseconds()), nil
		}
		lastErr = err
		if handshakeOK {
			break
		}
	}
	return traceInfo{}, "", 0, lastErr
}

// tunnelOnce membuka satu koneksi lewat proxy, meminta trace, dan memeriksa keaslian balasannya.
// Nilai kedua menandakan handshake proxy sudah berhasil (jadi relay-nya yang bermasalah).
func tunnelOnce(ctx context.Context, addr, protocol string, port int, useTLS bool, opts Options) (traceInfo, bool, error) {
	host := traceHost
	if port != httpPort && port != tlsPort {
		host = altHost
	}

	conn, br, err := dial(ctx, addr, opts.DialTimeout)
	if err != nil {
		return traceInfo{}, false, err
	}
	defer closeRST(conn)
	conn.SetDeadline(time.Now().Add(opts.StageTimeout))

	if err := handshakeTo(protocol, conn, br, host, port); err != nil {
		return traceInfo{}, false, err
	}

	var writer io.Writer = conn
	var reader io.Reader = br
	tlsVer := ""
	if useTLS {
		tlsConn := tls.Client(&bufferedConn{Conn: conn, br: br}, &tls.Config{
			ServerName: host,
			MinVersion: tls.VersionTLS12,
		})
		tlsConn.SetDeadline(time.Now().Add(opts.ProbeTimeout))
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return traceInfo{}, true, fmt.Errorf("tls %s:%d: %s", host, port, trimErr(err))
		}
		writer, reader = tlsConn, tlsConn
		tlsVer = tlsVersionName(tlsConn.ConnectionState().Version)
	}

	if err := conn.SetDeadline(time.Now().Add(opts.ProbeTimeout)); err != nil {
		return traceInfo{}, true, err
	}
	info, err := exchangeTrace(reader, writer, host)
	if err != nil {
		return traceInfo{}, true, err
	}
	info.Status = fmt.Sprintf("%d %s", port, info.Status)
	info.TLSVer = tlsVer
	return info, true, nil
}

// handshakeTo menjalankan handshake proxy ke target bebas.
func handshakeTo(protocol string, conn net.Conn, br *bufio.Reader, host string, port int) error {
	if protocol == "socks5" {
		return socks5HandshakeTo(conn, br, host, port)
	}
	return httpConnectHandshakeTo(conn, br, host, port)
}

// exchangeTrace meminta /cdn-cgi/trace dan memastikan balasannya asli dari edge Cloudflare:
// status 200, ada penanda cf-ray/Server: cloudflare, dan `h=` cocok dengan host yang diminta.
func exchangeTrace(r io.Reader, w io.Writer, host string) (traceInfo, error) {
	request := "GET " + tracePath + " HTTP/1.1\r\n" +
		"Host: " + host + "\r\n" +
		"User-Agent: Mozilla/5.0\r\n" +
		"Accept: */*\r\n" +
		"Connection: close\r\n\r\n"
	if _, err := io.WriteString(w, request); err != nil {
		return traceInfo{}, fmt.Errorf("GET: %w", err)
	}

	// Batasi total byte yang mau dibaca: proxy nakal tidak boleh membuat kita menunggu selamanya.
	br := bufio.NewReader(io.LimitReader(r, maxStatus+maxHeader+maxBody))

	status, err := readStatusLine(br)
	if err != nil {
		return traceInfo{}, err
	}
	if !strings.Contains(status, " 200") {
		return traceInfo{}, fmt.Errorf("bukan respons target: %q", status)
	}

	info := traceInfo{Status: strings.TrimSpace(strings.TrimPrefix(status, "HTTP/1.1"))}
	edgeProof := false
	total := 0
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return traceInfo{}, fmt.Errorf("header: %w", err)
		}
		total += len(line)
		if total > maxHeader {
			return traceInfo{}, fmt.Errorf("header terlalu besar")
		}
		lower := strings.ToLower(line)
		if strings.HasPrefix(lower, "cf-ray:") {
			info.CFRay = strings.TrimSpace(line[len("cf-ray:"):])
			edgeProof = true
		}
		if strings.HasPrefix(lower, "server:") && strings.Contains(lower, "cloudflare") {
			edgeProof = true
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	if !edgeProof {
		return traceInfo{}, fmt.Errorf("bukan edge Cloudflare (tanpa cf-ray)")
	}

	body, _ := io.ReadAll(io.LimitReader(br, maxBody))
	fields := parseTrace(string(body))
	if fields["h"] != host {
		return traceInfo{}, fmt.Errorf("isi trace bukan %s (h=%q)", host, fields["h"])
	}
	info.Hostname = fields["h"]
	info.ExitIP = fields["ip"]
	info.Colo = fields["colo"]
	info.Country = normalizeCountry(fields["loc"])
	return info, nil
}

// parseTrace mengubah isi trace (baris `kunci=nilai`) menjadi map.
func parseTrace(body string) map[string]string {
	fields := make(map[string]string, 16)
	for _, line := range strings.Split(body, "\n") {
		key, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if !ok {
			continue
		}
		fields[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}
	return fields
}

// normalizeCountry menerima loc= dari trace dan hanya meloloskan kode negara 2 huruf.
func normalizeCountry(loc string) string {
	loc = strings.ToUpper(strings.TrimSpace(loc))
	if len(loc) != 2 || loc == "XX" || loc == "T1" {
		return ""
	}
	return loc
}

// tlsVersionName menerjemahkan kode versi TLS ke nama yang enak dibaca.
func tlsVersionName(version uint16) string {
	switch version {
	case tls.VersionTLS13:
		return "TLSv1.3"
	case tls.VersionTLS12:
		return "TLSv1.2"
	case tls.VersionTLS11:
		return "TLSv1.1"
	case tls.VersionTLS10:
		return "TLSv1.0"
	}
	return ""
}

// dial membuka koneksi TCP ke proxy. SetNoDelay penting: handshake SOCKS5/HTTP terdiri dari
// beberapa tulis-baca kecil dan akan lambat kalau menunggu Nagle.
func dial(ctx context.Context, addr string, timeout time.Duration) (net.Conn, *bufio.Reader, error) {
	d := net.Dialer{Timeout: timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetNoDelay(true)
	}
	return conn, bufio.NewReader(conn), nil
}

// bufferedConn menyatukan koneksi dengan byte yang sudah tersangga di bufio.Reader
// (proxy bisa saja mengirim balasan handshake dan data pertama dalam satu paket).
type bufferedConn struct {
	net.Conn
	br *bufio.Reader
}

func (c *bufferedConn) Read(p []byte) (int, error) { return c.br.Read(p) }

// closeRST menutup koneksi dengan RST (SetLinger(0)) supaya soket tidak menyangkut di
// TIME_WAIT. Tanpa ini, memverifikasi puluhan ribu kandidat akan menghabiskan port
// efemeral mesin (pengalaman nyata: connect berikutnya gagal total).
func closeRST(conn net.Conn) {
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetLinger(0)
	}
	conn.Close()
}

func trimErr(err error) string {
	if err == nil {
		return "ok"
	}
	msg := err.Error()
	if len(msg) > 100 {
		msg = msg[:100]
	}
	return msg
}

// socks5HandshakeTo mengirim greeting, CONNECT ke host:port, lalu menelan balasan bind.
func socks5HandshakeTo(conn net.Conn, br *bufio.Reader, host string, port int) error {
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		return fmt.Errorf("greeting: %w", err)
	}
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(br, greeting); err != nil {
		return fmt.Errorf("greeting: %w", err)
	}
	if greeting[0] != 0x05 {
		return fmt.Errorf("bukan SOCKS5")
	}
	if greeting[1] != 0x00 {
		return fmt.Errorf("butuh auth (%d)", greeting[1])
	}

	hostBytes := []byte(host)
	req := make([]byte, 0, 7+len(hostBytes))
	req = append(req, 0x05, 0x01, 0x00, 0x03, byte(len(hostBytes)))
	req = append(req, hostBytes...)
	req = binary.BigEndian.AppendUint16(req, uint16(port))
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	reply := make([]byte, 4)
	if _, err := io.ReadFull(br, reply); err != nil {
		return fmt.Errorf("reply: %w", err)
	}
	if reply[1] != 0x00 {
		return fmt.Errorf("CONNECT ditolak (%d)", reply[1])
	}
	switch reply[3] {
	case 0x01:
		return skip(br, 4+2)
	case 0x04:
		return skip(br, 16+2)
	case 0x03:
		l, err := br.ReadByte()
		if err != nil {
			return fmt.Errorf("bind: %w", err)
		}
		return skip(br, int(l)+2)
	default:
		return fmt.Errorf("ATYP %d", reply[3])
	}
}

// httpConnectHandshakeTo mengirim CONNECT dan menelan header balasan proxy.
func httpConnectHandshakeTo(conn net.Conn, br *bufio.Reader, host string, port int) error {
	target := net.JoinHostPort(host, strconv.Itoa(port))
	request := "CONNECT " + target + " HTTP/1.1\r\nHost: " + target + "\r\nUser-Agent: Mozilla/5.0\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	status, err := readStatusLine(br)
	if err != nil {
		return err
	}
	if !strings.HasPrefix(status, "HTTP/1.0 200") && !strings.HasPrefix(status, "HTTP/1.1 200") {
		return fmt.Errorf("CONNECT ditolak (%s)", status)
	}
	return skipHeader(br)
}

// readStatusLine membaca sampai CRLF pertama (batas 1 KiB).
func readStatusLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("status: %w", err)
	}
	if len(line) > maxStatus {
		return "", fmt.Errorf("status terlalu panjang")
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// skipHeader menelan sisa header sampai baris kosong (batas 16 KiB).
func skipHeader(br *bufio.Reader) error {
	total := 0
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return fmt.Errorf("header: %w", err)
		}
		total += len(line)
		if total > maxHeader {
			return fmt.Errorf("header terlalu besar")
		}
		if line == "\r\n" || line == "\n" {
			return nil
		}
	}
}

func skip(br *bufio.Reader, n int) error {
	if _, err := io.CopyN(io.Discard, br, int64(n)); err != nil {
		return fmt.Errorf("bind: %w", err)
	}
	return nil
}
