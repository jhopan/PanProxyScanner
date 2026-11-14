// Package sources memuat katalog sumber daftar proxy dan parser generiknya.
//
// Semua sumber diperlakukan sama: ambil isi (teks/JSON/HTML), lalu cari pola ip:port.
// Parser generik ini cukup untuk semua format yang dipakai sumber-sumber di bawah, tanpa
// parser khusus per situs. Uji keberadaan/format tiap sumber dengan:
//
//	go run . sources
package sources

import (
	"encoding/json"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// Source adalah satu daftar proxy.
type Source struct {
	Name     string
	URL      string
	Kind     string // txt | json | api | html (keterangan saja)
	Optional bool   // sumber yang sering down: kegagalannya hanya peringatan
}

// All adalah katalog sumber. Urutan tidak berpengaruh terhadap prioritas hasil.
var All = []Source{
	// --- GitHub mentah (paling stabil, sering di-update) ---
	{Name: "proxifly-all", Kind: "json", URL: "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.json"},
	{Name: "proxifly-socks5", Kind: "json", URL: "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.json"},
	{Name: "proxifly-http", Kind: "json", URL: "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.json"},
	{Name: "monosans-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt"},
	{Name: "monosans-socks4", Kind: "txt", URL: "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks4.txt"},
	{Name: "monosans-http", Kind: "txt", URL: "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt"},
	{Name: "thespeedx-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks5.txt"},
	{Name: "thespeedx-socks4", Kind: "txt", URL: "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/socks4.txt"},
	{Name: "thespeedx-http", Kind: "txt", URL: "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt"},
	{Name: "hookzof-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt"},
	{Name: "shiftytr-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks5.txt"},
	{Name: "shiftytr-socks4", Kind: "txt", URL: "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/socks4.txt"},
	{Name: "shiftytr-http", Kind: "txt", URL: "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt"},
	{Name: "shiftytr-https", Kind: "txt", URL: "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/https.txt"},
	{Name: "clarketm", Kind: "txt", URL: "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt"},
	{Name: "jetkai-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks5.txt"},
	{Name: "jetkai-socks4", Kind: "txt", URL: "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-socks4.txt"},
	{Name: "jetkai-http", Kind: "txt", URL: "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-http.txt"},
	{Name: "jetkai-https", Kind: "txt", URL: "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies-https.txt"},
	{Name: "roosterkid-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5_RAW.txt"},
	{Name: "roosterkid-socks4", Kind: "txt", URL: "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS4_RAW.txt"},
	{Name: "roosterkid-https", Kind: "txt", URL: "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt"},
	{Name: "ercin-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/socks5.txt"},
	{Name: "ercin-http", Kind: "txt", URL: "https://raw.githubusercontent.com/ErcinDedeoglu/proxies/main/proxies/http.txt"},
	{Name: "aliilapro-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/socks5.txt"},
	{Name: "aliilapro-http", Kind: "txt", URL: "https://raw.githubusercontent.com/ALIILAPRO/Proxy/main/http.txt"},
	{Name: "murongpig-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/socks5.txt"},
	{Name: "murongpig-http", Kind: "txt", URL: "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt"},
	{Name: "vakhov-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/socks5.txt"},
	{Name: "vakhov-https", Kind: "txt", URL: "https://raw.githubusercontent.com/vakhov/fresh-proxy-list/master/https.txt"},
	{Name: "kangproxy-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/officialputuid/KangProxy/main/socks5/socks5.txt"},
	{Name: "kangproxy-http", Kind: "txt", URL: "https://raw.githubusercontent.com/officialputuid/KangProxy/main/http/http.txt"},
	{Name: "prxchk-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/prxchk/proxy-list/main/socks5.txt"},
	{Name: "prxchk-http", Kind: "txt", URL: "https://raw.githubusercontent.com/prxchk/proxy-list/main/http.txt"},
	{Name: "mmpx12-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/mmpx12/proxy-list/master/socks5.txt"},
	{Name: "mmpx12-http", Kind: "txt", URL: "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt"},
	{Name: "hideip-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/zloi-user/hideip.me/main/socks5.txt"},
	{Name: "hideip-http", Kind: "txt", URL: "https://raw.githubusercontent.com/zloi-user/hideip.me/main/http.txt"},
	{Name: "freeproxies-socks5", Kind: "txt", URL: "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/socks5_proxies.txt"},
	{Name: "freeproxies-http", Kind: "txt", URL: "https://raw.githubusercontent.com/Anonym0usWork1221/Free-Proxies/main/proxy_files/http_proxies.txt"},

	// --- API teks/JSON ---
	{Name: "proxyscrape-v4-socks5", Kind: "api", URL: "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=socks5&format=text"},
	{Name: "proxyscrape-v4-socks4", Kind: "api", URL: "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=socks4&format=text"},
	{Name: "proxyscrape-v4-http", Kind: "api", URL: "https://api.proxyscrape.com/v4/free-proxy-list/get?request=display_proxies&protocol=http&format=text"},
	{Name: "proxy-list-download-socks5", Kind: "api", Optional: true, URL: "https://www.proxy-list.download/api/v1/get?type=socks5"},
	{Name: "proxy-list-download-socks4", Kind: "api", Optional: true, URL: "https://www.proxy-list.download/api/v1/get?type=socks4"},
	{Name: "proxy-list-download-http", Kind: "api", Optional: true, URL: "https://www.proxy-list.download/api/v1/get?type=http"},
	{Name: "geonode", Kind: "api", URL: "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc&protocols=socks5,http"},
	{Name: "spys-me-proxy", Kind: "txt", URL: "https://spys.me/proxy.txt"},
	// --- API per-negara (kandidat langsung berlabel negara) ---------------------------------
	// Sumber ini penting untuk negara prioritas: kandidatnya sudah membawa kode negara, jadi
	// target per negara di halaman Pengaturan bisa memprioritaskan verifikasinya. Sumber umum
	// di atas tidak punya label negara — kandidatnya baru ketahuan negaranya setelah diverifikasi.
	{Name: "geonode-id", Kind: "api", URL: "https://proxylist.geonode.com/api/proxy-list?limit=200&page=1&sort_by=lastChecked&sort_type=desc&country=ID"},
	{Name: "geonode-sg", Kind: "api", URL: "https://proxylist.geonode.com/api/proxy-list?limit=200&page=1&sort_by=lastChecked&sort_type=desc&country=SG"},
	{Name: "geonode-my", Kind: "api", URL: "https://proxylist.geonode.com/api/proxy-list?limit=200&page=1&sort_by=lastChecked&sort_type=desc&country=MY"},
	{Name: "geonode-jp", Kind: "api", URL: "https://proxylist.geonode.com/api/proxy-list?limit=200&page=1&sort_by=lastChecked&sort_type=desc&country=JP"},
	{Name: "geonode-hk", Kind: "api", URL: "https://proxylist.geonode.com/api/proxy-list?limit=200&page=1&sort_by=lastChecked&sort_type=desc&country=HK"},
	{Name: "geonode-kr", Kind: "api", URL: "https://proxylist.geonode.com/api/proxy-list?limit=200&page=1&sort_by=lastChecked&sort_type=desc&country=KR"},

	{Name: "spys-me-socks", Kind: "txt", URL: "https://spys.me/socks.txt"},
	{Name: "proxyspace-socks5", Kind: "txt", URL: "https://proxyspace.pro/socks5.txt"},
	{Name: "proxyspace-http", Kind: "txt", URL: "https://proxyspace.pro/http.txt"},

	// --- Halaman tabel HTML (parser generik tetap bisa) ---
	{Name: "free-proxy-list-net", Kind: "html", URL: "https://free-proxy-list.net/"},
	{Name: "sslproxies-org", Kind: "html", URL: "https://www.sslproxies.org/"},
	{Name: "socks-proxy-net", Kind: "html", URL: "https://www.socks-proxy.net/"},
	{Name: "us-proxy-org", Kind: "html", URL: "https://www.us-proxy.org/"},
}

var (
	ipPortRe   = regexp.MustCompile(`(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})`)
	ipFieldRe  = regexp.MustCompile(`"ip"\s*:\s*"(\d{1,3}(?:\.\d{1,3}){3})"[^{}]{0,400}?"port"\s*:\s*"?(\d{1,5})"?`)
	portFieldR = regexp.MustCompile(`"port"\s*:\s*"?(\d{1,5})"?[^{}]{0,400}?"ip"\s*:\s*"(\d{1,3}(?:\.\d{1,3}){3})"`)
)

// ReservedPrefixes adalah rentang IP yang tidak mungkin jadi proxy publik.
var ReservedPrefixes = []string{
	"10.", "127.", "0.", "169.254.", "192.168.", "100.64.", "100.65.", "100.66.", "100.67.",
	"100.68.", "100.69.", "100.70.", "100.71.", "100.72.", "100.73.", "100.74.", "100.75.",
	"100.76.", "100.77.", "100.78.", "100.79.", "100.80.", "100.81.", "100.82.", "100.83.",
	"100.84.", "100.85.", "100.86.", "100.87.", "100.88.", "100.89.", "100.90.", "100.91.",
	"100.92.", "100.93.", "100.94.", "100.95.", "100.96.", "100.97.", "100.98.", "100.99.",
	"100.100.", "100.101.", "100.102.", "100.103.", "100.104.", "100.105.", "100.106.",
	"100.107.", "100.108.", "100.109.", "100.110.", "100.111.", "100.112.", "100.113.",
	"100.114.", "100.115.", "100.116.", "100.117.", "100.118.", "100.119.", "100.120.",
	"100.121.", "100.122.", "100.123.", "100.124.", "100.125.", "100.126.", "100.127.",
	"172.16.", "172.17.", "172.18.", "172.19.", "172.20.", "172.21.", "172.22.", "172.23.",
	"172.24.", "172.25.", "172.26.", "172.27.", "172.28.", "172.29.", "172.30.", "172.31.",
	"224.", "225.", "226.", "227.", "228.", "229.", "230.", "231.", "232.", "233.", "234.",
	"235.", "236.", "237.", "238.", "239.", "240.", "241.", "242.", "243.", "244.", "245.",
	"246.", "247.", "248.", "249.", "250.", "251.", "252.", "253.", "254.", "255.",
}

// IsPublicIP menolak alamat privat/reserved dan oktet di luar 0-255.
func IsPublicIP(ip string) bool {
	parts := strings.Split(ip, ".")
	if len(parts) != 4 {
		return false
	}
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n > 255 {
			return false
		}
	}
	for _, prefix := range ReservedPrefixes {
		if strings.HasPrefix(ip, prefix) {
			return false
		}
	}
	return true
}

// ExtractCountryHints mengambil pasangan addr -> kode negara dari sumber JSON yang
// menyertakan data lokasi (proxifly menyimpannya di objek "geolocation", geonode di
// field "country" sejajar dengan ip/port). Sumber teks biasa tidak menghasilkan apa pun.
func ExtractCountryHints(text string) map[string]string {
	out := make(map[string]string)
	if strings.Contains(text, `\"`) {
		text = strings.ReplaceAll(text, `\"`, `"`)
	}
	trimmed := strings.TrimSpace(text)
	if !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[") {
		// sebagian API mengembalikan JSON sebagai string yang di-quote
		var inner string
		if err := json.Unmarshal([]byte(trimmed), &inner); err != nil {
			return out
		}
		text = inner
	}
	var root any
	if err := json.Unmarshal([]byte(text), &root); err != nil {
		return out
	}
	walkHints(root, out)
	return out
}

func walkHints(node any, out map[string]string) {
	switch value := node.(type) {
	case map[string]any:
		if addr := addrFromMap(value); addr != "" {
			if cc := countryFromMap(value); cc != "" {
				if _, exists := out[addr]; !exists {
					out[addr] = cc
				}
			}
		}
		for _, child := range value {
			walkHints(child, out)
		}
	case []any:
		for _, child := range value {
			walkHints(child, out)
		}
	}
}

func addrFromMap(m map[string]any) string {
	if raw, ok := m["proxy"].(string); ok {
		if addr := normalizeAddr(raw); addr != "" {
			return addr
		}
	}
	ip := stringField(m, "ip")
	port := stringField(m, "port")
	if ip == "" || port == "" {
		return ""
	}
	return normalizeAddr(ip + ":" + port)
}

// normalizeAddr mengubah "socks5://1.2.3.4:8080" atau "1.2.3.4:8080" menjadi "1.2.3.4:8080".
func normalizeAddr(raw string) string {
	raw = strings.TrimSpace(raw)
	if i := strings.Index(raw, "://"); i >= 0 {
		raw = raw[i+3:]
	}
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return ""
	}
	if _, err := strconv.Atoi(port); err != nil {
		return ""
	}
	if !IsPublicIP(host) {
		return ""
	}
	return host + ":" + port
}

func countryFromMap(m map[string]any) string {
	for _, key := range []string{"country", "country_code", "countryCode", "country_short", "cc"} {
		if cc := normalizeCountry(stringField(m, key)); cc != "" {
			return cc
		}
	}
	if geo, ok := m["geolocation"].(map[string]any); ok {
		for _, key := range []string{"country", "country_code", "countryCode"} {
			if cc := normalizeCountry(stringField(geo, key)); cc != "" {
				return cc
			}
		}
	}
	return ""
}

// normalizeCountry hanya menerima kode 2 huruf (mis. "SG", "us", "Id").
func normalizeCountry(value string) string {
	value = strings.TrimSpace(value)
	if len(value) != 2 {
		return ""
	}
	for _, r := range value {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') {
			return ""
		}
	}
	return strings.ToUpper(value)
}

func stringField(m map[string]any, key string) string {
	switch v := m[key].(type) {
	case string:
		return v
	case float64:
		return strconv.Itoa(int(v))
	case json.Number:
		return v.String()
	default:
		return ""
	}
}

// Extract mengambil semua "ip:port" yang wajar dari isi sumber apa pun.
//
// Menangani tiga bentuk sekaligus: daftar teks biasa (`ip:port` di mana saja, termasuk
// tabel HTML), JSON yang memisah field ip dan port (geonode/proxifly), serta JSON yang
// dibungkus sebagai string sehingga field-nya ter-escape (\"ip\":\"...\").
func Extract(text string) map[string]struct{} {
	out := make(map[string]struct{})
	add := func(ip, portStr string) {
		port, err := strconv.Atoi(portStr)
		if err != nil || port < 1 || port > 65535 {
			return
		}
		if !IsPublicIP(ip) {
			return
		}
		out[ip+":"+strconv.Itoa(port)] = struct{}{}
	}

	for _, m := range ipPortRe.FindAllStringSubmatch(text, -1) {
		add(m[1], m[2])
	}

	normalized := text
	if strings.Contains(text, `\"`) {
		normalized = strings.ReplaceAll(text, `\"`, `"`)
	}
	for _, m := range ipFieldRe.FindAllStringSubmatch(normalized, -1) {
		add(m[1], m[2])
	}
	for _, m := range portFieldR.FindAllStringSubmatch(normalized, -1) {
		add(m[2], m[1])
	}
	return out
}
