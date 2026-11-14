// Patch 4: push sadar-edge + yield per sumber.
import fs from "node:fs";
const must = (label, ok) => { if (!ok) { console.error("GAGAL: " + label); process.exit(1); } };
const edit = (path, from, to, label) => {
  const s = fs.readFileSync(path, "utf8");
  must(label, s.includes(from));
  fs.writeFileSync(path, s.replace(from, to));
};
const append = (path, text) => fs.appendFileSync(path, text);

// ============================================================ A. push sadar-edge
edit("internal/supabase/client.go",
  "\tTLSOK  *bool   `json:\"tls_ok,omitempty\"`",
  "\tTunnelOK *bool  `json:\"tunnel_ok,omitempty\"` // verdict edge dari cron panel (hanya dibaca)\n\tTLSOK  *bool   `json:\"tls_ok,omitempty\"`",
  "Row TunnelOK");

// PullEdgeVerdicts: ambil verdict edge yang sudah ada saja (tidak menyentuh logika Pull).
append("internal/supabase/client.go", `

// PullEdgeVerdicts mengambil verdict probe edge (\`tunnel_ok\`) yang sudah tercatat di tabel.
// Dipakai push supaya baris yang TERBUKTI MATI dari edge tidak dinaikkan lagi ke is_active=true
// hanya karena verifikasi lokal menganggapnya hidup — vantage lokal dan edge tidak selalu sama.
func (c *Client) PullEdgeVerdicts(ctx context.Context, limit int) (map[string]bool, error) {
	if !c.Configured() {
		return nil, fmt.Errorf("url atau key Supabase belum diisi")
	}
	if limit <= 0 {
		limit = 5000
	}
	query := url.Values{}
	query.Set("select", "ip,port,tunnel_ok")
	query.Set("tunnel_ok", "not.is.null")
	query.Set("limit", strconv.Itoa(limit))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.restURL("?"+query.Encode()), nil)
	if err != nil {
		return nil, err
	}
	c.authorize(req)
	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pull verdict HTTP %d", resp.StatusCode)
	}
	var rows []Row
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}
	out := make(map[string]bool, len(rows))
	for _, row := range rows {
		if row.TunnelOK == nil {
			continue
		}
		out[row.IP+":"+row.Port] = *row.TunnelOK
	}
	return out, nil
}
`);

// pushToSupabase menerima daftar blocked + mengembalikan jumlah dilewati.
edit("internal/api/server.go",
  "func (s *Server) pushToSupabase(ctx context.Context, client *supabase.Client, cfg settings.Settings, limit int) (int, error) {",
  "func (s *Server) pushToSupabase(ctx context.Context, client *supabase.Client, cfg settings.Settings, limit int, edgeDead map[string]bool) (int, int, error) {",
  "pushToSupabase signature");

edit("internal/api/server.go",
  "\t\tif p.OKCount < minOK {\n\t\t\tskippedYoung++\n\t\t\tcontinue\n\t\t}",
  [
    "\t\tif p.OKCount < minOK {",
    "\t\t\tskippedYoung++",
    "\t\t\tcontinue",
    "\t\t}",
    "\t\t// Verdict edge lebih kuat daripada verifikasi lokal: proxy yang di edge terbukti mati",
    "\t\t// tidak boleh dinaikkan lagi (dulu 17 baris berakhir aktif-tapi-mati dan bolak-balik).",
    "\t\tif edgeDead[p.IP+\":\"+strconv.Itoa(p.Port)] {",
    "\t\t\tskippedEdge++",
    "\t\t\tcontinue",
    "\t\t}",
  ].join("\n"), "skip edge-dead");

edit("internal/api/server.go",
  "\tskippedYoung, skippedDup := 0, 0",
  "\tskippedYoung, skippedDup, skippedEdge := 0, 0, 0",
  "counter edge");

console.log("patch 4 (A) selesai sebagian — sisa dikerjakan patch 5");