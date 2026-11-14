// Patch 12: ekstrak refreshStored + topUpCountries, lalu push delta di pipeline/auto-push.
import fs from "node:fs";
const must = (label, ok) => { if (!ok) { console.error("GAGAL: " + label); process.exit(1); } };
const edit = (path, from, to, label) => {
  const s = fs.readFileSync(path, "utf8");
  must(label, s.includes(from));
  fs.writeFileSync(path, s.replace(from, to));
};

// ---------------------------------------------------------------- scraper: ekstrak dua blok
edit("internal/scraper/scraper.go",
  "\t// 4. Segarkan baris yang sudah tersimpan tapi catatannya basi.\n\tif opts.RefreshLimit > 0 && opts.RefreshAfter > 0 {",
  "\t// 4. Segarkan baris yang sudah tersimpan tapi catatannya basi.\n\tif err := refreshStored(ctx, st, counts, countries, opts, sink); err != nil {\n\t\treturn stats, err\n\t}\n\tif false {",
  "ganti blok refresh");

edit("internal/scraper/scraper.go",
  "\t// 5. Pass tambahan untuk negara yang stok TLS-nya belum mencapai target: verifikasi ulang",
  "\tif err := topUpCountries(ctx, st, counts, countries, opts, sink); err != nil {\n\t\treturn stats, err\n\t}\n\n\treturn stats, nil\n}\n\n// refreshStored memverifikasi ulang baris tersimpan yang catatannya basi.\nfunc refreshStored(ctx context.Context, st *store.Store, counts map[string]int, countries map[string]string, opts Options, sink Sink) error {\n\tif opts.RefreshLimit <= 0 || opts.RefreshAfter <= 0 {\n\t\treturn nil\n\t}\n\t// 4. Segarkan baris yang sudah tersimpan tapi catatannya basi.",
  "pindah blok refresh ke fungsi");

console.log("patch 12 langkah 1 selesai (perlu penyesuaian manual berikutnya)");