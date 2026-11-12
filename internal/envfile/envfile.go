// Package envfile membaca berkas .env sederhana (KEY=VALUE, # komentar, nilai boleh
// dibungkus kutip). Tanpa mengubah proses lain — hanya dipakai saat startup.
package envfile

import (
	"bufio"
	"errors"
	"io/fs"
	"os"
	"strings"
)

// Load membaca berkas env dan mengembalikan peta nilai. Berkas tidak ada → peta kosong,
// tanpa error (supaya .env bersifat opsional).
func Load(path string) (map[string]string, error) {
	out := make(map[string]string)
	if path == "" {
		return out, nil
	}
	file, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}
		key, value, found := strings.Cut(line, "=")
		if !found {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}
		// komentar di akhir baris (hanya kalau tidak dibungkus kutip)
		if !strings.HasPrefix(strings.TrimSpace(value), "\"") {
			if idx := strings.Index(value, " #"); idx >= 0 {
				value = strings.TrimSpace(value[:idx])
			}
		}
		if key != "" {
			out[key] = value
		}
	}
	return out, scanner.Err()
}

// Get mengembalikan nilai env; kosong → fallback.
func Get(env map[string]string, key, fallback string) string {
	if v, ok := env[key]; ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return fallback
}

// GetBool membaca nilai boolean ("1","true","ya","yes" → true).
func GetBool(env map[string]string, key string, fallback bool) bool {
	v, ok := env[key]
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "ya", "yes", "on":
		return true
	case "0", "false", "tidak", "no", "off":
		return false
	default:
		return fallback
	}
}

// GetInt membaca nilai integer.
func GetInt(env map[string]string, key string, fallback int) int {
	v, ok := env[key]
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	n := 0
	for _, r := range strings.TrimSpace(v) {
		if r < '0' || r > '9' {
			return fallback
		}
		n = n*10 + int(r-'0')
	}
	return n
}
