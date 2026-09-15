package archiver

import (
	"fmt"
	"os"
	"path/filepath"
)

// putFile writes atomically: readers (DuckDB, notebooks) never see a partial file.
func putFile(root, path string, data []byte) error {
	full := filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	tmp := full + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, full); err != nil {
		return fmt.Errorf("rename %s: %w", tmp, err)
	}
	return nil
}
