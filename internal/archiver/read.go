package archiver

import (
	"errors"
	"fmt"
	"path/filepath"
	"slices"

	"github.com/parquet-go/parquet-go"
)

// Days returns the archive's dt=<date> directories in date order, skipping dt=unknown (nothing to replay).
func Days(root string) ([]string, error) {
	days, err := filepath.Glob(filepath.Join(root, "dt=????-??-??"))
	if err != nil {
		return nil, err
	}
	if len(days) == 0 {
		return nil, errors.New("no dt=<date> directories under " + root)
	}
	slices.Sort(days)
	return days, nil
}

// ReadDay loads one day's files, dedupes on the row (files from a partial re-read overlap by design, and a
// station retry can archive an event twice), and sorts into the archive's canonical order. A duplicate shares
// its event's date, so deduping per day is exact, and a day bounds memory.
// ponytail: a whole day in memory; stream per file with a k-way merge if a day outgrows RAM.
func ReadDay(dir string) ([]Row, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
	if err != nil {
		return nil, err
	}
	var rows []Row
	for _, path := range files {
		got, err := parquet.ReadFile[Row](path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		rows = append(rows, got...)
	}
	slices.SortFunc(rows, Less)
	return slices.CompactFunc(rows, func(a, b Row) bool { return Less(a, b) == 0 }), nil
}
