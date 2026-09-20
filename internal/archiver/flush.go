package archiver

import (
	"cmp"
	"fmt"
	"slices"
	"time"
)

// Less orders rows by event time, then station, then line: the archive's canonical order, shared with
// cmd/backfill so a replay folds events the way they happened.
func Less(a, b Row) int {
	return cmp.Or(a.TS.Compare(b.TS), cmp.Compare(a.StationID, b.StationID), cmp.Compare(a.Raw, b.Raw))
}

// groupAndWrite writes one file per event date present in rows, named by the time range it holds. The same
// batch regenerates the same path, so a crash before the commit overwrites rather than duplicates; two batches
// whose ranges overlap (a late backlog, a partial re-read) are expected, and readers dedupe on the row.
func groupAndWrite(dir string, rows []Row) ([]string, error) {
	groups := map[string][]Row{}
	for _, r := range rows {
		k := r.partition()
		groups[k] = append(groups[k], r)
	}

	var files []string
	for k, g := range groups {
		slices.SortFunc(g, Less)
		path := fmt.Sprintf("%s/%s-%s.parquet", k, stamp(g[0].TS), stamp(g[len(g)-1].TS))
		data, err := writeParquet(g)
		if err != nil {
			return files, fmt.Errorf("%s: %w", path, err)
		}
		if err := putFile(dir, path, data); err != nil {
			return files, err
		}
		files = append(files, path)
	}
	return files, nil
}

func stamp(t time.Time) string { return t.UTC().Format("20060102T150405.000000Z") }
