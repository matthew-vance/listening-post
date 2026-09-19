package archiver

import (
	"cmp"
	"fmt"
	"slices"
)

// groupAndWrite writes one file per (event date, station, kafka partition) present in rows. File names carry
// the offset range they hold, so re-processing the same records regenerates identical paths and overwrites them.
func groupAndWrite(dir string, rows []Row) ([]string, error) {
	groups := map[string][]Row{}
	for _, r := range rows {
		k := fmt.Sprintf("%s/p%d", r.partition(), r.KafkaPartition)
		groups[k] = append(groups[k], r)
	}

	var files []string
	for k, g := range groups {
		slices.SortFunc(g, func(a, b Row) int { return cmp.Compare(a.KafkaOffset, b.KafkaOffset) })
		path := fmt.Sprintf("%s-%012d-%012d.parquet", k, g[0].KafkaOffset, g[len(g)-1].KafkaOffset)
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
