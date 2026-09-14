package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
)

type fileKey struct {
	partition string // dt=…/station=…
	kafkaPart int32
}

// groupAndWrite writes one file per (event date, station, kafka partition) present in rows. File names carry
// the offset range they hold, so re-processing the same records regenerates identical paths and overwrites them.
func groupAndWrite(ctx context.Context, logger *slog.Logger, store blobStore, rows []row) ([]string, error) {
	groups := map[fileKey][]row{}
	for _, r := range rows {
		k := fileKey{r.partition(), r.KafkaPartition}
		groups[k] = append(groups[k], r)
	}

	var files []string
	for k, g := range groups {
		slices.SortFunc(g, func(a, b row) int { return int(a.KafkaOffset - b.KafkaOffset) })
		path := fmt.Sprintf("%s/p%d-%012d-%012d.parquet", k.partition, k.kafkaPart, g[0].KafkaOffset, g[len(g)-1].KafkaOffset)
		data, err := writeParquet(g)
		if err != nil {
			return files, fmt.Errorf("%s: %w", path, err)
		}
		if err := store.Put(ctx, path, data); err != nil {
			return files, err
		}
		if k.partition == "dt=unknown/station=unknown" {
			logger.Error("archived undecodable records", "file", path, "count", len(g))
		}
		files = append(files, path)
	}
	return files, nil
}
