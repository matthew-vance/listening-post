package processor

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// requireTopics checks what the state loop assumes about the broker, so a mis-declared topic fails Run instead
// of producing to a partition that doesn't exist. Snapshots are written to the same partition number of the
// state topic their decoded message arrived on (see run.go's ManualPartitioner), so the state topic needs at
// least as many partitions as the decoded one; and warm-up rebuilds state by reading the state topic to its
// end, which only stays bounded and complete if the topic is compacted.
func requireTopics(ctx context.Context, client *kgo.Client, decoded, state string) error {
	admin := kadm.NewClient(client)
	topics, err := admin.ListTopics(ctx, decoded, state)
	if err != nil {
		return fmt.Errorf("list topics: %w", err)
	}
	for _, name := range []string{decoded, state} {
		if t, ok := topics[name]; !ok || t.Err != nil {
			return fmt.Errorf("topic %s: not found", name)
		}
	}
	if d, s := len(topics[decoded].Partitions), len(topics[state].Partitions); s < d {
		return fmt.Errorf("topic %s has %d partitions but %s has %d: snapshots go to the same partition number, so it needs at least %d", state, s, decoded, d, d)
	}

	configs, err := admin.DescribeTopicConfigs(ctx, state)
	if err != nil {
		return fmt.Errorf("describe %s: %w", state, err)
	}
	cfg, err := configs.On(state, nil)
	if err != nil {
		return fmt.Errorf("describe %s: %w", state, err)
	}
	for _, c := range cfg.Configs {
		if c.Key == "cleanup.policy" {
			if c.Value != nil && strings.Contains(*c.Value, "compact") {
				return nil
			}
			return fmt.Errorf("topic %s has cleanup.policy=%s, want compact: warm-up reads it as the latest snapshot per aircraft", state, c.MaybeValue())
		}
	}
	return fmt.Errorf("topic %s: cleanup.policy not reported", state)
}
