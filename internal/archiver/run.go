package archiver

import (
	"context"
	"log/slog"
	"time"

	"github.com/matthew-vance/listening-post/internal/pause"
	"github.com/matthew-vance/listening-post/internal/wire"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Run archives events.raw to Parquet until ctx is cancelled. When gate pauses, the current session first drains
// the topic — ingest is refusing events by then, so the end is fixed — then flushes and commits, and closes the
// client so the consumer group is released. Run then waits for the resume and reconnects at the group's
// committed offset; the backfill moves that offset itself while the archiver is away, so the replay isn't
// re-archived. Draining matters because the backfill truncates the topic next: anything not yet polled would be
// gone from the archive for good.
func Run(ctx context.Context, cfg Config, logger *slog.Logger, gate *pause.Gate) error {
	for {
		changed := gate.Changed() // before Paused, so a transition between the two is never missed
		if gate.Paused() {
			select {
			case <-ctx.Done():
				logger.Info("shut down")
				return nil
			case <-changed:
			}
			continue
		}
		a, err := open(ctx, cfg, logger)
		if err != nil {
			return err
		}
		session, stop := context.WithCancel(ctx)
		go func() {
			select {
			case <-changed:
				a.drain(session)
				stop()
			case <-session.Done():
			}
		}()
		err = a.run(session)
		stop()
		a.client.Close()
		if err != nil || ctx.Err() != nil {
			logger.Info("shut down")
			return err
		}
		logger.Info("paused")
	}
}

// drain blocks until every partition has been consumed to its end, checking once a second. The first check waits
// a tick so a request already past the gateway's 503 check can still land its batch.
func (a *archiver) drain(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
		if a.caughtUp(ctx) {
			return
		}
	}
}

// caughtUp compares this session's position on each partition with its end. UncommittedOffsets only lists
// partitions with something polled since the last commit, so a partition that is fully committed falls back to
// CommittedOffsets, and one with neither (never committed, nothing fetched yet) is read from its start.
func (a *archiver) caughtUp(ctx context.Context) bool {
	adm := kadm.NewClient(a.client)
	start, err := adm.ListStartOffsets(ctx, a.KafkaRaw)
	if err != nil {
		return false
	}
	end, err := adm.ListEndOffsets(ctx, a.KafkaRaw)
	if err != nil {
		return false
	}
	consumed, committed := a.client.UncommittedOffsets()[a.KafkaRaw], a.client.CommittedOffsets()[a.KafkaRaw]
	for p, e := range end[a.KafkaRaw] {
		at := start[a.KafkaRaw][p].Offset
		if o, ok := committed[p]; ok {
			at = o.Offset
		}
		if o, ok := consumed[p]; ok {
			at = o.Offset
		}
		if e.Err != nil || at < e.Offset {
			return false
		}
	}
	return true
}

// open connects to Kafka and pings, failing at startup rather than on the first record.
func open(ctx context.Context, cfg Config, logger *slog.Logger) (*archiver, error) {
	client, err := wire.OpenKafka(ctx, cfg.KafkaBrokers,
		kgo.ConsumerGroup(cfg.ArchiverGroup),
		kgo.ConsumeTopics(cfg.KafkaRaw),
		kgo.DisableAutoCommit(), // offsets advance only once files are on disk
	)
	if err != nil {
		return nil, err
	}
	logger.Info("archiving", "topic", cfg.KafkaRaw, "group", cfg.ArchiverGroup, "dir", cfg.ArchiveDir, "flush_records", cfg.FlushRecords, "flush_seconds", cfg.FlushSeconds)
	return &archiver{client: client, logger: logger, Config: cfg}, nil
}
