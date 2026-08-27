// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package adaptivetailsamplingprocessor // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/adaptivetailsamplingprocessor"

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/adaptivetailsamplingprocessor/internal/counterstore"
	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/adaptivetailsamplingprocessor/internal/sampler"
)

// runCounterSync drives one throughput sampler's interval loop: every
// adjustment interval it publishes this instance's counts to the counter
// store, reads back the merged totals, and folds them into the rate engine.
// Ticks are aligned to wall-clock interval boundaries so instances sharing a
// store address the same bucket for the same time period and publish at
// roughly the same moment.
func (p *adaptiveTailSamplingProcessor) runCounterSync(ctx context.Context, name string, st *sampler.SharedThroughput, store counterstore.Store) {
	defer p.syncWG.Done()
	interval := st.Interval()
	timer := time.NewTimer(time.Until(nextBoundary(time.Now(), interval)))
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-timer.C:
			p.syncCounters(ctx, now, name, st, store)
			timer.Reset(time.Until(nextBoundary(time.Now(), interval)))
		}
	}
}

// nextBoundary returns the first wall-clock multiple of interval after now.
func nextBoundary(now time.Time, interval time.Duration) time.Time {
	return now.Truncate(interval).Add(interval)
}

// syncCounters runs one tick: publish the counts accumulated over the
// just-completed bucket, read the merged totals for that bucket, and apply
// them. A peer publishing slightly later than this instance reads is missed
// for that bucket; the engines smooth over that skew and it corrects on the
// next tick.
//
// Store failures fail open: the local counts are applied instead, degrading
// to per-instance rates against the full goal (over-sampling across the
// fleet) rather than stalling the sampler. The decide path never touches the
// store, so it is unaffected either way.
func (p *adaptiveTailSamplingProcessor) syncCounters(ctx context.Context, now time.Time, name string, st *sampler.SharedThroughput, store counterstore.Store) {
	interval := st.Interval()
	// The tick fires at (or just after) a bucket boundary; stepping back half
	// an interval indexes the bucket that just completed.
	bucket := now.Add(-interval/2).UnixNano() / interval.Nanoseconds()
	counts := st.SnapshotCounts()
	if len(counts) > 0 {
		if err := store.AddCounts(ctx, name, bucket, counts); err != nil {
			p.recordCounterSyncError(ctx, name, "add", err)
			st.ApplyMergedCounts(counts)
			return
		}
	}
	merged, err := store.ReadCounts(ctx, name, bucket)
	if err != nil {
		p.recordCounterSyncError(ctx, name, "read", err)
		st.ApplyMergedCounts(counts)
		return
	}
	st.ApplyMergedCounts(merged)
}

func (p *adaptiveTailSamplingProcessor) recordCounterSyncError(ctx context.Context, name, op string, err error) {
	p.telemetry.ProcessorAdaptiveTailSamplingCounterSyncErrors.Add(ctx, 1,
		metric.WithAttributes(attribute.String("rule", name), attribute.String("op", op)))
	p.logger.Warn("counter store sync failed; applying this instance's own counts",
		zap.String("rule", name),
		zap.String("op", op),
		zap.Error(err))
}
