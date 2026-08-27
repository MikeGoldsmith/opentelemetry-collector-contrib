// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package sampler // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/adaptivetailsamplingprocessor/internal/sampler"

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// SharedThroughputConfig configures a throughput sampler whose per-key rates
// are computed over externally merged counts (fleet-wide observations), so
// GoalThroughputPerSec is the fleet budget rather than a per-instance one.
type SharedThroughputConfig struct {
	GoalThroughputPerSec float64
	// InitialSamplingRate applies to every key until the first merged
	// interval produces a rate table, and to keys absent from the table.
	InitialSamplingRate int
	AdjustmentInterval  time.Duration
	Weight              float64
}

// SharedThroughput implements Sampler over a rate table computed from merged
// fleet counts. It accumulates this instance's observations per interval; the
// sync loop (owned by the processor) drains them with SnapshotCounts,
// publishes them to the counter store, and applies the merged fleet counts
// back with ApplyMergedCounts. The decide path only touches the local counts
// map and an atomically swapped rate table, never the store.
type SharedThroughput struct {
	goalPerSec  float64
	interval    time.Duration
	initialRate int

	// mu guards counts, the current interval's local accumulation.
	mu     sync.Mutex
	counts map[string]float64

	// rates is the active table, swapped whole by ApplyMergedCounts.
	rates atomic.Pointer[map[string]int]

	// ema is only touched by ApplyMergedCounts; the sync loop serializes it.
	ema *emaState
}

// NewSharedThroughput constructs the sampler. The sync loop wiring happens in
// the processor; the sampler itself has no background work.
func NewSharedThroughput(cfg SharedThroughputConfig) (*SharedThroughput, error) {
	if cfg.GoalThroughputPerSec <= 0 {
		return nil, errors.New("adaptive_throughput (shared) sampler: goal_throughput must be greater than zero")
	}
	interval := cfg.AdjustmentInterval
	if interval == 0 {
		interval = 15 * time.Second
	}
	initialRate := cfg.InitialSamplingRate
	if initialRate < 1 {
		initialRate = 10
	}
	return &SharedThroughput{
		goalPerSec:  cfg.GoalThroughputPerSec,
		interval:    interval,
		initialRate: initialRate,
		counts:      make(map[string]float64),
		ema:         newEMAState(cfg.Weight),
	}, nil
}

// GetSampleRate implements Sampler. It records the observation locally and
// answers from the current merged rate table, falling back to the bootstrap
// rate for keys the table does not cover yet.
func (s *SharedThroughput) GetSampleRate(key string, spanCount int) int {
	if spanCount <= 0 {
		spanCount = 1
	}
	s.mu.Lock()
	s.counts[key] += float64(spanCount)
	s.mu.Unlock()

	if table := s.rates.Load(); table != nil {
		if rate, ok := (*table)[key]; ok {
			return max(rate, 1)
		}
	}
	return s.initialRate
}

// SnapshotCounts returns and resets this instance's accumulated counts for
// the interval that just completed. Called by the sync loop once per tick.
func (s *SharedThroughput) SnapshotCounts() map[string]float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.counts
	s.counts = make(map[string]float64, len(out))
	return out
}

// ApplyMergedCounts folds one completed interval of merged fleet counts into
// the moving averages and swaps in the recomputed rate table. An empty
// interval leaves both untouched. merged is consumed. Only the sync loop may
// call this; it is not safe for concurrent use with itself.
func (s *SharedThroughput) ApplyMergedCounts(merged map[string]float64) {
	s.ema.observe(merged)
	if table := s.ema.rates(s.goalPerSec, s.interval); table != nil {
		s.rates.Store(&table)
	}
}

// Interval returns the adjustment interval the sync loop should tick on.
func (s *SharedThroughput) Interval() time.Duration { return s.interval }

// Start implements Sampler.
func (*SharedThroughput) Start() error { return nil }

// Stop implements Sampler.
func (*SharedThroughput) Stop() error { return nil }
