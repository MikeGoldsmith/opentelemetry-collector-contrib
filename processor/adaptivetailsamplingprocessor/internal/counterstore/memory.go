// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package counterstore // import "github.com/open-telemetry/opentelemetry-collector-contrib/processor/adaptivetailsamplingprocessor/internal/counterstore"

import (
	"context"
	"maps"
	"sync"
)

// retainedBuckets bounds how many interval buckets Memory keeps per sampler.
// The sync loop reads the bucket it just wrote, so anything older is garbage;
// a small margin tolerates a reader that lags a tick.
const retainedBuckets = 3

// Memory is the in-process Store used when no extension is configured. It
// gives a single instance the exact per-instance behavior (this instance's
// counts are the merged counts) through the same code path the shared
// backends use.
type Memory struct {
	mu sync.Mutex
	// buckets maps samplerID -> bucket index -> merged counts.
	buckets map[string]map[int64]map[string]float64
}

var _ Store = (*Memory)(nil)

// NewMemory returns an empty in-process store.
func NewMemory() *Memory {
	return &Memory{buckets: make(map[string]map[int64]map[string]float64)}
}

// AddCounts implements Store.
func (m *Memory) AddCounts(_ context.Context, samplerID string, bucket int64, counts map[string]float64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	perSampler := m.buckets[samplerID]
	if perSampler == nil {
		perSampler = make(map[int64]map[string]float64)
		m.buckets[samplerID] = perSampler
	}
	merged := perSampler[bucket]
	if merged == nil {
		merged = make(map[string]float64, len(counts))
		perSampler[bucket] = merged
	}
	for k, v := range counts {
		merged[k] += v
	}
	for b := range perSampler {
		if b <= bucket-retainedBuckets {
			delete(perSampler, b)
		}
	}
	return nil
}

// ReadCounts implements Store.
func (m *Memory) ReadCounts(_ context.Context, samplerID string, bucket int64) (map[string]float64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	merged := m.buckets[samplerID][bucket]
	out := make(map[string]float64, len(merged))
	maps.Copy(out, merged)
	return out, nil
}
