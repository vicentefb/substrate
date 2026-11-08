// Copyright 2026 The Agent Substrate Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package authz

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

// checkCache holds the embedded OpenFGA server's Check results and tuple
// reads. Invalidate drops every entry at once. The datastore calls it after
// each tuple write, so a Check on this api server never answers from entries
// older than the write; the other api servers age theirs out within
// checkCacheTTL.
type checkCache struct {
	lru        *storage.InMemoryLRUCache[any]
	generation atomic.Uint64
	stopOnce   sync.Once
}

// cachedEntry stamps a value with the generation it was written in. An entry
// from an older generation reads as absent.
type cachedEntry struct {
	generation uint64
	value      any
}

func newCheckCache(limit int64) (*checkCache, error) {
	lru, err := storage.NewInMemoryLRUCache(storage.WithMaxCacheSize[any](limit))
	if err != nil {
		return nil, err
	}
	return &checkCache{lru: lru}, nil
}

func (c *checkCache) Get(key keys.Key) any {
	entry, ok := c.lru.Get(key).(cachedEntry)
	if !ok {
		return nil
	}
	if entry.generation != c.generation.Load() {
		c.lru.Delete(key)
		return nil
	}
	return entry.value
}

func (c *checkCache) Set(key keys.Key, value any, ttl time.Duration) {
	c.lru.Set(key, cachedEntry{generation: c.generation.Load(), value: value}, ttl)
}

func (c *checkCache) Delete(key keys.Key) { c.lru.Delete(key) }

// Stop is called by OpenFGA's shared resources and by Close, whichever comes
// first.
func (c *checkCache) Stop() { c.stopOnce.Do(c.lru.Stop) }

// Invalidate makes every entry written so far read as absent.
func (c *checkCache) Invalidate() { c.generation.Add(1) }
