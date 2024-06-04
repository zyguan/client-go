package txnsnapshot

import (
	"bytes"
	"container/list"
	"encoding/json"
	"fmt"
	"hash/maphash"
	"math"
	"os"
	"strconv"
	"sync"
	"unsafe"

	"github.com/google/btree"
	"github.com/prometheus/client_golang/prometheus"
)

type GlobalCacheArgs struct {
	Ver    uint `json:"ver"`
	Cap    uint `json:"cap"`
	Shards uint `json:"shards"`
}

var (
	globalCache        PointGetCache
	globalCacheEnabled bool

	globalCacheUnsafePatchReadTS int64
)

var (
	globalCacheEvent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "tikvclient",
			Name:      "global_cache_event_total",
		}, []string{"type"})
	globalCacheEventHit = globalCacheEvent.WithLabelValues("hit")

	globalCacheOverhead = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tidb",
		Subsystem: "tikvclient",
		Name:      "global_cache_overhead_duration_seconds",
		Buckets:   []float64{0.0005, 0.001, 0.002, 0.004},
	}, []string{"phase"})
	globalCacheOverheadRead   = globalCacheOverhead.WithLabelValues("read")
	globalCacheOverheadUpdate = globalCacheOverhead.WithLabelValues("update")
)

func init() {
	prometheus.MustRegister(globalCacheEvent)
	prometheus.MustRegister(globalCacheOverhead)

	s := os.Getenv("GLOBAL_POINT_GET_CACHE")
	var args GlobalCacheArgs
	if len(s) == 0 {
		return
	} else if len(s) > 0 {
		if err := json.Unmarshal([]byte(s), &args); err != nil {
			fmt.Fprintf(os.Stderr, "init global point get cache failed: %v\n", err)
			return
		} else if args.Cap == 0 {
			return
		}
	}
	if args.Ver == 1 {
		globalCacheEnabled = true
		if args.Shards > 1 && args.Cap/args.Shards > 1 {
			shards := make([]PointGetCache, args.Shards)
			for i := 0; i < int(args.Shards); i++ {
				shards[i] = NewPointGetCacheV1(int(args.Cap/args.Shards), 32)
			}
			globalCache = NewShardedCache(shards)
		} else {
			globalCache = NewPointGetCacheV1(int(args.Cap), 32)
		}
	} else if args.Ver == 2 {
		globalCacheEnabled = true
		if args.Shards > 1 {
			shards := make([]PointGetCache, args.Shards)
			for i := 0; i < int(args.Shards); i++ {
				shards[i] = NewPointGetCacheV2(int(args.Cap / args.Shards))
			}
			globalCache = NewShardedCache(shards)
		} else {
			globalCache = NewPointGetCacheV2(int(args.Cap))
		}
	} else {
		fmt.Fprintf(os.Stderr, "init global point get cache failed: unknown version %d\n", args.Ver)
	}
	fmt.Fprintf(os.Stderr, "init global point get cache: %+v\n", args)

	s = os.Getenv("GLOBAL_POINT_GET_CACHE_UNSAFE_PATCH_READ_TS")
	if len(s) > 0 {
		if offset, err := strconv.Atoi(s); err == nil && offset > 0 {
			globalCacheUnsafePatchReadTS = int64(offset)
			fmt.Fprintf(os.Stderr, "unsafe patch read-ts with %dus\n", offset)
		}
	}
}

type PointGetCache interface {
	Read(key []byte, ts uint64) (val []byte, ok bool)
	Update(key []byte, value []byte, version uint64, readTS uint64)
}

type ShardedCache struct {
	seed   maphash.Seed
	shards []PointGetCache
}

func NewShardedCache(shards []PointGetCache) *ShardedCache {
	return &ShardedCache{seed: maphash.MakeSeed(), shards: shards}
}

func (c *ShardedCache) Read(key []byte, ts uint64) (val []byte, ok bool) {
	idx := maphash.Bytes(c.seed, key) % uint64(len(c.shards))
	return c.shards[int(idx)].Read(key, ts)
}

func (c *ShardedCache) Update(key []byte, value []byte, version uint64, readTS uint64) {
	idx := maphash.Bytes(c.seed, key) % uint64(len(c.shards))
	c.shards[int(idx)].Update(key, value, version, readTS)
}

type entryV1 struct {
	key       []byte
	value     []byte
	version   uint64
	maxReadTS *uint64
	elem      *list.Element
}

type PointGetCacheV1 struct {
	lock  sync.RWMutex
	data  []entryV1
	order *list.List
	index *btree.BTreeG[entryV1]
}

func NewPointGetCacheV1(capacity int, degree int) *PointGetCacheV1 {
	data := make([]entryV1, capacity)
	ts := make([]uint64, capacity)
	for i := 0; i < capacity; i++ {
		data[i].maxReadTS = &ts[i]
	}
	return &PointGetCacheV1{
		data:  data,
		order: list.New(),
		index: btree.NewG(degree, func(a entryV1, b entryV1) bool {
			cmp := bytes.Compare(a.key, b.key)
			return cmp < 0 || (cmp == 0 && a.version < b.version)
		}),
	}
}

func (c *PointGetCacheV1) Read(key []byte, ts uint64) (val []byte, ok bool) {
	if ts == math.MaxUint64 {
		return nil, false
	}
	c.lock.RLock()
	c.index.DescendLessOrEqual(entryV1{key: key, version: ts}, func(e entryV1) bool {
		if ts <= *e.maxReadTS && bytes.Equal(e.key, key) {
			val, ok = e.value, true
		}
		return false
	})
	c.lock.RUnlock()
	return
}

func (c *PointGetCacheV1) Update(key []byte, value []byte, version uint64, readTS uint64) {
	if readTS == math.MaxUint64 {
		return
	}
	c.lock.Lock()
	e, ok := c.index.Get(entryV1{key: key, version: version})
	if ok {
		if *e.maxReadTS < readTS {
			*e.maxReadTS = readTS
		}
		c.order.MoveToFront(e.elem)
		c.lock.Unlock()
		return
	}
	if n := c.order.Len(); n < len(c.data) {
		e := &c.data[n]
		e.key = append(e.key[:0], key...)
		e.value = value
		e.version = version
		*e.maxReadTS = readTS
		e.elem = c.order.PushFront(e)
		c.index.ReplaceOrInsert(*e)
	} else {
		elem := c.order.Back()
		e := elem.Value.(*entryV1)
		c.index.Delete(*e)
		e.key = append(e.key[:0], key...)
		e.value = value
		e.version = version
		*e.maxReadTS = readTS
		c.order.MoveToFront(elem)
		c.index.ReplaceOrInsert(*e)
	}
	c.lock.Unlock()
}

type entryV2 struct {
	key       []byte
	value     []byte
	version   uint64
	maxReadTS uint64
	elem      *list.Element
}

type PointGetCacheV2 struct {
	lock  sync.RWMutex
	data  []entryV2
	order *list.List
	index map[string]*entryV2
}

func NewPointGetCacheV2(capacity int) *PointGetCacheV2 {
	return &PointGetCacheV2{
		data:  make([]entryV2, capacity),
		order: list.New(),
		index: make(map[string]*entryV2, capacity),
	}
}

func (c *PointGetCacheV2) Read(key []byte, ts uint64) (val []byte, ok bool) {
	if ts == math.MaxUint64 {
		return nil, false
	}
	c.lock.RLock()
	e, exists := c.index[unsafe.String(unsafe.SliceData(key), len(key))]
	if exists && e.version <= ts && ts <= e.maxReadTS {
		val, ok = e.value, true
	}
	c.lock.RUnlock()
	return
}

func (c *PointGetCacheV2) Update(key []byte, value []byte, version uint64, readTS uint64) {
	if readTS == math.MaxUint64 {
		return
	}
	c.lock.Lock()
	e, ok := c.index[unsafe.String(unsafe.SliceData(key), len(key))]
	if ok {
		if e.version == version {
			if e.maxReadTS < readTS {
				e.maxReadTS = readTS
			}
		} else if e.version < version {
			e.value = value
			e.version = version
			e.maxReadTS = readTS
		}
		c.order.MoveToFront(e.elem)
		c.lock.Unlock()
		return
	}
	if n := c.order.Len(); n < len(c.data) {
		e := &c.data[n]
		e.key = append(e.key, key...)
		e.value = value
		e.version = version
		e.maxReadTS = readTS
		e.elem = c.order.PushFront(e)
		c.index[unsafe.String(unsafe.SliceData(e.key), len(e.key))] = e
	} else {
		elem := c.order.Back()
		e := elem.Value.(*entryV2)
		delete(c.index, string(e.key))
		e.key = append(e.key[:0], key...)
		e.value = value
		e.version = version
		e.maxReadTS = readTS
		c.order.MoveToFront(elem)
		c.index[unsafe.String(unsafe.SliceData(e.key), len(e.key))] = e
	}
	c.lock.Unlock()
}
