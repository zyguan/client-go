package txnsnapshot

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

func BenchmarkCacheV1(b *testing.B) {
	c := NewPointGetCacheV1(1000, 32)
	rng := rand.NewZipf(rand.New(rand.NewSource(0)), 1.2, 1.2, 100000)
	key := make([]byte, 8)
	for i := 0; i < b.N; i++ {
		key = binary.BigEndian.AppendUint64(key[:0], rng.Uint64())
		ts := uint64(i)
		c.Read(key, ts)
		c.Update(key, key, ts/10, ts)
	}
}

func BenchmarkCacheV2(b *testing.B) {
	c := NewPointGetCacheV2(1000)
	rng := rand.NewZipf(rand.New(rand.NewSource(0)), 1.2, 1.2, 100000)
	key := make([]byte, 8)
	for i := 0; i < b.N; i++ {
		key = binary.BigEndian.AppendUint64(key[:0], rng.Uint64())
		ts := uint64(i)
		c.Read(key, ts)
		c.Update(key, key, ts/10, ts)
	}
}
