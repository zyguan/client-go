package main

import (
	"encoding/hex"
	"flag"
	"strings"
	"sync"
	"time"

	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"
)

var opts struct {
	pd      []string
	from    string
	to      string
	limit   int
	time    time.Duration
	threads int
}

func main() {
	flag.Func("pd", "pd address", func(s string) error {
		opts.pd = strings.Split(s, ",")
		return nil
	})
	flag.DurationVar(&opts.time, "time", time.Minute, "time to run")
	flag.StringVar(&opts.from, "from", "", "start key")
	flag.StringVar(&opts.to, "to", "", "end key")
	flag.IntVar(&opts.limit, "limit", 100, "limit")
	flag.IntVar(&opts.threads, "threads", 1, "number of threads")
	flag.Parse()
	log := logutil.BgLogger()

	if len(opts.pd) == 0 {
		log.Fatal("pd address not set")
	}
	if opts.threads < 1 {
		opts.threads = 1
	}
	if opts.limit < 1 {
		opts.limit = 1
	}
	startKey, err := hex.DecodeString(opts.from)
	if err != nil {
		log.Fatal("invalid start key", zap.String("key", opts.from), zap.Error(err))
	}
	endKey, err := hex.DecodeString(opts.to)
	if err != nil {
		log.Fatal("invalid end key", zap.String("key", opts.to), zap.Error(err))
	}

	kv, err := txnkv.NewClient(opts.pd)
	if err != nil {
		log.Fatal("failed to create client", zap.Error(err))
	}
	defer kv.Close()
	var wg sync.WaitGroup
	for i := 0; i < opts.threads; i++ {
		wg.Add(1)
		go func() {
			ticker := time.NewTicker(time.Second)
			defer func() {
				wg.Done()
				ticker.Stop()
			}()
			done := time.After(opts.time)
			snap := kv.GetSnapshot(oracle.GoTimeToTS(time.Now()))
			for {
				select {
				case <-done:
					return
				case t := <-ticker.C:
					snap = kv.GetSnapshot(oracle.GoTimeToTS(t))
				default:
				}
				iter, err := snap.Iter(startKey, endKey)
				if err != nil {
					log.Error("failed to create iterator", zap.Error(err))
					return
				}
				for i := 1; i < opts.limit && iter.Valid(); i++ {
					err := iter.Next()
					if err != nil {
						log.Warn("failed to scan next", zap.Error(err))
						break
					}
				}
				iter.Close()
			}
		}()
	}
	wg.Wait()
}
