package main

import (
	"context"
	"flag"
	"log"

	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
)

var opts struct {
	pd             string
	regionsPerTask int
}

func main() {
	flag.StringVar(&opts.pd, "pd", "", "pd endpoint")
	flag.IntVar(&opts.regionsPerTask, "task-size", 128, "number of regions per task")
	flag.Parse()
	cli, err := txnkv.NewClient([]string{opts.pd})
	if err != nil {
		log.Panic(err)
	}
	rc := cli.KVStore.GetRegionCache()
	key := []byte("")
	for {
		regions, err := rc.BatchLoadRegionsWithKeyRange(rangetask.NewLocateRegionBackoffer(context.TODO()), key, nil, opts.regionsPerTask)
		if err != nil {
			log.Panic(err)
		}
		lastRegion := regions[len(regions)-1]
		rangeEndKey := lastRegion.EndKey()
		regionInfo := lastRegion.VerID()
		log.Printf("range task: [ %q %q ], last region: %s", kv.StrKey(key), kv.StrKey(rangeEndKey), &regionInfo)
		if len(rangeEndKey) == 0 {
			break
		}
		key = rangeEndKey
	}
}
