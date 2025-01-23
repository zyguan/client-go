package txnsnapshot

import (
	"context"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/client"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"github.com/tikv/client-go/v2/util/async"
)

const (
	batchGetBase = iota
	batchGetLite
	batchGetAsync

	defaultBatchGetMethod = batchGetBase
)

func (s *KVSnapshot) batchGetSingleRegionLite(bo *retry.Backoffer, batch batchKeys, readTier int, collectF func(k, v []byte)) error {
	cli := s.store.GetTiKVClient()
	s.mu.RLock()
	scope := s.mu.readReplicaScope
	busyThresholdMs := s.mu.busyThreshold.Milliseconds()
	req, err := s.buildBatchGetRequest(batch.keys, busyThresholdMs, readTier)
	if err != nil {
		s.mu.RUnlock()
		return err
	}
	if s.mu.resourceGroupTag == nil && s.mu.resourceGroupTagger != nil {
		s.mu.resourceGroupTagger(req)
	}
	s.mu.RUnlock()
	req.TxnScope = scope
	req.ReadReplicaScope = scope
	timeout := client.ReadTimeoutMedium
	if s.readTimeout > 0 {
		timeout = s.readTimeout
	}
	req.MaxExecutionDurationMs = uint64(timeout.Milliseconds())
	rpcCtx, err := s.store.GetRegionCache().GetTiKVRPCContext(bo, batch.region, kv.ReplicaReadLeader, 0)
	if err != nil {
		return err
	}
	if rpcCtx == nil {
		s.store.GetRegionCache().InvalidateCachedRegion(batch.region)
		return errors.WithStack(tikverr.ErrRegionUnavailable)
	}
	req.Context.ClusterId = rpcCtx.ClusterID
	if err := tikvrpc.SetContextNoAttach(req, rpcCtx.Meta, rpcCtx.Peer); err != nil {
		return err
	}
	resp, err := cli.SendRequest(bo.GetCtx(), rpcCtx.Addr, req, timeout)
	if err != nil {
		return err
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return err
	}
	// handle region error
	if regionErr != nil {
		if regionErr.GetEpochNotMatch() == nil {
			err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return err
			}
		}
		s.store.GetRegionCache().InvalidateCachedRegion(batch.region)
		return errors.Errorf("meet region error: %s", regionErr)
	}
	if resp.Resp == nil {
		return errors.WithStack(tikverr.ErrBodyMissing)
	}
	// handle locks
	var (
		keyLocked bool

		keyErr *kvrpcpb.KeyError
		pairs  []*kvrpcpb.KvPair
	)
	switch v := resp.Resp.(type) {
	case *kvrpcpb.BatchGetResponse:
		keyErr = v.GetError()
		pairs = v.Pairs
	case *kvrpcpb.BufferBatchGetResponse:
		keyErr = v.GetError()
		pairs = v.Pairs
	default:
		return errors.Errorf("unknown response %T", v)
	}
	if keyErr != nil {
		_, err := txnlock.ExtractLockFromKeyErr(keyErr)
		if err != nil {
			return err
		}
		keyLocked = true
	} else {
		for _, pair := range pairs {
			keyErr := pair.GetError()
			if keyErr == nil {
				collectF(pair.GetKey(), pair.GetValue())
				continue
			}
			_, err := txnlock.ExtractLockFromKeyErr(keyErr)
			if err != nil {
				return err
			}
			keyLocked = true
			break
		}
	}
	if keyLocked {
		return errors.New("some keys are locked")
	}
	return nil
}

func (s *KVSnapshot) batchGetSingleRegionAsync(bo *retry.Backoffer, batch batchKeys, readTier int, collectF func(k, v []byte), rl *async.RunLoop, ret chan<- error) {
	cli, ok := s.store.GetTiKVClient().(client.ClientAsync)
	if !ok {
		ret <- errors.New("client does not support async send")
		return
	}
	s.mu.RLock()
	scope := s.mu.readReplicaScope
	busyThresholdMs := s.mu.busyThreshold.Milliseconds()
	req, err := s.buildBatchGetRequest(batch.keys, busyThresholdMs, readTier)
	if err != nil {
		s.mu.RUnlock()
		ret <- err
		return
	}
	if s.mu.resourceGroupTag == nil && s.mu.resourceGroupTagger != nil {
		s.mu.resourceGroupTagger(req)
	}
	s.mu.RUnlock()
	req.TxnScope = scope
	req.ReadReplicaScope = scope
	timeout := client.ReadTimeoutMedium
	if s.readTimeout > 0 {
		timeout = s.readTimeout
	}
	req.MaxExecutionDurationMs = uint64(timeout.Milliseconds())
	rpcCtx, err := s.store.GetRegionCache().GetTiKVRPCContext(bo, batch.region, kv.ReplicaReadLeader, 0)
	if err != nil {
		ret <- err
		return
	}
	if rpcCtx == nil {
		s.store.GetRegionCache().InvalidateCachedRegion(batch.region)
		ret <- errors.WithStack(tikverr.ErrRegionUnavailable)
		return
	}
	req.Context.ClusterId = rpcCtx.ClusterID
	if err := tikvrpc.SetContextNoAttach(req, rpcCtx.Meta, rpcCtx.Peer); err != nil {
		ret <- err
		return
	}
	sendCtx, cancel := context.WithTimeout(bo.GetCtx(), timeout)
	cli.SendRequestAsync(sendCtx, rpcCtx.Addr, req, async.NewCallback(rl, func(resp *tikvrpc.Response, err error) {
		cancel()
		if err != nil {
			ret <- err
			return
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			ret <- err
			return
		}
		// handle region error
		if regionErr != nil {
			if regionErr.GetEpochNotMatch() == nil {
				err = bo.Backoff(retry.BoRegionMiss, errors.New(regionErr.String()))
				if err != nil {
					ret <- err
					return
				}
			}
			s.store.GetRegionCache().InvalidateCachedRegion(batch.region)
			ret <- errors.Errorf("meet region error: %s", regionErr)
			return
		}
		if resp.Resp == nil {
			ret <- errors.WithStack(tikverr.ErrBodyMissing)
			return
		}
		// handle locks
		var (
			keyLocked bool

			keyErr *kvrpcpb.KeyError
			pairs  []*kvrpcpb.KvPair
		)
		switch v := resp.Resp.(type) {
		case *kvrpcpb.BatchGetResponse:
			keyErr = v.GetError()
			pairs = v.Pairs
		case *kvrpcpb.BufferBatchGetResponse:
			keyErr = v.GetError()
			pairs = v.Pairs
		default:
			ret <- errors.Errorf("unknown response %T", v)
			return
		}
		if keyErr != nil {
			_, err := txnlock.ExtractLockFromKeyErr(keyErr)
			if err != nil {
				ret <- err
				return
			}
			keyLocked = true
		} else {
			for _, pair := range pairs {
				keyErr := pair.GetError()
				if keyErr == nil {
					collectF(pair.GetKey(), pair.GetValue())
					continue
				}
				_, err := txnlock.ExtractLockFromKeyErr(keyErr)
				if err != nil {
					ret <- err
					return
				}
				keyLocked = true
				break
			}
		}
		if keyLocked {
			ret <- errors.New("some keys are locked")
			return
		}
		ret <- nil
	}))
}
