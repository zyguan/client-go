// Copyright 2025 TiKV Authors
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

package tikv_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/suite"
	"github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
)

func TestSharedLock(t *testing.T) {
	suite.Run(t, new(testSharedLockSuite))
}

type testSharedLockSuite struct {
	suite.Suite
	store tikv.StoreProbe
}

func (s *testSharedLockSuite) SetupSuite() {
	atomic.StoreUint64(&transaction.ManagedLockTTL, 3000) // 3s
	atomic.StoreUint64(&transaction.CommitMaxBackoff, 1000)
	s.Nil(failpoint.Enable("tikvclient/injectLiveness", `return("reachable")`))
}

func (s *testSharedLockSuite) TearDownSuite() {
	s.Nil(failpoint.Disable("tikvclient/injectLiveness"))
	atomic.StoreUint64(&transaction.CommitMaxBackoff, 20000)
}

func (s *testSharedLockSuite) SetupTest() {
	s.store = tikv.StoreProbe{KVStore: NewTestStore(s.T())}
}

func (s *testSharedLockSuite) TearDownTest() {
	s.store.Close()
}

func (s *testSharedLockSuite) begin() transaction.TxnProbe {
	txn, err := s.store.Begin()
	s.Require().Nil(err)
	txn.SetPessimistic(true)
	return txn
}

func (s *testSharedLockSuite) getTS() uint64 {
	ts, err := s.store.GetOracle().GetTimestamp(context.Background(), &oracle.Option{})
	s.Nil(err)
	return ts
}

func (s *testSharedLockSuite) loadLock(key []byte) *txnlock.Lock {
	locks, err := s.store.ScanLocks(context.Background(), key, append(key, 0), s.getTS())
	s.Nil(err)
	if len(locks) == 0 {
		return nil
	}
	return locks[0]
}

func (s *testSharedLockSuite) TestSharedLockBlockExclusiveLock() {
	for _, commit := range []bool{true, false} {
		txn1 := s.begin()
		txn2 := s.begin()
		txn3 := s.begin()

		pk1 := []byte("pk1")
		pk2 := []byte("pk2")
		pk3 := []byte("pk3")
		key := []byte("shared_lock_key")

		s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk1))
		s.Equal(txn1.GetCommitter().GetPrimaryKey(), pk1)
		lockctx1 := kv.NewLockCtx(s.getTS(), 1000, time.Now())
		lockctx1.InShareMode = true
		s.Nil(txn1.LockKeys(context.Background(), lockctx1, key))

		s.Nil(txn2.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk2))
		s.Equal(txn2.GetCommitter().GetPrimaryKey(), pk2)
		lockctx2 := kv.NewLockCtx(s.getTS(), 1000, time.Now())
		lockctx2.InShareMode = true
		fmt.Println(lockctx2.ForUpdateTS)
		s.Nil(txn2.LockKeys(context.Background(), lockctx2, key))

		flags, err := txn2.GetMemBuffer().GetFlags(key)
		s.Nil(err)
		s.True(flags.HasLockedInShareMode())

		s.Nil(txn3.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk3))
		s.Equal(txn3.GetCommitter().GetPrimaryKey(), pk3)
		lockDone := make(chan time.Time)
		go func() {
			s.NotNil(txn3.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), key)) // should block and return conflict
			lockDone <- time.Now()
		}()

		time.Sleep(500 * time.Millisecond)
		beforeRelease := time.Now()

		if commit {
			s.Nil(txn1.Commit(context.Background()))
			s.Nil(txn2.Commit(context.Background()))
		} else {
			s.Nil(txn1.Rollback())
			s.Nil(txn2.Rollback())
		}

		afterRelease := <-lockDone
		s.True(afterRelease.After(beforeRelease), "txn3(exclusive lock) should block until txn1(shared lock) and txn2(shared lock) commit")
		s.Nil(txn3.Rollback())
	}
}

func (s *testSharedLockSuite) TestExclusiveLockBlockSharedLock() {
	for _, commit := range []bool{true, false} {
		txn1 := s.begin()
		txn2 := s.begin()
		txn3 := s.begin()

		pk1 := []byte("pk1")
		pk2 := []byte("pk2")
		pk3 := []byte("pk3")
		key := []byte("shared_lock_key")

		s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk1))
		s.Equal(txn1.GetCommitter().GetPrimaryKey(), pk1)
		s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), key))

		s.Nil(txn2.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk2))
		s.Equal(txn2.GetCommitter().GetPrimaryKey(), pk2)
		s.Nil(txn3.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk3))
		s.Equal(txn3.GetCommitter().GetPrimaryKey(), pk3)

		txn2LockDone := make(chan time.Time)
		go func() {
			lockctx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
			lockctx.InShareMode = true
			s.NotNil(txn2.LockKeys(context.Background(), lockctx, key)) // should block and return conflict
			txn2LockDone <- time.Now()
		}()
		txn3LockDone := make(chan time.Time)
		go func() {
			lockctx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
			lockctx.InShareMode = true
			s.NotNil(txn3.LockKeys(context.Background(), lockctx, key)) // should block and return conflict
			txn3LockDone <- time.Now()
		}()

		time.Sleep(500 * time.Millisecond)
		beforeRelease := time.Now()

		if commit {
			s.Nil(txn1.Commit(context.Background()))
		} else {
			s.Nil(txn1.Rollback())
		}

		txn2Locked := <-txn2LockDone
		txn3Locked := <-txn3LockDone
		s.True(txn2Locked.After(beforeRelease), "txn2(shared lock) should block until txn1(exclusive lock) commit/rollback")
		s.True(txn3Locked.After(beforeRelease), "txn3(shared lock) should block until txn1(exclusive lock) commit/rollback")
		s.Nil(txn2.Rollback())
		s.Nil(txn3.Rollback())
	}
}

func (s *testSharedLockSuite) TestResolveSharedLock() {
	txn1 := s.begin()

	pk := []byte("shared_lock_pk")
	key := []byte("shared_lock_key")

	_, err := s.store.SplitRegions(context.Background(), [][]byte{pk}, false, nil)
	s.Nil(err)

	s.Nil(txn1.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk))
	s.Equal(pk, txn1.GetCommitter().GetPrimaryKey())
	lockCtx := kv.NewLockCtx(s.getTS(), 1000, time.Now())
	lockCtx.InShareMode = true
	s.Nil(txn1.LockKeys(context.Background(), lockCtx, key))

	s.Nil(failpoint.Enable("tikvclient/beforeCommitSecondaries", `return("skip")`))
	txn1.SetSessionID(1)
	s.Nil(txn1.Commit(context.Background()))

	lock := s.loadLock(key)
	s.NotNil(lock)
	s.Equal(key, lock.Key)
	s.Equal(pk, lock.Primary)

	s.Equal(txn1.StartTS(), lock.TxnID)
	s.True(lock.IsShared())

	txn2 := s.begin()
	s.Nil(txn2.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), pk))
	s.Equal(pk, txn2.GetCommitter().GetPrimaryKey())
	s.Nil(txn2.LockKeys(context.Background(), kv.NewLockCtx(s.getTS(), 1000, time.Now()), key))

	lock = s.loadLock(key)
	s.NotNil(lock)
	s.Equal(key, lock.Key)
	s.Equal(pk, lock.Primary)
	s.Equal(txn2.StartTS(), lock.TxnID)
	s.False(lock.IsShared())

	s.Nil(txn2.Rollback())
	s.Nil(s.loadLock(key))
	s.Nil(failpoint.Disable("tikvclient/beforeCommitSecondaries"))
}
