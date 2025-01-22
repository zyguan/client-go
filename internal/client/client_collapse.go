// Copyright 2021 TiKV Authors
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

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/client/client_collapse.go
//

// Copyright 2020 PingCAP, Inc.
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

// Package client provides tcp connection to kvserver.
package client

import (
	"context"
	"reflect"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/co"
	"golang.org/x/sync/singleflight"
)

var _ Client = reqCollapse{}

var resolveRegionSf singleflight.Group

type reqCollapse struct {
	Client
}

// NewReqCollapse creates a reqCollapse.
func NewReqCollapse(client Client) Client {
	return &reqCollapse{client}
}
func (r reqCollapse) Close() error {
	if r.Client == nil {
		panic("client should not be nil")
	}
	return r.Client.Close()
}

func (r reqCollapse) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if r.Client == nil {
		panic("client should not be nil")
	}
	if canCollapse, resp, err := r.tryCollapseRequest(ctx, addr, req, timeout); canCollapse {
		return resp, err
	}
	return r.Client.SendRequest(ctx, addr, req, timeout)
}

func (r reqCollapse) AsyncSendRequest(ctx context.Context, addr string, req *tikvrpc.Request) co.Coroutine[co.Result[*tikvrpc.Response]] {
	if r.Client == nil {
		panic("client should not be nil")
	}
	cli, ok := r.Client.(AsyncClient)
	if !ok {
		panic("client should implement AsyncClient interface")
	}
	return func(yield func(co.Result[*tikvrpc.Response], *co.Pending) bool) {
		if req.Type == tikvrpc.CmdResolveLock && len(req.ResolveLock().Keys) == 0 && len(req.ResolveLock().TxnInfos) == 0 {
			// try collapse resolve lock request.
			key := strconv.FormatUint(req.Context.RegionId, 10) + "-" + strconv.FormatUint(req.ResolveLock().StartVersion, 10)
			copyReq := *req
			rsC := resolveRegionSf.DoChan(key, func() (interface{}, error) {
				// singleflight group will call this function in a goroutine, thus use SendRequest instead of AsyncSendRequest.
				return r.Client.SendRequest(context.Background(), addr, &copyReq, ReadTimeoutShort)
			})
			pending := &co.Pending{
				Cases: []reflect.SelectCase{
					{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(rsC)},
					{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
				},
			}
			if !yield(co.Result[*tikvrpc.Response]{}, pending) {
				return
			}
			switch pending.Chosen {
			case 0:
				rs := pending.Recv.Interface().(singleflight.Result)
				if rs.Err != nil {
					yield(co.Result[*tikvrpc.Response]{Err: errors.WithStack(rs.Err)}, nil)
				} else {
					yield(co.Result[*tikvrpc.Response]{Val: rs.Val.(*tikvrpc.Response)}, nil)
				}
			case 1:
				yield(co.Result[*tikvrpc.Response]{Err: errors.WithStack(ctx.Err())}, nil)
			default:
				yield(co.Result[*tikvrpc.Response]{Err: errors.New("unreachable")}, nil)
			}
		} else {
			co.Return(cli.AsyncSendRequest(ctx, addr, req), yield)
		}
	}
}

func (r reqCollapse) tryCollapseRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (canCollapse bool, resp *tikvrpc.Response, err error) {
	switch req.Type {
	case tikvrpc.CmdResolveLock:
		resolveLock := req.ResolveLock()
		if len(resolveLock.Keys) > 0 {
			// can not collapse resolve lock lite
			return
		}
		if len(resolveLock.TxnInfos) > 0 {
			// can not collapse batch resolve locks which is only used by GC worker.
			return
		}
		canCollapse = true
		key := strconv.FormatUint(req.Context.RegionId, 10) + "-" + strconv.FormatUint(resolveLock.StartVersion, 10)
		resp, err = r.collapse(ctx, key, &resolveRegionSf, addr, req, timeout)
		return
	default:
		// now we only support collapse resolve lock.
		return
	}
}

func (r reqCollapse) collapse(ctx context.Context, key string, sf *singleflight.Group,
	addr string, req *tikvrpc.Request, timeout time.Duration) (resp *tikvrpc.Response, err error) {
	// because the request may be used by other goroutines, copy the request to avoid data race.
	copyReq := *req
	rsC := sf.DoChan(key, func() (interface{}, error) {
		return r.Client.SendRequest(context.Background(), addr, &copyReq, ReadTimeoutShort) // use resolveLock timeout.
	})
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		err = errors.WithStack(ctx.Err())
		return
	case <-timer.C:
		err = errors.WithStack(context.DeadlineExceeded)
		return
	case rs := <-rsC:
		if rs.Err != nil {
			err = errors.WithStack(rs.Err)
			return
		}
		resp = rs.Val.(*tikvrpc.Response)
		return
	}
}
