package client

import (
	"context"
	"fmt"
	"reflect"
	"runtime/trace"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/client-go/v2/util/co"
	"go.uber.org/zap"
)

type AsyncClient interface {
	Client
	// AsyncSendRequest builds a coroutine for sending the request.
	AsyncSendRequest(ctx context.Context, addr string, req *tikvrpc.Request) co.Coroutine[co.Result[*tikvrpc.Response]]
}

// AsyncSendRequest implements the AsyncClient interface.
func (c *RPCClient) AsyncSendRequest(ctx context.Context, addr string, req *tikvrpc.Request) co.Coroutine[co.Result[*tikvrpc.Response]] {
	if atomic.CompareAndSwapUint32(&c.idleNotify, 1, 0) {
		go c.recycleIdleConnArray()
	}

	return func(yield func(co.Result[*tikvrpc.Response], *co.Pending) bool) {
		var err error

		ok, reason := req.CanBeBatched()
		if !ok {
			yield(co.Result[*tikvrpc.Response]{Err: errors.New(reason)}, nil)
			return
		}

		useCodec := c.option != nil && c.option.codec != nil
		if useCodec {
			req, err = c.option.codec.EncodeRequest(req)
			if err != nil {
				yield(co.Result[*tikvrpc.Response]{Err: err}, nil)
				return
			}
		}
		tikvrpc.AttachContext(req, req.Context)

		connArray, err := c.getConnArray(addr, true)
		if err != nil {
			yield(co.Result[*tikvrpc.Response]{Err: err}, nil)
			return
		}

		resp, err, ok := coSendBatchRequest(ctx, connArray, req, yield)

		if !ok {
			return
		}
		if err != nil {
			yield(co.Result[*tikvrpc.Response]{Err: WrapErrConn(err, connArray)}, nil)
			return
		}
		if useCodec {
			resp, err = c.option.codec.DecodeResponse(req, resp)
		}
		yield(co.Result[*tikvrpc.Response]{Val: resp, Err: err}, nil)
	}
}

func coSendBatchRequest(
	ctx context.Context,
	connArray *connArray,
	req *tikvrpc.Request,
	yield func(co.Result[*tikvrpc.Response], *co.Pending) bool,
) (resp *tikvrpc.Response, err error, ok bool) {

	region := trace.StartRegion(ctx, req.Type.String())
	span := opentracing.SpanFromContext(ctx)
	if span != nil && span.Tracer() != nil {
		span = span.Tracer().StartSpan(fmt.Sprintf("rpcClient.SendRequest, region ID: %d, type: %s", req.RegionId, req.Type), opentracing.ChildOf(span.Context()))
		ctx = opentracing.ContextWithSpan(ctx, span)
	} else {
		span = nil
	}

	batchConn := connArray.batchConn
	entry := &batchCommandsEntry{
		ctx:           ctx,
		req:           req.ToBatchCommandsRequest(), // not nil, checked by the caller.
		res:           make(chan *tikvpb.BatchCommandsResponse_Response, 1),
		forwardedHost: req.ForwardedHost,
		canceled:      0,
		err:           nil,
		pri:           req.GetResourceControlContext().GetOverridePriority(),
		start:         time.Now(),
	}

	defer func() {
		elapsed := time.Since(entry.start)

		// batch client metrics
		if sendLat := atomic.LoadInt64(&entry.sendLat); sendLat > 0 {
			metrics.BatchRequestDurationSend.Observe(time.Duration(sendLat).Seconds())
		}
		if recvLat := atomic.LoadInt64(&entry.recvLat); recvLat > 0 {
			metrics.BatchRequestDurationRecv.Observe(time.Duration(recvLat).Seconds())
		}
		metrics.BatchRequestDurationDone.Observe(elapsed.Seconds())

		// rpc metrics
		connArray.updateRPCMetrics(req, resp, elapsed)

		// resouce control
		if stmtExec := ctx.Value(util.ExecDetailsKey); stmtExec != nil {
			execDetails := stmtExec.(*util.ExecDetails)
			atomic.AddInt64(&execDetails.WaitKVRespDuration, int64(elapsed))
			execNetworkCollector := networkCollector{}
			execNetworkCollector.onReq(req, execDetails)
			execNetworkCollector.onResp(req, resp, execDetails)
		}

		// tracing
		if span != nil && util.TraceExecDetailsEnabled(ctx) {
			if si := buildSpanInfoFromResp(resp); si != nil {
				si.addTo(span, entry.start)
			}
		}
		region.End()
	}()

	// try `select` directly to avoid yielding.
	enqueued := false
	select {
	case batchConn.batchCommandsCh <- entry:
		enqueued = true
	default:
	}
	if !enqueued {
		// queue is full, yield to avoid blocking other coroutines.
		pending := &co.Pending{
			Cases: []reflect.SelectCase{
				{Dir: reflect.SelectSend, Chan: reflect.ValueOf(batchConn.batchCommandsCh), Send: reflect.ValueOf(entry)},
				{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
				{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(batchConn.closed)},
			},
		}
		if !yield(co.Result[*tikvrpc.Response]{}, pending) {
			logutil.Logger(ctx).Warn("AsyncSendRequest terminated on enqueuing request")
			return nil, nil, false
		}
		switch pending.Chosen {
		case 0:
		case 1:
			logutil.Logger(ctx).Debug("send request aborted (context concelled)",
				zap.String("to", connArray.target), zap.String("cause", ctx.Err().Error()))
			return nil, errors.WithStack(ctx.Err()), true
		case 2:
			logutil.Logger(ctx).Debug("send request aborted (connection closed)",
				zap.String("to", connArray.target))
			return nil, errors.New("batchConn closed"), true
		default:
			return nil, errors.New("unreachable"), true
		}
	}

	pending := &co.Pending{
		Cases: []reflect.SelectCase{
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(entry.res)},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())},
			{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(batchConn.closed)},
		},
	}
	if !yield(co.Result[*tikvrpc.Response]{}, pending) {
		logutil.Logger(ctx).Warn("AsyncSendRequest terminated on waiting response")
		return nil, nil, false
	}
	switch pending.Chosen {
	case 0:
		if !pending.RecvOK {
			return nil, errors.WithStack(entry.err), true
		}
		res := pending.Recv.Interface().(*tikvpb.BatchCommandsResponse_Response)
		resp, err = tikvrpc.FromBatchCommandsResponse(res)
		return resp, err, true
	case 1:
		atomic.StoreInt32(&entry.canceled, 1)
		logutil.Logger(ctx).Debug("send request aborted (context concelled)",
			zap.String("to", connArray.target), zap.String("cause", ctx.Err().Error()))
		return nil, errors.WithStack(ctx.Err()), true
	case 2:
		atomic.StoreInt32(&entry.canceled, 1)
		logutil.Logger(ctx).Debug("send request aborted (connection closed)",
			zap.String("to", connArray.target))
		return nil, errors.New("batchConn closed"), true
	default:
		return nil, errors.New("unreachable"), true
	}
}
