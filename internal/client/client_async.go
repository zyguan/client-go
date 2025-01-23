package client

import (
	"context"
	"fmt"
	"runtime/trace"
	"sync/atomic"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/client-go/v2/util/async"
	"go.uber.org/zap"
)

type ClientAsync interface {
	Client
	// SendRequestAsync sends a request to the target address asynchronously.
	SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response])
}

func (c *RPCClient) SendRequestAsync(ctx context.Context, addr string, req *tikvrpc.Request, cb async.Callback[*tikvrpc.Response]) {
	var err error

	if atomic.CompareAndSwapUint32(&c.idleNotify, 1, 0) {
		go c.recycleIdleConnArray()
	}

	if config.GetGlobalConfig().TiKVClient.MaxBatchSize == 0 {
		cb.Invoke(nil, errors.New("batch client is disabled"))
		return
	}
	if req.StoreTp != tikvrpc.TiKV {
		cb.Invoke(nil, errors.New("unsupported store type: "+req.StoreTp.Name()))
		return
	}

	batchReq := req.ToBatchCommandsRequest()
	if batchReq == nil {
		cb.Invoke(nil, errors.New("unsupported request type: "+req.Type.String()))
		return
	}

	regionRPC := trace.StartRegion(ctx, req.Type.String())
	spanRPC := opentracing.SpanFromContext(ctx)
	if spanRPC != nil && spanRPC.Tracer() != nil {
		spanRPC = spanRPC.Tracer().StartSpan(fmt.Sprintf("rpcClient.SendRequestAsync, region ID: %d, type: %s", req.RegionId, req.Type), opentracing.ChildOf(spanRPC.Context()))
		ctx = opentracing.ContextWithSpan(ctx, spanRPC)
	}

	useCodec := c.option != nil && c.option.codec != nil
	if useCodec {
		req, err = c.option.codec.EncodeRequest(req)
		if err != nil {
			cb.Invoke(nil, err)
			return
		}
	}
	tikvrpc.AttachContext(req, req.Context)

	// TODO(zyguan): would getConnArray be blocked?
	connArray, err := c.getConnArray(addr, true)
	if err != nil {
		cb.Invoke(nil, err)
		return
	}

	var (
		entry = &batchCommandsEntry{
			ctx:           ctx,
			req:           batchReq,
			cb:            cb,
			forwardedHost: req.ForwardedHost,
			canceled:      0,
			err:           nil,
			pri:           req.GetResourceControlContext().GetOverridePriority(),
			start:         time.Now(),
		}
		stop func() bool
	)

	// defer post actions
	entry.cb.Inject(func(resp *tikvrpc.Response, err error) (*tikvrpc.Response, error) {
		if stop != nil {
			stop()
		}

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

		// resource control
		if stmtExec := ctx.Value(util.ExecDetailsKey); stmtExec != nil {
			execDetails := stmtExec.(*util.ExecDetails)
			atomic.AddInt64(&execDetails.WaitKVRespDuration, int64(elapsed))
			execNetworkCollector := networkCollector{}
			execNetworkCollector.onReq(req, execDetails)
			execNetworkCollector.onResp(req, resp, execDetails)
		}

		// tracing
		if spanRPC != nil {
			if util.TraceExecDetailsEnabled(ctx) {
				if si := buildSpanInfoFromResp(resp); si != nil {
					si.addTo(spanRPC, entry.start)
				}
			}
			spanRPC.Finish()
		}
		regionRPC.End()

		// codec
		if useCodec && err == nil {
			resp, err = c.option.codec.DecodeResponse(req, resp)
		}

		return resp, WrapErrConn(err, connArray)
	})

	stop = context.AfterFunc(ctx, func() {
		logutil.Logger(ctx).Debug("async send request cancelled (context done)", zap.String("to", addr), zap.Error(ctx.Err()))
		entry.error(ctx.Err())
	})

	batchConn := connArray.batchConn
	select {
	case batchConn.batchCommandsCh <- entry:
		// will be fulfilled in batch send/recv loop.
	case <-ctx.Done():
		// will be fulfilled by the after callback of ctx.
	case <-batchConn.closed:
		logutil.Logger(ctx).Debug("async send request cancelled (conn closed)", zap.String("to", addr))
		cb.Invoke(nil, errors.New("batchConn closed"))
	}
}
