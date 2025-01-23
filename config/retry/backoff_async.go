package retry

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pkg/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/internal/logutil"
	"github.com/tikv/client-go/v2/util"
	"github.com/tikv/client-go/v2/util/async"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func (b *Backoffer) BackoffAsync(cfg *Config, err error, cb async.Callback[int], optMaxSleepMs ...int) {
	if strings.Contains(err.Error(), tikverr.MismatchClusterID) {
		logutil.Logger(b.ctx).Fatal("critical error", zap.Error(err))
	}
	select {
	case <-b.ctx.Done():
		cb.Invoke(0, errors.WithStack(err))
		return
	default:
	}
	if b.noop {
		cb.Invoke(0, err)
		return
	}
	maxBackoffTimeExceeded := (b.totalSleep - b.excludedSleep) >= b.maxSleep
	maxExcludedTimeExceeded := false
	if maxLimit, ok := isSleepExcluded[cfg.name]; ok {
		maxExcludedTimeExceeded = b.excludedSleep >= maxLimit && b.excludedSleep >= b.maxSleep
	}
	maxTimeExceeded := maxBackoffTimeExceeded || maxExcludedTimeExceeded
	if b.maxSleep > 0 && maxTimeExceeded {
		longestSleepCfg, longestSleepTime := b.longestSleepCfg()
		errMsg := fmt.Sprintf("%s backoffer.maxSleep %dms is exceeded, errors:", cfg.String(), b.maxSleep)
		for i, err := range b.errors {
			// Print only last 3 errors for non-DEBUG log levels.
			if log.GetLevel() == zapcore.DebugLevel || i >= len(b.errors)-3 {
				errMsg += "\n" + err.Error()
			}
		}
		var backoffDetail bytes.Buffer
		totalTimes := 0
		for name, times := range b.backoffTimes {
			totalTimes += times
			if backoffDetail.Len() > 0 {
				backoffDetail.WriteString(", ")
			}
			backoffDetail.WriteString(name)
			backoffDetail.WriteString(":")
			backoffDetail.WriteString(strconv.Itoa(times))
		}
		errMsg += fmt.Sprintf("\ntotal-backoff-times: %v, backoff-detail: %v, maxBackoffTimeExceeded: %v, maxExcludedTimeExceeded: %v",
			totalTimes, backoffDetail.String(), maxBackoffTimeExceeded, maxExcludedTimeExceeded)
		returnedErr := err
		if longestSleepCfg != nil {
			errMsg += fmt.Sprintf("\nlongest sleep type: %s, time: %dms", longestSleepCfg.String(), longestSleepTime)
			returnedErr = longestSleepCfg.err
		}
		logutil.Logger(b.ctx).Warn(errMsg)
		// Use the backoff type that contributes most to the timeout to generate a MySQL error.
		cb.Invoke(0, errors.WithStack(returnedErr))
		return
	}
	b.errors = append(b.errors, errors.Errorf("%s at %s", err.Error(), time.Now().Format(time.RFC3339Nano)))
	b.configs = append(b.configs, cfg)

	// Lazy initialize.
	if b.fn == nil {
		b.fn = make(map[string]backoffFn)
	}
	f, ok := b.fn[cfg.name]
	if !ok {
		f = cfg.createBackoffFn(b.vars)
		b.fn[cfg.name] = f
	}

	cb.Inject(func(realSleep int, err error) (int, error) {
		if cfg.metric != nil {
			(*cfg.metric).Observe(float64(realSleep) / 1000)
		}

		b.totalSleep += realSleep
		if _, ok := isSleepExcluded[cfg.name]; ok {
			b.excludedSleep += realSleep
		}
		if b.backoffSleepMS == nil {
			b.backoffSleepMS = make(map[string]int)
		}
		b.backoffSleepMS[cfg.name] += realSleep
		if b.backoffTimes == nil {
			b.backoffTimes = make(map[string]int)
		}
		b.backoffTimes[cfg.name]++

		stmtExec := b.ctx.Value(util.ExecDetailsKey)
		if stmtExec != nil {
			detail := stmtExec.(*util.ExecDetails)
			atomic.AddInt64(&detail.BackoffDuration, int64(realSleep)*int64(time.Millisecond))
			atomic.AddInt64(&detail.BackoffCount, 1)
		}

		err2 := b.CheckKilled()
		if err2 != nil {
			return realSleep, err2
		}

		var startTs interface{}
		if ts := b.ctx.Value(TxnStartKey); ts != nil {
			startTs = ts
		}
		logutil.Logger(b.ctx).Debug("retry later",
			zap.Error(err),
			zap.Int("totalSleep", b.totalSleep),
			zap.Int("excludedSleep", b.excludedSleep),
			zap.Int("maxSleep", b.maxSleep),
			zap.Stringer("type", cfg),
			zap.Reflect("txnStartTS", startTs))
		return realSleep, nil
	})

	maxSleepMs := -1
	if len(optMaxSleepMs) > 0 {
		maxSleepMs = optMaxSleepMs[0]
	}
	f(b.ctx, maxSleepMs, cb)
}
