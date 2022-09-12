// Copyright 2022 Tigris Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package quota

import (
	"context"
	"time"

	"golang.org/x/time/rate"
)

// Do not attempt to wait for the token, when context expires in less than waitDelta.
var waitDelta = 1 * time.Millisecond

// Used if no timeout set in the context.
var maxWait = 5 * time.Second

type LimiterConfig struct {
	Rate      int
	BurstRate int
}

type Limiter struct {
	isWrite bool
	Rate    *rate.Limiter
}

// SetLimit sets new limiter limits values.
func (l *Limiter) SetLimit(ratel int) {
	l.Rate.SetLimit(rate.Limit(ratel))
}

// SetBurst sets new limiter burst values.
func (l *Limiter) SetBurst(ratel int) {
	l.Rate.SetBurst(ratel)
}

// Allow checks if the request with given size can be executed at this moment.
// It returns immediately. If no error returned the request is allowed to proceed.
// In the case of error, it returns specific error indicating, which limiter parameter
// is violated, whether read or write.
func (l *Limiter) Allow(size int) (err error) {
	now := time.Now()

	rt := l.Rate.ReserveN(now, size)

	defer func() {
		if err != nil {
			rt.CancelAt(now)
		}
	}()

	if !rt.OK() || rt.Delay() > 0 {
		if l.isWrite {
			return ErrWriteUnitsExceeded
		}
		return ErrReadUnitsExceeded
	}

	return nil
}

// Wait checks if the request with given size can be executed at this moment.
// It returns immediately if the request allowed to proceed.
// But, in the case of limits violation, unlike Allow, it doesn't return
// immediately, but reserves a token and delays the execution till the moment
// when request can proceed without violating the limits.
// The execution can be delayed upto the timeout set in the context.
func (l *Limiter) Wait(ctx context.Context, size int) (err error) {
	now := time.Now()

	rt := l.Rate.ReserveN(now, size)

	defer func() {
		if err != nil {
			rt.CancelAt(now)
		}
	}()

	d, ok := ctx.Deadline()
	dur := d.Sub(now) - waitDelta
	if !ok {
		dur = maxWait
	}

	if !rt.OK() || dur < rt.DelayFrom(now) {
		if l.isWrite {
			return ErrWriteUnitsExceeded
		}
		return ErrReadUnitsExceeded
	}

	delay := rt.DelayFrom(now)

	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}
