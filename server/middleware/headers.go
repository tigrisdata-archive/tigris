// Copyright 2022-2023 Tigris Data, Inc.
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

package middleware

import (
	"context"
	"fmt"
	"time"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	CookieMaxAgeKey    = "Expires"
	ServerTimingHeader = "Server-Timing"
)

func setHeadersToUnary(ctx context.Context, resp any, measurement *metrics.Measurement) error {
	respHeaders := metadata.New(map[string]string{
		ServerTimingHeader: serverTimingHeaderValue(measurement),
	})

	// add cookie header for sticky routing for interactive transactional operations
	if ty, ok := resp.(*api.BeginTransactionResponse); ok {
		expirationTime := time.Now().Add(MaximumTimeout + 2*time.Second)
		respHeaders.Append(api.SetCookie, fmt.Sprintf("%s=%s;%s=%s", api.HeaderTxID, ty.GetTxCtx().GetId(), CookieMaxAgeKey, expirationTime.Format(time.RFC1123)))
	}

	return grpc.SendHeader(ctx, respHeaders)
}

// setHeadersToStream is called by wrapped stream before pushing every message. This method checks if the header is
// already set if not then it sets them.
func setHeadersToStream(w *wrappedStream) error {
	if w.setServerTimingHeader {
		return nil
	}

	if w.measurement != nil {
		if err := w.SetHeader(metadata.New(map[string]string{
			ServerTimingHeader: serverTimingHeaderValue(w.measurement),
		})); err != nil {
			return err
		}
	}
	w.setServerTimingHeader = true
	return nil
}

func serverTimingHeaderValue(measurement *metrics.Measurement) string {
	totalTime := measurement.TimeSinceStart().Milliseconds()
	commitTime := measurement.GetCommitDuration().Milliseconds()
	searchTime := measurement.GetSearchIndexDuration().Milliseconds()
	if commitTime > 0 && searchTime > 0 {
		return fmt.Sprintf(
			"total;dur=%d,commit;dur=%d,search;dur=%d",
			totalTime,
			commitTime,
			searchTime,
		)
	}
	if commitTime > 0 {
		return fmt.Sprintf(
			"total;dur=%d,commit;dur=%d",
			totalTime,
			commitTime,
		)
	}

	return fmt.Sprintf("total;dur=%d", totalTime)
}
