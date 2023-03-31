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

package api

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestTigrisError(t *testing.T) {
	// Test api.Errorf
	err := Errorf(Code_NOT_FOUND, "message %d", 1)
	require.Equal(t, &TigrisError{Code: Code_NOT_FOUND, Message: "message 1"}, err)

	// Test errors.As
	var (
		err1 error = err
		ep   *TigrisError
	)

	require.True(t, errors.As(err1, &ep))
	require.Equal(t, &TigrisError{Code: Code_NOT_FOUND, Message: "message 1"}, ep)

	// Test retry delay
	err = err.WithRetry(7 * time.Second)
	require.Equal(t, 7*time.Second, err.RetryDelay())

	st, err1 := status.New(codes.AlreadyExists, "msg2").WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(11 * time.Second)})
	require.NoError(t, err1)

	// GRPC gateway call this to marshall error from GRPC status.
	st1, err1 := MarshalStatus(st.Proto())
	require.NoError(t, err1)
	require.Equal(t, `{"error":{"code":"ALREADY_EXISTS","message":"msg2","retry":{"delay":11000}}}`, string(st1))

	// Test unmarshal status
	// This is used by HTTP client to reconstruct Tigris error from status error in JSON format, produced by
	// custom marshaller calling MarshalStatus.
	err = UnmarshalStatus([]byte(`{"error": {"code": "NOT_FOUND", "message": "msg1", "retry": { "delay": 9 } } }`))
	expErr := (&TigrisError{Code: Code_NOT_FOUND, Message: "msg1"}).WithRetry(9 * time.Millisecond)
	require.Equal(t, expErr, err)

	// This is used by GRPC to convert Tigris error to GRPC status for marshalling.
	st2 := err.GRPCStatus()

	expSt, err1 := status.New(codes.NotFound, "msg1").WithDetails(
		&errdetails.ErrorInfo{Reason: CodeToString(Code_NOT_FOUND)},
		&errdetails.RetryInfo{RetryDelay: durationpb.New(9 * time.Millisecond)},
	)
	require.NoError(t, err1)

	require.Equal(t, expSt, st2)

	// This is used by GRPC client to reconstruct Tigris error from the GRPC status error
	te := FromStatusError(st.Err())
	require.Equal(t, (&TigrisError{Code: Code_ALREADY_EXISTS, Message: "msg2"}).WithRetry(11*time.Second), te)

	// Test that we can substitute standard GRPC gateway codes with our custom HTTP codes.
	te.Code = Code_CONTENT_TOO_LARGE
	he := &runtime.HTTPStatusError{HTTPStatus: 400, Err: fmt.Errorf("some error")}
	require.True(t, te.As(&he))
	require.Equal(t, 413, he.HTTPStatus)
	require.Equal(t, "msg2", he.Error())

	require.Equal(t, (*TigrisError)(nil), Errorf(Code_OK, "err msg"))
}
