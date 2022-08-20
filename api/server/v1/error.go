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

package api

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto" //nolint:staticcheck
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// This file contains helpers for error handling
//
// 1. GRPC interface
// We reuse GRPCs standard payloads to propagate extended error information:
// https://cloud.google.com/apis/design/errors
// Our extended error code is passed in ErrorInfo and automatically unmarshalled
// on the client.
// The flow:
//   * Server uses `api.Errorf({tigris code}, ...)` to report a TigrisError
//   * TigrisError implements `GRPCStatus()` interface, so GRPC code can construct a GRPC error out of it
//   * Client code calls `FromStatusError` to reconstruct TigrisError from GRPC status and it's payloads
//     (Extended code is taken from errdetails.ErrorInfo and retry delay from errdetails.RetryInfo)
//
// 2. HTTP interface
// So as HTTP interface is intended to be inspected by users, we marshal HTTP errors
// in a more human-readable form, less verbose then GRPC errors.
//
// Example HTTP error:
// {
//   "error": {
//      "code": "ALREADY_EXISTS"
//      "message": "database already exists"
//      "retry": {
//         "delay" : "1s"
//      }
//   }
// }
//
// The flow:
//   * Server uses `api.Errorf({tigris code}, ...)` to report a TigrisError
//   * We provide TigrisError.As(*runtime.HTTPStatusError) to be able to override HTTP
//     status code in GRPC gateway if need to
//   * The error is converted to GRPC status by GRPCStatus() interface
//   * We setup (see CustomMarshaller) and provide custom marshaller of
//     the GRPC status (see MarshalStatus) in GRPC gateway
//   * Client parses the error using UnmarshalStatus into TigrisError

// TigrisError is our Tigris HTTP counterpart of grpc status. All the APIs should use this Error to return as a user facing
// error. TigrisError will return grpcStatus to the muxer so that grpc client can see grpcStatus as the final output. For
// HTTP clients see the **MarshalStatus** method where we are returning the response by not the grpc code as that is not
// needed for HTTP clients.
type TigrisError struct {
	// Contains Tigris extended error code.
	// Codes upto Code_UNAUTHENTICATED are identical to GRPC error codes
	Code Code `json:"code,omitempty"`
	// A developer-facing error message.
	Message string `json:"message,omitempty"`

	// Contains extended error information.
	// For example retry information.
	Details []proto.Message `json:"details,omitempty"`
}

// Error to return the underlying error message
func (e *TigrisError) Error() string {
	return e.Message
}

// WithDetails a helper method for adding details to the TigrisError
func (e *TigrisError) WithDetails(details ...proto.Message) *TigrisError {
	e.Details = append(e.Details, details...)
	return e
}

// WithRetry attached retry information to the error
func (e *TigrisError) WithRetry(d time.Duration) *TigrisError {
	if d != 0 {
		e.Details = append(e.Details, &errdetails.RetryInfo{RetryDelay: durationpb.New(d)})
	}
	return e
}

// RetryDelay retrieves retry delay if it's attached to the error
func (e *TigrisError) RetryDelay() time.Duration {
	var dur time.Duration

	for _, d := range e.Details {
		switch t := d.(type) {
		case *errdetails.RetryInfo:
			dur = t.RetryDelay.AsDuration()
		}
	}

	return dur
}

// ToGRPCCode converts Tigris error code to GRPC code
// Extended codes converted to 'Unknown' GRPC code
func ToGRPCCode(code Code) codes.Code {
	if code <= Code_UNAUTHENTICATED {
		return codes.Code(code)
	}

	// Cannot be represented by GRPC codes
	return codes.Unknown
}

// ToTigrisCode converts GRPC code to Tigris code
func ToTigrisCode(code codes.Code) Code {
	return Code(code)
}

// CodeFromString parses Tigris error code from its string representation
func CodeFromString(c string) Code {
	code, ok := Code_value[c]
	if !ok {
		return Code_UNKNOWN
	}

	return Code(code)
}

// CodeToString convert Tigris error code into string representation
func CodeToString(c Code) string {
	r, ok := Code_name[int32(c)]
	if !ok {
		r = Code_name[int32(Code_UNKNOWN)]
	}

	return r
}

func FromHttpCode(httpCode int) Code {
	switch httpCode {
	case 200:
		return Code_OK
	case 499:
		return Code_CANCELLED
	case 400:
		return Code_INVALID_ARGUMENT
	case 504:
		return Code_DEADLINE_EXCEEDED
	case 404:
		return Code_NOT_FOUND
	case 409:
		return Code_CONFLICT
	case 403:
		return Code_PERMISSION_DENIED
	case 401:
		return Code_UNAUTHENTICATED
	case 429:
		return Code_RESOURCE_EXHAUSTED
	case 500:
		return Code_INTERNAL
	case 503:
		return Code_UNAVAILABLE
	default:
		return Code_UNKNOWN
	}
}

// ToHTTPCode converts Tigris' code to HTTP status code
// Used to customize HTTP codes returned by GRPC-gateway
func ToHTTPCode(code Code) int {
	switch code {
	case Code_OK:
		return 200
	case Code_CANCELLED:
		return 499
	case Code_UNKNOWN:
		return 500
	case Code_INVALID_ARGUMENT:
		return 400
	case Code_DEADLINE_EXCEEDED:
		return 504
	case Code_NOT_FOUND:
		return 404
	case Code_ALREADY_EXISTS:
		return 409
	case Code_PERMISSION_DENIED:
		return 403
	case Code_RESOURCE_EXHAUSTED:
		return 429
	case Code_FAILED_PRECONDITION:
		return 412
	case Code_ABORTED:
		return 409
	case Code_OUT_OF_RANGE:
		return 400
	case Code_UNIMPLEMENTED:
		return 501
	case Code_INTERNAL:
		return 500
	case Code_UNAVAILABLE:
		return 503
	case Code_DATA_LOSS:
		return 500
	case Code_UNAUTHENTICATED:
		return 401

	// Extended codes
	case Code_CONFLICT:
		return 409
	case Code_BAD_GATEWAY:
		return 502
	}

	return 500
}

// As is used by runtime.DefaultHTTPErrorHandler to override HTTP status code
func (e *TigrisError) As(i any) bool {
	switch t := i.(type) {
	case **runtime.HTTPStatusError:
		*t = &runtime.HTTPStatusError{HTTPStatus: ToHTTPCode(e.Code), Err: e}
		return true
	}
	return false
}

// GRPCStatus converts the TigrisError and return status.Status. This is used to return grpc status to the grpc clients
func (e *TigrisError) GRPCStatus() *status.Status {
	st, _ := status.New(ToGRPCCode(e.Code), e.Message).
		WithDetails(&errdetails.ErrorInfo{Reason: CodeToString(e.Code)})

	if e.Details != nil {
		st, _ = st.WithDetails(e.Details...)
	}

	return st
}

// MarshalStatus marshal status object
func MarshalStatus(status *spb.Status) ([]byte, error) {
	resp := struct {
		Error ErrorDetails `json:"error"`
	}{}

	resp.Error.Message = status.Message
	// Get standard GRPC code first
	resp.Error.Code = Code_name[int32(ToTigrisCode(codes.Code(status.Code)))]

	// Get extended Tigris code if it's attached to the details
	for _, d := range status.Details {
		var ei errdetails.ErrorInfo
		if d.MessageIs(&ei) {
			err := d.UnmarshalTo(&ei)
			if err != nil {
				return nil, err
			}
			resp.Error.Code = ei.Reason
		}
		var ri errdetails.RetryInfo
		if d.MessageIs(&ri) {
			err := d.UnmarshalTo(&ri)
			if err != nil {
				return nil, err
			}
			resp.Error.Retry = &RetryInfo{
				Delay: int32(ri.RetryDelay.AsDuration().Milliseconds()),
			}
		}
	}

	return json.Marshal(&resp)
}

// FromErrorDetails construct TigrisError from the ErrorDetails,
// which contains extended code, retry information, etc...
func FromErrorDetails(e *ErrorDetails) *TigrisError {
	var te TigrisError

	c, ok := Code_value[e.Code]
	if !ok {
		c = int32(Code_UNKNOWN)
	}

	te.Code = Code(c)
	te.Message = e.Message

	if e.Retry == nil {
		return &te
	}

	return te.WithRetry(time.Duration(e.Retry.Delay) * time.Millisecond)
}

// UnmarshalStatus reconstruct TigrisError from HTTP error JSON body
func UnmarshalStatus(b []byte) *TigrisError {
	resp := struct {
		Error ErrorDetails `json:"error"`
	}{}

	if err := json.Unmarshal(b, &resp); err != nil {
		return &TigrisError{Code: Code_UNKNOWN, Message: err.Error()}
	}

	return FromErrorDetails(&resp.Error)
}

// FromStatusError parses GRPC status from error into TigrisError
func FromStatusError(err error) *TigrisError {
	st := status.Convert(err)
	code := ToTigrisCode(st.Code())

	var details []proto.Message
	for _, v := range st.Details() {
		switch d := v.(type) {
		case *errdetails.ErrorInfo:
			code = CodeFromString(d.Reason)
		case *errdetails.RetryInfo:
			details = append(details, &errdetails.RetryInfo{RetryDelay: d.RetryDelay})
		}
	}

	return &TigrisError{Code: code, Message: st.Message(), Details: details}
}

// Errorf constructs TigrisError
func Errorf(c Code, format string, a ...interface{}) *TigrisError {
	if c == Code_OK {
		return nil
	}

	e := &TigrisError{
		Code:    c,
		Message: fmt.Sprintf(format, a...),
	}

	return e
}
