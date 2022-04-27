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

	"github.com/golang/protobuf/ptypes/any"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// TigrisError is our Tigris HTTP counterpart of grpc status. All the APIs should use this Error to return as a user facing
// error. TigrisError will return grpcStatus to the muxer so that grpc client can see grpcStatus as the final output. For
// HTTP clients see the **MarshalStatus** method where we are returning the response by not the grpc code as that is not
// needed for HTTP clients.
type TigrisError struct {
	// The status code, which should be an enum value of [google.rpc.Code][google.rpc.Code]. We don't need to marshal
	// this code for HTTP clients.
	Code codes.Code `json:"code,omitempty"`
	// A developer-facing error message.
	Message string `json:"message,omitempty"`
	// A list of messages that carry the error details. This is mainly to send our internal codes and messages.
	Details []*ErrorDetails `json:"details,omitempty"`
}

// Error to return the underlying error message
func (e *TigrisError) Error() string {
	return e.Message
}

// GRPCStatus converts the TigrisError and return status.Status. This is used to return grpc status to the grpc clients
func (e *TigrisError) GRPCStatus() *status.Status {
	s := &spb.Status{
		Code:    int32(e.Code),
		Message: e.Message,
	}

	if len(e.Details) == 0 {
		return status.FromProto(s)
	}

	var details []*any.Any
	for _, d := range e.Details {
		var a = &any.Any{}
		err := anypb.MarshalFrom(a, d, proto.MarshalOptions{})
		if err == nil {
			details = append(details, a)
		}
	}

	s.Details = details
	return status.FromProto(s)
}

// WithDetails a helper method for adding details to the TigrisError
func (e *TigrisError) WithDetails(details ...*ErrorDetails) *TigrisError {
	e.Details = details
	return e
}

// MarshalStatus marshal status object
func MarshalStatus(status *spb.Status) ([]byte, error) {
	var resp = &TigrisError{}
	resp.Message = status.Message
	resp.Code = codes.Code(status.Code)
	if len(status.Details) > 0 {
		var internalDetails []*ErrorDetails
		for _, d := range status.Details {
			var ed = &ErrorDetails{}
			err := anypb.UnmarshalTo(d, ed, proto.UnmarshalOptions{})
			if err != nil {
				return nil, err
			}
			internalDetails = append(internalDetails, ed)
		}
		resp.Details = internalDetails
	}

	return json.Marshal(resp)
}

// Errorf returns Error(c, fmt.Sprintf(format, a...)).
func Errorf(c codes.Code, format string, a ...interface{}) *TigrisError {
	return Error(c, fmt.Sprintf(format, a...))
}

// Error returns an error representing c and msg.  If c is OK, returns nil.
func Error(c codes.Code, msg string) *TigrisError {
	if c == codes.OK {
		return nil
	}

	return &TigrisError{
		Code:    c,
		Message: msg,
	}
}
