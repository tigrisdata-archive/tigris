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

package errors

import (
	"fmt"

	api "github.com/tigrisdata/tigris/api/server/v1"
)

type WSErrorCode int32

const (
	CodeOK                                   = 0
	CloseNormalClosure           WSErrorCode = 1000
	CloseGoingAway               WSErrorCode = 1001
	CloseProtocolError           WSErrorCode = 1002
	CloseUnsupportedData         WSErrorCode = 1003
	CloseNoStatusReceived        WSErrorCode = 1005
	CloseAbnormalClosure         WSErrorCode = 1006
	CloseInvalidFramePayloadData WSErrorCode = 1007
	ClosePolicyViolation         WSErrorCode = 1008
	CloseMessageTooBig           WSErrorCode = 1009
	CloseMandatoryExtension      WSErrorCode = 1010
	CloseInternalServerErr       WSErrorCode = 1011
	CloseServiceRestart          WSErrorCode = 1012
	CloseTryAgainLater           WSErrorCode = 1013
	CloseTLSHandshake            WSErrorCode = 1015
)

func InternalWS(format string, args ...any) *api.ErrorEvent {
	return Errorf(CloseInternalServerErr, format, args...)
}

func Errorf(c WSErrorCode, format string, a ...interface{}) *api.ErrorEvent {
	if c == CodeOK {
		return nil
	}

	e := &api.ErrorEvent{
		Code:    int32(c),
		Message: fmt.Sprintf(format, a...),
	}

	return e
}
