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
