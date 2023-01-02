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
	"errors"

	api "github.com/tigrisdata/tigris/api/server/v1"
)

// Internal constructs internal server error (HTTP: 500).
func Internal(format string, args ...any) error {
	return api.Errorf(api.Code_INTERNAL, format, args...)
}

// InvalidArgument constructs bad request error (HTTP: 400).
func InvalidArgument(format string, args ...any) error {
	return api.Errorf(api.Code_INVALID_ARGUMENT, format, args...)
}

// AlreadyExists construct conflict error (HTTP: 409).
func AlreadyExists(format string, args ...any) error {
	return api.Errorf(api.Code_ALREADY_EXISTS, format, args...)
}

// NotFound constructs not found error (HTTP: 404).
func NotFound(format string, args ...any) error {
	return api.Errorf(api.Code_NOT_FOUND, format, args...)
}

// Unauthenticated construct unauthorized error (HTTP: 401).
func Unauthenticated(format string, args ...any) error {
	return api.Errorf(api.Code_UNAUTHENTICATED, format, args...)
}

// ResourceExhausted constructs too many requests error (HTTP: 429).
func ResourceExhausted(format string, args ...any) *api.TigrisError {
	return api.Errorf(api.Code_RESOURCE_EXHAUSTED, format, args...)
}

// PermissionDenied constructs forbidden error (HTTP: 403).
func PermissionDenied(format string, args ...any) error {
	return api.Errorf(api.Code_PERMISSION_DENIED,
		format, args...)
}

// DeadlineExceeded constructs timeout error (HTTP: 504).
func DeadlineExceeded(format string, args ...any) error {
	return api.Errorf(api.Code_DEADLINE_EXCEEDED,
		format, args...)
}

// Unimplemented constructs not implemented error (HTTP: 501).
func Unimplemented(format string, args ...any) error {
	return api.Errorf(api.Code_UNIMPLEMENTED,
		format, args...)
}

// MethodNotAllowed constructs method not allowed error (HTTP: 405).
func MethodNotAllowed(format string, args ...any) error {
	return api.Errorf(api.Code_METHOD_NOT_ALLOWED,
		format, args...)
}

// Aborted constructs conflict error (HTTP: 409).
func Aborted(format string, args ...any) error {
	return api.Errorf(api.Code_ABORTED,
		format, args...)
}

// Unavailable constructs service unavailable error (HTTP: 503).
func Unavailable(format string, args ...any) error {
	return api.Errorf(api.Code_UNAVAILABLE,
		format, args...)
}

// Unknown constructs internal server error (HTTP: 500).
func Unknown(format string, args ...any) error {
	return api.Errorf(api.Code_UNKNOWN,
		format, args...)
}

// Convenience helpers.

var (
	As = errors.As
	Is = errors.Is
)
