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

package search

import (
	"fmt"
	"net/http"

	api "github.com/tigrisdata/tigris/api/server/v1"
	apiErrors "github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
)

// createApiError helps construct API errors from internal errors.
func createApiError(err error) error {
	switch e := err.(type) {
	case metadata.Error:
		switch e.Code() {
		case metadata.ErrCodeProjectNotFound, metadata.ErrCodeSearchIndexNotFound:
			return apiErrors.NotFound(e.Error())
		case metadata.ErrCodeSearchIndexExists:
			return apiErrors.AlreadyExists(e.Error())
		}
	case search.Error:
		switch e.HttpCode {
		case http.StatusConflict:
			return apiErrors.AlreadyExists(e.Msg)
		default:
			return api.Errorf(api.FromHttpCode(e.HttpCode), e.Msg)
		}
	case kv.StoreError:
		switch e.Code() {
		case kv.ErrCodeTransactionMaxDuration, kv.ErrCodeTransactionTimedOut:
			return apiErrors.DeadlineExceeded("the server is taking longer than 5 seconds to process the transaction")
		case kv.ErrCodeTransactionNotCommitted:
			return apiErrors.DeadlineExceeded("the transaction may not be committed")
		case kv.ErrCodeValueSizeExceeded, kv.ErrCodeTransactionSizeExceeded:
			return apiErrors.ContentTooLarge(e.Msg())
		}
	default:
		return err
	}
	return err
}

func shouldRecheckTenantVersion(err error) bool {
	if e, ok := err.(metadata.Error); ok {
		return e.Code() == metadata.ErrCodeProjectNotFound || e.Code() == metadata.ErrCodeSearchIndexNotFound
	}

	return false
}

func convertStoreErrToApiErr(id string, code int, msg string) *api.Error {
	if len(msg) == 0 || code == 0 {
		return nil
	}

	switch code {
	case http.StatusConflict:
		return &api.Error{
			Code:    http.StatusConflict,
			Message: fmt.Sprintf("A document with id '%s' already exists.", id),
		}
	default:
		return &api.Error{
			Code:    api.Code(code),
			Message: msg,
		}
	}
}
