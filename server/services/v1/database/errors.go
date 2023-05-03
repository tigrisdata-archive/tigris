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

package database

import (
	"net/http"

	api "github.com/tigrisdata/tigris/api/server/v1"
	apiErrors "github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
)

// CreateApiError helps construct API errors from internal errors.
func CreateApiError(err error) error {
	switch e := err.(type) {
	case nil:
		return nil
	case metadata.Error:
		switch e.Code() {
		case metadata.ErrCodeDatabaseNotFound, metadata.ErrCodeBranchNotFound:
			return apiErrors.NotFound(e.Error())
		case metadata.ErrCodeDatabaseBranchExists, metadata.ErrCodeDatabaseExists:
			return apiErrors.AlreadyExists(e.Error())
		case metadata.ErrCodeCannotDeleteBranch:
			return apiErrors.InvalidArgument(e.Error())
		case metadata.ErrCodeProjectNotFound:
			return apiErrors.NotFound(e.Error())
		}
	case search.Error:
		switch e.HttpCode {
		case http.StatusConflict:
			return apiErrors.AlreadyExists(e.Msg)
		case http.StatusBadRequest:
			return apiErrors.InvalidArgument(e.Msg)
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
			// ToDo: change it to 413, need to add it in proto
			return apiErrors.ContentTooLarge(e.Msg())
		case kv.ErrCodeConflictingTransaction:
			return apiErrors.Aborted(e.Msg())
		}
	default:
		return err
	}
	return err
}

func IsErrConflictingTransaction(err error) bool {
	return err == kv.ErrConflictingTransaction
}
