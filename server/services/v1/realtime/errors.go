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

package realtime

import (
	apiErrors "github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
)

// createApiError helps construct API errors from internal errors.
func createApiError(err error) error {
	switch e := err.(type) {
	case metadata.Error:
		if e.Code() == metadata.ErrCodeProjectNotFound {
			return apiErrors.NotFound(e.Error())
		}
	default:
		return err
	}
	return err
}
