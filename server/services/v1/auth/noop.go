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

package auth

import (
	"context"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
)

type noop struct{}

func (noop) GetAccessToken(_ context.Context, _ *api.GetAccessTokenRequest) (*api.GetAccessTokenResponse, error) {
	return &api.GetAccessTokenResponse{}, nil
}

func (noop) CreateAppKey(_ context.Context, _ *api.CreateAppKeyRequest) (*api.CreateAppKeyResponse, error) {
	return nil, errors.Internal("authentication not enabled on this server")
}

func (noop) UpdateAppKey(_ context.Context, _ *api.UpdateAppKeyRequest) (*api.UpdateAppKeyResponse, error) {
	return nil, errors.Internal("authentication not enabled on this server")
}

func (noop) RotateAppKey(_ context.Context, _ *api.RotateAppKeyRequest) (*api.RotateAppKeyResponse, error) {
	return nil, errors.Internal("authentication not enabled on this server")
}

func (noop) DeleteAppKey(_ context.Context, _ *api.DeleteAppKeyRequest) (*api.DeleteAppKeyResponse, error) {
	return nil, errors.Internal("authentication not enabled on this server")
}

func (noop) ListAppKeys(_ context.Context, _ *api.ListAppKeysRequest) (*api.ListAppKeysResponse, error) {
	return nil, errors.Internal("authentication not enabled on this server")
}

func (noop) DeleteAppKeys(_ context.Context, _ string) error {
	return errors.Internal("authentication not enabled on this server")
}
