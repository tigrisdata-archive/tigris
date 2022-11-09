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

package realtime

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/store/cache"
)

// RTMRunner is used to run the realtime HTTP APIs related to channel like accessing a channel, subscribing to a channel, etc
type RTMRunner interface {
	Run(ctx context.Context, tenant *metadata.Tenant) (*Response, error)
}

type RTMRunnerFactory struct {
	cache   cache.Cache
	factory *ChannelFactory
}

// NewRTMRunnerFactory returns RTMRunnerFactory object.
func NewRTMRunnerFactory(cache cache.Cache, factory *ChannelFactory) *RTMRunnerFactory {
	return &RTMRunnerFactory{
		cache:   cache,
		factory: factory,
	}
}

func (f *RTMRunnerFactory) GetMessagesRunner(r *api.MessagesRequest) *MessagesRunner {
	return &MessagesRunner{
		baseRunner: newBaseRunner(f.cache, f.factory),
		req:        r,
	}
}

func (f *RTMRunnerFactory) GetReadMessagesRunner(r *api.ReadMessagesRequest, streaming Streaming) *ReadMessagesRunner {
	return &ReadMessagesRunner{
		baseRunner: newBaseRunner(f.cache, f.factory),
		req:        r,
		streaming:  streaming,
	}
}

func (f *RTMRunnerFactory) GetChannelRunner() *ChannelRunner {
	return &ChannelRunner{
		baseRunner: newBaseRunner(f.cache, f.factory),
	}
}

type baseRunner struct {
	cache   cache.Cache
	factory *ChannelFactory
}

func newBaseRunner(cache cache.Cache, factory *ChannelFactory) *baseRunner {
	return &baseRunner{
		cache:   cache,
		factory: factory,
	}
}

func (runner *baseRunner) getProject(ctx context.Context, tenant *metadata.Tenant, project string) (*metadata.Database, error) {
	proj, err := tenant.GetDatabase(ctx, project)
	if err != nil {
		return nil, err
	}
	if proj == nil {
		return nil, errors.NotFound("project doesn't exist '%s'", project)
	}
	return proj, err
}

// MessagesRunner is to publish messages to a channel
type MessagesRunner struct {
	*baseRunner

	req *api.MessagesRequest
}

func (runner *MessagesRunner) Run(ctx context.Context, tenant *metadata.Tenant) (*Response, error) {
	project, err := runner.getProject(ctx, tenant, runner.req.Project)
	if err != nil {
		return nil, err
	}

	channel, err := runner.factory.GetOrCreateChannel(ctx, tenant.GetNamespace().Id(), project.Id(), runner.req.Channel)
	if err != nil {
		return nil, err
	}

	var ids []string
	for _, m := range runner.req.Messages {
		streamData, err := NewEventDataFromMessage("", "", m.Name, m)
		if err != nil {
			return nil, err
		}

		id, err := channel.PublishMessage(ctx, streamData)
		if err != nil {
			return nil, err
		}

		ids = append(ids, id)
	}

	return &Response{
		Response: &api.MessagesResponse{
			Ids: ids,
		},
	}, nil
}

type ReadMessagesRunner struct {
	*baseRunner

	req       *api.ReadMessagesRequest
	streaming Streaming
}

func (runner *ReadMessagesRunner) Run(ctx context.Context, tenant *metadata.Tenant) (*Response, error) {
	project, err := runner.getProject(ctx, tenant, runner.req.Project)
	if err != nil {
		return nil, err
	}

	channel, err := runner.factory.GetChannel(ctx, tenant.GetNamespace().Id(), project.Id(), runner.req.Channel)
	if err != nil {
		return nil, err
	}

	pos := runner.req.GetStart()
	if len(pos) == 0 {
		pos = streamFromCurrentPos
	}

	var count = int64(0)
	for {
		resp, exists, err := channel.Read(ctx, pos)
		if !exists {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		var id string
		for _, m := range resp.Messages {
			data, err := resp.Decode(m)
			if err != nil {
				return nil, err
			}

			md, err := DecodeStreamMD(data.Metadata)
			if err != nil {
				return nil, err
			}
			runner.streaming.Send(&api.ReadMessagesResponse{
				Message: &api.Message{
					Id:   &m.ID,
					Name: md.EventName,
					Data: data.RawData,
				},
			})
			count++
			if runner.req.GetLimit() > 0 && count == runner.req.GetLimit() {
				return nil, nil
			}

			id = m.ID
		}

		if len(id) > 0 {
			split := strings.Split(id, "-")
			incrId, _ := strconv.ParseInt(strings.Split(id, "-")[1], 10, 64)
			pos = fmt.Sprintf("%s-%d", split[0], incrId+1)
		}
	}
}

type ChannelRunner struct {
	*baseRunner

	channelReq        *api.GetRTChannelRequest
	channelsReq       *api.GetRTChannelsRequest
	listSubscriptions *api.ListSubscriptionRequest
}

func (runner *ChannelRunner) SetChannelReq(req *api.GetRTChannelRequest) {
	runner.channelReq = req
}

func (runner *ChannelRunner) SetChannelsReq(req *api.GetRTChannelsRequest) {
	runner.channelsReq = req
}

func (runner *ChannelRunner) SetListSubscriptionsReq(req *api.ListSubscriptionRequest) {
	runner.listSubscriptions = req
}

func (runner *ChannelRunner) Run(ctx context.Context, tenant *metadata.Tenant) (*Response, error) {
	if runner.listSubscriptions != nil {
		project, err := runner.getProject(ctx, tenant, runner.listSubscriptions.Project)
		if err != nil {
			return nil, err
		}

		channel, err := runner.factory.GetChannel(ctx, tenant.GetNamespace().Id(), project.Id(), runner.listSubscriptions.Channel)
		if err != nil {
			return nil, err
		}

		watchers := channel.ListWatchers()
		return &Response{
			Response: &api.ListSubscriptionResponse{
				Devices: watchers,
			},
		}, nil
	} else if runner.channelsReq != nil {
		project, err := runner.getProject(ctx, tenant, runner.channelsReq.Project)
		if err != nil {
			return nil, err
		}

		channels, err := runner.factory.ListChannels(ctx, tenant.GetNamespace().Id(), project.Id(), "*")
		if err != nil {
			return nil, err
		}

		var channelsResp []*api.ChannelMetadata
		for _, c := range channels {
			channelsResp = append(channelsResp, &api.ChannelMetadata{
				Channel: c,
			})
		}

		return &Response{
			Response: &api.GetRTChannelsResponse{
				Channels: channelsResp,
			},
		}, nil
	} else {
		project, err := runner.getProject(ctx, tenant, runner.channelReq.Project)
		if err != nil {
			return nil, err
		}

		channels, err := runner.factory.ListChannels(ctx, tenant.GetNamespace().Id(), project.Id(), runner.channelReq.Channel)
		if err != nil {
			return nil, err
		}

		if len(channels) == 0 {
			return nil, errors.NotFound("channel '%s' not present ", runner.channelReq.Channel)
		}

		return &Response{
			Response: &api.GetRTChannelResponse{
				Channel: channels[0],
			},
		}, nil
	}
}
