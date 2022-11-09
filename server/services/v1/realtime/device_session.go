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
	"sync"
	"time"

	"github.com/gorilla/websocket"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/lib/uuid"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/request"
	"google.golang.org/protobuf/proto"
)

type Session struct {
	sync.RWMutex

	id           string
	clientId     string
	socketId     string
	closed       bool
	conn         *websocket.Conn
	lastReceived time.Time
	chFactory    *ChannelFactory
	heartbeat    *HeartbeatTable
	tenant       *metadata.Tenant
	project      *metadata.Database
	watchers     map[string]*ChannelWatcher
}

func (s *Sessions) CreateDeviceSession(ctx context.Context, conn *websocket.Conn, params ConnectionParams) (*Session, error) {
	var sessionId = params.SessionId
	if len(sessionId) == 0 {
		sessionId = uuid.NewUUIDAsString()
	}

	namespaceForThisSession, err := request.GetNamespace(ctx)
	if err != nil {
		return nil, err
	}
	tenant, err := s.tenantMgr.GetTenant(ctx, namespaceForThisSession)
	if err != nil {
		return nil, errors.NotFound("tenant '%s' not found", namespaceForThisSession)
	}

	proj, err := tenant.GetDatabase(ctx, params.ProjectName)
	if err != nil {
		return nil, errors.NotFound("project '%s' not found", params.ProjectName)
	}
	if proj == nil {
		tx, err := s.txMgr.StartTx(ctx)
		if err != nil {
			return nil, err
		}

		var version metadata.Version
		if version, err = s.versionH.Read(ctx, tx, false); err != nil {
			return nil, err
		}

		if err = tenant.Reload(ctx, tx, version); err != nil {
			return nil, err
		}

		if proj, _ = tenant.GetDatabase(ctx, params.ProjectName); proj == nil {
			return nil, errors.NotFound("project '%s' not found", params.ProjectName)
		}
	}

	return &Session{
		id:        sessionId,
		conn:      conn,
		tenant:    tenant,
		project:   proj,
		chFactory: s.channelFactory,
		watchers:  make(map[string]*ChannelWatcher),
		heartbeat: s.heartbeatFactory.GetHeartbeatTable(tenant.GetNamespace().Id(), proj.Id()),
	}, nil
}

func (session *Session) IsActive() bool {
	if time.Since(session.lastReceived) > 30*time.Second {
		return false
	}

	return true
}

func (session *Session) OnPong(_ string) error {
	return session.SendHeartbeat()
}

func (session *Session) OnPing(_ string) error {
	return session.SendHeartbeat()
}

func (session *Session) OnClose(code int, _ string) error {
	if code == 1006 {
		return nil
	}

	return session.Close()
}

func (session *Session) Close() error {
	session.Lock()
	defer session.Unlock()

	if session.closed {
		return nil
	}

	session.closed = true
	for _, w := range session.watchers {
		w.Close()
	}

	return session.conn.Close()
}

// Start an entry point for handling all the events from a device
func (session *Session) Start(ctx context.Context) error {
	for {
		_ = session.heartbeat.Ping(session.id)
		if session.closed {
			return nil
		}

		_, message, err := session.conn.ReadMessage()
		if err != nil {
			return err
		}
		session.lastReceived = time.Now()
		if errEvent := session.onMessage(ctx, message); errEvent != nil {
			session.SendReply(api.EventType_error, errEvent)
		}
	}
}

func (session *Session) onMessage(ctx context.Context, message []byte) *api.ErrorEvent {
	rtm, err := DecodeRealtime(jsonEncoding, message)
	if err != nil {
		return errors.InternalWS(err.Error())
	}

	return session.handleMessage(ctx, rtm)
}

func (session *Session) handleMessage(ctx context.Context, req *api.RealTimeMessage) *api.ErrorEvent {
	decoded, err := DecodeEvent(jsonEncoding, req.EventType, req.Event)
	if err != nil {
		return errors.InternalWS(err.Error())
	}

	switch req.EventType {
	case api.EventType_disconnect:
		event, ok := decoded.(*api.DisconnectEvent)
		if !ok {
			return errors.InternalWS("expecting disconnect event")
		}

		if len(event.Channel) > 0 {
			watcher := session.watchers[event.Channel]
			watcher.Close()
			delete(session.watchers, event.Channel)
		} else {
			if err := session.Close(); err != nil {
				return errors.InternalWS(err.Error())
			}
		}

		return nil
	case api.EventType_heartbeat:
		if err := session.SendHeartbeat(); err != nil {
			return errors.InternalWS(err.Error())
		}
		return nil
	case api.EventType_subscribe:
		event, ok := decoded.(*api.SubscribeEvent)
		if !ok {
			return errors.InternalWS("expecting subscribe event")
		}

		channel, err := session.chFactory.GetOrCreateChannel(ctx, session.tenant.GetNamespace().Id(), session.project.Id(), event.Channel)
		if err != nil {
			return errors.InternalWS(err.Error())
		}

		if _, ok := session.watchers[event.Channel]; ok {
			// if already watching ignore
			return nil
		}

		watcher, err := channel.GetWatcher(ctx, session.id, event.Position)
		if err != nil {
			return nil
		}
		session.watchers[event.Channel] = watcher
		watcher.StartWatching(NewDevicePusher(session, event.Channel).Watch)
		session.SendReply(api.EventType_subscribed, &api.SubscribedEvent{
			Channel: event.Channel,
		})
		return nil
	case api.EventType_message:
		event, ok := decoded.(*api.MessageEvent)
		if !ok {
			return errors.InternalWS("expecting message event")
		}

		ch, err := session.chFactory.GetOrCreateChannel(ctx, session.tenant.GetNamespace().Id(), session.project.Id(), event.Channel)
		if err != nil {
			return errors.InternalWS(err.Error())
		}

		streamData, err := NewMessageData(session.clientId, session.socketId, event.Name, event)
		if err != nil {
			return errors.InternalWS(err.Error())
		}
		if _, err := ch.PublishMessage(ctx, streamData); err != nil {
			return errors.InternalWS(err.Error())
		}
		return nil
	case api.EventType_presence_message:
		event, ok := decoded.(*api.MessageEvent)
		if !ok {
			return errors.InternalWS("expecting presence event")
		}

		ch, err := session.chFactory.GetOrCreateChannel(ctx, session.tenant.GetNamespace().Id(), session.project.Id(), event.Channel)
		if err != nil {
			return errors.InternalWS(err.Error())
		}

		streamData, err := NewPresenceData(session.clientId, session.socketId, event.Name, event)
		if err != nil {
			return errors.InternalWS(err.Error())
		}
		if _, err := ch.PublishPresence(ctx, streamData); err != nil {
			return errors.InternalWS(err.Error())
		}
	default:
		// ToDo: presence channel subscribe by adding a different watcher name
	}
	return nil
}

func (session *Session) SendReply(eventType api.EventType, event proto.Message) {
	eventEnc, err := EncodeEvent(jsonEncoding, eventType, event)
	if err != nil {
		panic(err)
	}

	msg := &api.RealTimeMessage{
		EventType: eventType,
		Event:     eventEnc,
	}

	encRealtime, err := EncodeRealtime(jsonEncoding, msg)
	if err != nil {
		panic(err)
	}

	_ = session.conn.WriteMessage(
		websocket.TextMessage,
		encRealtime,
	)
}

func (session *Session) SendHeartbeat() error {
	b, _ := EncodeRealtime(jsonEncoding, &api.RealTimeMessage{EventType: api.EventType_heartbeat})
	return session.conn.WriteMessage(websocket.TextMessage, b)
}

func (session *Session) SendConnectionEstablished() error {
	session.socketId = uuid.New().String()
	var event = &api.ConnectedMessage{
		SessionId: session.id,
		SocketId:  session.socketId,
	}

	session.SendReply(api.EventType_connected, event)
	return nil
}
