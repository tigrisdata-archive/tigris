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
	"context"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/internal"
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
	encType      internal.UserDataEncType
	conn         *websocket.Conn
	lastReceived time.Time
	chFactory    *ChannelFactory
	heartbeat    *HeartbeatTable
	tenant       *metadata.Tenant
	project      *metadata.Database
	watchers     map[string]*ChannelWatcher
}

func (s *Sessions) CreateDeviceSession(ctx context.Context, conn *websocket.Conn, params ConnectionParams) (*Session, error) {
	sessionId := params.SessionId
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

	proj, err := tenant.GetDatabase(ctx, metadata.NewDatabaseName(params.ProjectName))
	if err != nil {
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

		if proj, err = tenant.GetDatabase(ctx, metadata.NewDatabaseName(params.ProjectName)); err != nil {
			return nil, errors.NotFound("project '%s' not found", params.ProjectName)
		}
	}

	return &Session{
		id:        sessionId,
		conn:      conn,
		tenant:    tenant,
		project:   proj,
		encType:   params.ToEncodingType(),
		chFactory: s.channelFactory,
		watchers:  make(map[string]*ChannelWatcher),
		heartbeat: s.heartbeatFactory.GetHeartbeatTable(tenant.GetNamespace().Id(), proj.Id()),
	}, nil
}

func (session *Session) IsActive() bool {
	return time.Since(session.lastReceived) <= 30*time.Second
}

func (session *Session) OnPong(_ string) error {
	return session.sendHeartbeat()
}

func (session *Session) OnPing(_ string) error {
	return session.sendHeartbeat()
}

func (session *Session) OnClose(code int, _ string) error {
	if code == int(errors.CloseAbnormalClosure) {
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
		w.Disconnect()
	}

	return session.conn.Close()
}

// Start an entry point for handling all the events from a device.
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
			log.Err(err).Msgf("realtime send error '%s' %s", session.id, errEvent)
			err = SendReply(session.conn, session.encType, api.EventType_error, errEvent)
			log.Err(err).Msgf("failed to send error reply message")
		}
	}
}

// onMessage is responsible for handling all the messages that are received on websocket.
func (session *Session) onMessage(ctx context.Context, message []byte) *api.ErrorEvent {
	rtm, err := DecodeRealtime(session.encType, message)
	if err != nil {
		return errors.InternalWS(err.Error())
	}

	return session.handleMessage(ctx, rtm)
}

// handleMessage process the message received on websocket.
func (session *Session) handleMessage(ctx context.Context, req *api.RealTimeMessage) *api.ErrorEvent {
	decoded, err := DecodeEvent(session.encType, req.EventType, req.Event)
	if err != nil {
		return errors.InternalWS(err.Error())
	}

	switch req.EventType {
	case api.EventType_disconnect:
		event, ok := decoded.(*api.DisconnectEvent)
		if !ok {
			return errors.InternalWS("expecting 'disconnect' event")
		}

		if len(event.Channel) > 0 {
			watcher := session.watchers[event.Channel]
			watcher.Disconnect()
			delete(session.watchers, event.Channel)
		} else {
			if err := session.Close(); err != nil {
				return errors.InternalWS(err.Error())
			}
		}

		return nil
	case api.EventType_heartbeat:
		if err := session.sendHeartbeat(); err != nil {
			return errors.InternalWS(err.Error())
		}
		return nil
	case api.EventType_attach:
		event, ok := decoded.(*api.AttachEvent)
		if !ok {
			return errors.InternalWS("expecting 'attach' event")
		}

		// create a channel if it doesn't exist
		_, err := session.chFactory.GetOrCreateChannel(ctx, session.tenant.GetNamespace().Id(), session.project.Id(), event.Channel)
		if err != nil {
			return errors.InternalWS(err.Error())
		}
		return nil
	case api.EventType_detach:
		event, ok := decoded.(*api.DetachEvent)
		if !ok {
			return errors.InternalWS("expecting 'detach' event")
		}

		if watcher, ok := session.watchers[event.Channel]; ok {
			watcher.Disconnect()
			delete(session.watchers, event.Channel)
		}
		return nil
	case api.EventType_unsubscribe:
		event, ok := decoded.(*api.UnsubscribeEvent)
		if !ok {
			return errors.InternalWS("expecting 'detach' event")
		}

		if watcher, ok := session.watchers[event.Channel]; ok {
			watcher.Stop()
			delete(session.watchers, event.Channel)
		}
		return nil
	case api.EventType_subscribe:
		event, ok := decoded.(*api.SubscribeEvent)
		if !ok {
			return errors.InternalWS("expecting 'subscribe' event")
		}

		channel, err := session.chFactory.GetChannel(ctx, session.tenant.GetNamespace().Id(), session.project.Id(), event.Channel)
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
		err = SendReply(session.conn, session.encType, api.EventType_subscribed, &api.SubscribedEvent{
			Channel: event.Channel,
		})
		log.Err(err).Msgf("failed to send subscribe message")
		return nil
	case api.EventType_message:
		event, ok := decoded.(*api.MessageEvent)
		if !ok {
			return errors.InternalWS("expecting message event")
		}

		ch, err := session.chFactory.GetChannel(ctx, session.tenant.GetNamespace().Id(), session.project.Id(), event.Channel)
		if err != nil {
			return errors.InternalWS(err.Error())
		}

		streamData, err := NewMessageData(session.encType, session.clientId, session.socketId, event.Name, event)
		if err != nil {
			return errors.InternalWS(err.Error())
		}
		if _, err := ch.PublishMessage(ctx, streamData); err != nil {
			return errors.InternalWS(err.Error())
		}
		return nil
	case api.EventType_presence_member:
		event, ok := decoded.(*api.MessageEvent)
		if !ok {
			return errors.InternalWS("expecting presence event")
		}

		ch, err := session.chFactory.GetChannel(ctx, session.tenant.GetNamespace().Id(), session.project.Id(), event.Channel)
		if err != nil {
			return errors.InternalWS(err.Error())
		}

		streamData, err := NewPresenceData(session.encType, session.clientId, session.socketId, event.Name, event)
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

func SendReply(conn *websocket.Conn, encType internal.UserDataEncType, eventType api.EventType, event proto.Message) error {
	encEvent, err := EncodeEvent(encType, event)
	if err != nil {
		panic(err)
	}

	msg := &api.RealTimeMessage{
		EventType: eventType,
		Event:     encEvent,
	}

	encRealtime, err := EncodeRealtime(encType, msg)
	if err != nil {
		panic(err)
	}

	msgType := websocket.BinaryMessage

	if encType == internal.JsonEncoding {
		msgType = websocket.TextMessage
	}

	return conn.WriteMessage(
		msgType,
		encRealtime,
	)
}

func (session *Session) sendHeartbeat() error {
	var event proto.Message
	return SendReply(session.conn, session.encType, api.EventType_heartbeat, event)
}

func (session *Session) SendConnSuccess() error {
	session.socketId = uuid.New().String()
	event := &api.ConnectedEvent{
		SessionId: session.id,
		SocketId:  session.socketId,
	}

	return SendReply(session.conn, session.encType, api.EventType_connected, event)
}
