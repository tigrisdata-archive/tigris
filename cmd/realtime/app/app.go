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

package app

import (
	"fmt"
	"net/url"

	"github.com/rs/zerolog/log"
)

type App struct {
	Url     url.URL
	Channel string
	Devices []*Device
}

func NewApp(u url.URL, channel string) *App {
	return &App{
		Url:     u,
		Channel: channel,
	}
}

func (a *App) AddDevice() error {
	next := len(a.Devices)
	device, err := NewDevice(fmt.Sprintf("%d", next+1), a.Url)
	if err != nil {
		return err
	}

	a.Devices = append(a.Devices, device)
	return nil
}

func (a *App) Start() error {
	for _, d := range a.Devices {
		if err := d.Attach(a.Channel); err != nil {
			return err
		}
	}

	for _, d := range a.Devices {
		if err := d.Subscribe(a.Channel); err != nil {
			return err
		}
	}

	return nil
}

func (a *App) StartPublishing() {
	for i := 0; i < 20; i++ {
		if err := a.Devices[0].Publish(a.Channel, fmt.Sprintf("%d", i)); err != nil {
			log.Err(err).Msg("failed to publish")
		}
	}
}

func (a *App) Close() error {
	for _, d := range a.Devices {
		if err := d.DeAttach(a.Channel); err != nil {
			return err
		}

		d.Close()
	}

	return nil
}
