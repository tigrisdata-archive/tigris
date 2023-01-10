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

package main

import (
	"flag"
	"log"
	"net/url"
	"time"

	"github.com/tigrisdata/tigris/cmd/realtime/app"
)

func main() {
	flag.Parse()
	log.SetFlags(0)

	u := url.URL{Scheme: "ws", Host: "localhost:8083", Path: "/v1/projects/p1/realtime"}
	log.Printf("connecting to %s", u.String())

	a := app.NewApp(u, "test")
	for i := 0; i < 3; i++ {
		if err := a.AddDevice(); err != nil {
			log.Fatalf("adding a device failed %v", err)
		}
	}

	if err := a.Start(); err != nil {
		log.Fatalf("starting devices failed %v", err)
	}
	a.StartPublishing()

	time.Sleep(10 * time.Second)

	a.Close()
}
