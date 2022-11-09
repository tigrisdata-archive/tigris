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

package main

import (
	"flag"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"fmt"
)

func StartSubscriber(url url.URL) {
	c, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Printf("recv: %s", message)
		}
	}()

	err = c.WriteMessage(websocket.TextMessage, []byte(`{"event_type":4, "event": {"channel": "test"}}`))
	if err != nil {
		log.Println("write:", err)
		return
	}
	wg.Wait()
}

func StartPublisher(url url.URL) {
	c, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Printf("recv: %s", message)
		}
	}()

	for i := 0; i < 10000; i++ {
		time.Sleep(1 * time.Second)
		if err := c.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"event_type":3, "event": {"channel": "test", "data": {"a": %d}}}`, i))); err != nil {
			log.Fatalf("write: %v\n", err)
			return
		}
	}

	wg.Wait()
}

func main() {
	flag.Parse()
	log.SetFlags(0)

	u := url.URL{Scheme: "ws", Host: "localhost:8082", Path: "/v1/projects/p1/realtime"}
	log.Printf("connecting to %s", u.String())

	go StartSubscriber(u)
	go StartPublisher(u)
	select {}
}
