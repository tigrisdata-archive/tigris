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

package workload

import (
	"math/rand"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
)

type IDocument interface {
	ID() int64
}

type Document struct {
	Id int64  `json:"id"`
	F2 string `fake:"{sentence:50}" json:"F2"`
	F3 []byte
	F4 uuid.UUID
	F5 time.Time
}

func (d *Document) ID() int64 {
	return d.Id
}

func NewDocument(uniqueId int64) *Document {
	var document Document
	if err := gofakeit.Struct(&document); err != nil {
		log.Panic().Err(err).Msgf("failed in generating fake document")
	}
	document.Id = uniqueId
	document.F3 = []byte(document.F2[0:rand.Intn(len(document.F2))]) //nolint:gosec
	document.F4 = uuid.New()
	document.F5 = time.Now().UTC()
	return &document
}

func SerializeDocV1(doc *DocumentV1) ([]byte, error) {
	return jsoniter.Marshal(doc)
}

func Serialize(doc *Document) ([]byte, error) {
	return jsoniter.Marshal(doc)
}

func Deserialize(raw []byte, doc any) error {
	return jsoniter.Unmarshal(raw, doc)
}

type Address struct {
	City    string `fake:"{city}"    json:"city"`
	State   string `fake:"{state}"   json:"state"`
	Country string `fake:"{country}" json:"country"`
}

type Nested struct {
	Timestamp int64    `fake:"{nanosecond}"         json:"timestamp"`
	Random    string   `fake:"{paragraph:10,10,50}" json:"random"`
	Random1   string   `fake:"{paragraph:10,10,50}" json:"random1"`
	Random2   string   `fake:"{paragraph:10,10,50}" json:"random2"`
	Random3   string   `fake:"{paragraph:10,10,50}" json:"random3"`
	Random4   string   `fake:"{paragraph:10,10,50}" json:"random4"`
	Random5   string   `fake:"{paragraph:10,10,50}" json:"random5"`
	Random6   string   `fake:"{paragraph:10,10,50}" json:"random6"`
	Random7   string   `fake:"{paragraph:10,10,50}" json:"random7"`
	Random8   string   `fake:"{paragraph:10,10,50}" json:"random8"`
	Random9   string   `fake:"{paragraph:10,10,50}" json:"random9"`
	Random10  string   `fake:"{paragraph:10,10,50}" json:"random10"`
	Name      string   `fake:"{paragraph:10,10,50}" json:"name"`
	URL       string   `fake:"{paragraph:10,10,50}" json:"url"`
	Domain    string   `fake:"{sentence:50}"        json:"domain"`
	Sentence  string   `fake:"{paragraph:10,10,50}" json:"sentence"`
	Company   string   `fake:"{paragraph:10,10,50}" json:"company"`
	Labels    []string `fakesize:"20000"            json:"labels"`
	Address   Address  `json:"address"`
	NestedId  string   `fake:"{uuid}"               json:"nested_id"`
}

type DocumentV1 struct {
	Id        int64     `json:"id"`
	Cars      []string  `fake:"{carmaker}" fakesize:"20000"  json:"cars"`
	Food      []string  `fake:"{food}"     fakesize:"20000"  json:"food"`
	CreatedAt time.Time `fake:"{date}"     json:"created_at"`
	UpdatedAt time.Time `fake:"{date}"     json:"updated_at"`
	Nested    *Nested   `json:"nested"`
}

func NewDocumentV1(id int64) *DocumentV1 {
	var nested Nested
	if err := gofakeit.Struct(&nested); err != nil {
		log.Panic().Err(err).Msgf("failed in generating fake record")
	}

	var document DocumentV1
	if err := gofakeit.Struct(&document); err != nil {
		log.Panic().Err(err).Msgf("failed in generating fake document")
	}

	document.Id = id
	document.Nested = &nested

	return &document
}

func (d *DocumentV1) ID() int64 {
	return d.Id
}
