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
	F2 string `json:"F2" fake:"{sentence:50}"`
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
	City    string `json:"city"  fake:"{city}"`
	State   string `json:"state"  fake:"{state}"`
	Country string `json:"country"  fake:"{country}"`
}

type Nested struct {
	Timestamp int64    `json:"timestamp" fake:"{nanosecond}"`
	Random    string   `json:"random" fake:"{paragraph:10,10,50}"`
	Random1   string   `json:"random1" fake:"{paragraph:10,10,50}"`
	Random2   string   `json:"random2" fake:"{paragraph:10,10,50}"`
	Random3   string   `json:"random3" fake:"{paragraph:10,10,50}"`
	Random4   string   `json:"random4" fake:"{paragraph:10,10,50}"`
	Random5   string   `json:"random5" fake:"{paragraph:10,10,50}"`
	Random6   string   `json:"random6" fake:"{paragraph:10,10,50}"`
	Random7   string   `json:"random7" fake:"{paragraph:10,10,50}"`
	Random8   string   `json:"random8" fake:"{paragraph:10,10,50}"`
	Random9   string   `json:"random9" fake:"{paragraph:10,10,50}"`
	Random10  string   `json:"random10" fake:"{paragraph:10,10,50}"`
	Name      string   `json:"name"  fake:"{paragraph:10,10,50}"`
	URL       string   `json:"url" fake:"{paragraph:10,10,50}"`
	Domain    string   `json:"domain"  fake:"{sentence:50}"`
	Sentence  string   `json:"sentence" fake:"{paragraph:10,10,50}"`
	Company   string   `json:"company"  fake:"{paragraph:10,10,50}"`
	Labels    []string `json:"labels"  fakesize:"20000"`
	Address   Address  `json:"address"`
	NestedId  string   `json:"nested_id"  fake:"{uuid}"`
}

type DocumentV1 struct {
	Id        int64     `json:"id"`
	Cars      []string  `json:"cars" fake:"{carmaker}" fakesize:"20000"`
	Food      []string  `json:"food" fake:"{food}" fakesize:"20000"`
	CreatedAt time.Time `json:"created_at"  fake:"{date}"`
	UpdatedAt time.Time `json:"updated_at"  fake:"{date}"`
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
