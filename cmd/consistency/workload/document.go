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
	"fmt"
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
	Id int64 `json:"id"`
	F2 string
	F3 []byte
	F4 uuid.UUID
	F5 time.Time
}

func (d *Document) ID() int64 {
	return d.Id
}

func NewDocument(uniqueId int64) *Document {
	return &Document{
		Id: uniqueId,
		F2: fmt.Sprintf("id_%d", uniqueId),
		F3: []byte(`1234`),
		F4: uuid.New(),
		F5: time.Now().UTC(),
	}
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
	Random    string   `json:"random" fake:"{sentence:10}"`
	Name      string   `json:"name"  fake:"{firstname}"`
	URL       string   `json:"url" fake:"{url}"`
	Domain    string   `json:"domain"  fake:"{domainname}"`
	Sentence  string   `json:"sentence" fake:"{sentence:3}"`
	Company   string   `json:"company"  fake:"{company}"`
	Labels    []string `json:"labels"  fakesize:"200"`
	Address   Address  `json:"address"`
	NestedId  string   `json:"nested_id"  fake:"{uuid}"`
}

type DocumentV1 struct {
	Id        int64     `json:"id"`
	Cars      []string  `json:"cars" fake:"{carmaker}" fakesize:"10000"`
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
