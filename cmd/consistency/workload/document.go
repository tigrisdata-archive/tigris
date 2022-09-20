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

package workload

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Document struct {
	F1 int64
	F2 string
	F3 []byte
	F4 uuid.UUID
	F5 time.Time
}

func NewDocument(uniqueId int64) *Document {
	return &Document{
		F1: uniqueId,
		F2: fmt.Sprintf("id_%d", uniqueId),
		F3: []byte(`1234`),
		F4: uuid.New(),
		F5: time.Now().UTC(),
	}
}

func Serialize(doc *Document) ([]byte, error) {
	return json.Marshal(doc)
}

func Deserialize(raw []byte) (*Document, error) {
	var doc Document
	err := json.Unmarshal(raw, &doc)
	return &doc, err
}
