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

package util

import (
	"io"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
)

// JSONMix is a Marshaler which exploits GRPC gateway's nice feature to
// recursively allocate and populate pointers
// while still marshalling int64s as numbers.
//(PB version marshals int64s as strings)

type JSONMix struct {
	builtin runtime.JSONBuiltin
	pb      runtime.JSONPb
}

func (*JSONMix) ContentType(_ interface{}) string {
	return "application/json"
}

func (j *JSONMix) Marshal(v interface{}) ([]byte, error) {
	return j.builtin.Marshal(v)
}

func (j *JSONMix) Unmarshal(data []byte, v interface{}) error {
	return j.pb.Unmarshal(data, v)
}

func (j *JSONMix) NewDecoder(r io.Reader) runtime.Decoder {
	return j.pb.NewDecoder(r)
}

func (j *JSONMix) NewEncoder(w io.Writer) runtime.Encoder {
	return j.builtin.NewEncoder(w)
}

func (j *JSONMix) Delimiter() []byte {
	return j.builtin.Delimiter()
}
