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

package api

func IsTxSupported(req Request) bool {
	switch RequestType(req) {
	case Insert, Replace, Update, Delete, Read:
		return true
	}

	return false
}

func GetTransaction(req Request) *TransactionCtx {
	switch r := req.(type) {
	case *InsertRequest:
		if r.GetOptions() == nil {
			return nil
		}
		return r.GetOptions().GetTxCtx()
	default:
		return nil
	}
}

func GetFilter(req Request) []Filter {
	switch r := req.(type) {
	case *UpdateRequest:
		return r.Filter
	case *DeleteRequest:
		return r.Filter
	default:
		return nil
	}
}
