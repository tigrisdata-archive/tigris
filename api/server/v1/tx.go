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
		r.GetOptions().ProtoReflect()
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *ReplaceRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *UpdateRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *DeleteRequest:
		if r.GetOptions() == nil || r.GetOptions().GetWriteOptions() == nil {
			return nil
		}
		return r.GetOptions().GetWriteOptions().GetTxCtx()
	case *ReadRequest:
		if r.GetOptions() == nil {
			return r.GetOptions().GetTxCtx()
		}
	case *CreateOrUpdateCollectionRequest:
		if r.GetOptions() == nil || r.GetOptions().GetTxCtx() == nil {
			return nil
		}
		return r.GetOptions().GetTxCtx()
	}
	return nil
}
