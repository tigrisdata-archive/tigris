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

package v1

import (
	"context"

	structpb "github.com/golang/protobuf/ptypes/struct"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/encoding"
	"github.com/tigrisdata/tigrisdb/query/filter"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/protobuf/encoding/protojson"
)

// QueryRunner is responsible for executing the current query and return the response
type QueryRunner interface {
	Run(ctx context.Context, req *Request) (*Response, error)
}

// QueryRunnerFactory is responsible for creating query runners for different queries
type QueryRunnerFactory struct {
	txMgr   *transaction.Manager
	encoder encoding.Encoder
}

// NewQueryRunnerFactory returns QueryRunnerFactory object
func NewQueryRunnerFactory(txMgr *transaction.Manager, encoder encoding.Encoder) *QueryRunnerFactory {
	return &QueryRunnerFactory{
		txMgr:   txMgr,
		encoder: encoder,
	}
}

// GetTxQueryRunner returns TxQueryRunner
func (f *QueryRunnerFactory) GetTxQueryRunner() *TxQueryRunner {
	return &TxQueryRunner{
		txMgr:   f.txMgr,
		encoder: f.encoder,
	}
}

// GetStreamingQueryRunner returns StreamingQueryRunner
func (f *QueryRunnerFactory) GetStreamingQueryRunner(streaming Streaming) *StreamingQueryRunner {
	return &StreamingQueryRunner{
		txMgr:     f.txMgr,
		encoder:   f.encoder,
		streaming: streaming,
	}
}

// TxQueryRunner is a runner used for Queries mainly writes that needs to be executed in the context of the transaction
type TxQueryRunner struct {
	txMgr   *transaction.Manager
	encoder encoding.Encoder
}

// Run is responsible for running/executing the query
func (q *TxQueryRunner) Run(ctx context.Context, req *Request) (*Response, error) {
	tx, err := q.txMgr.GetInheritedOrStartTx(ctx, api.GetTransaction(req), false)
	if err != nil {
		return nil, err
	}

	var txErr error
	defer func() {
		var err error
		if txErr == nil {
			err = tx.Commit(ctx)
		} else {
			err = tx.Rollback(ctx)
		}
		if txErr == nil {
			txErr = err
		}
	}()

	for _, d := range req.documents {
		key, err := q.encoder.BuildKey(d.Doc.GetFields(), req.collection)
		if err != nil {
			return nil, err
		}

		value, err := d.Doc.MarshalJSON()
		if err != nil {
			return nil, err
		}

		switch api.RequestType(req) {
		case api.Insert:
			txErr = tx.Insert(ctx, key, value)
		case api.Replace:
			txErr = tx.Replace(ctx, key, value)
		case api.Update:
			txErr = tx.Update(ctx, key, value)
		case api.Delete:
			txErr = tx.Delete(ctx, key)
		}

		if txErr != nil {
			return nil, err
		}
	}
	if txErr != nil {
		return nil, txErr
	}

	return &Response{}, err
}

// StreamingQueryRunner is a runner used for Queries that are reads and needs to return result in streaming fashion
type StreamingQueryRunner struct {
	txMgr     *transaction.Manager
	encoder   encoding.Encoder
	streaming Streaming
}

// Run is responsible for running/executing the query
func (q *StreamingQueryRunner) Run(ctx context.Context, req *Request) (*Response, error) {
	_, err := q.txMgr.GetInherited(api.GetTransaction(req))
	if err != nil {
		return nil, err
	}

	filters, err := filter.Build(api.GetFilter(req.Request))
	if err != nil {
		return nil, err
	}

	kb := filter.NewKeyBuilder(filter.NewStrictEqKeyComposer(req.collection.StorageName()))
	iKeys, err := kb.Build(filters, req.collection.PrimaryKeys())
	if err != nil {
		return nil, err
	}
	for _, key := range iKeys {
		it, err := q.txMgr.GetKV().Read(ctx, req.collection.StorageName(), kv.BuildKey(key.PrimaryKeys()...))
		if err != nil {
			return nil, err
		}

		for it.More() {
			v, err := it.Next()
			if err != nil {
				return nil, err
			}

			s := &structpb.Struct{}
			err = protojson.Unmarshal(v.Value, s)
			if err != nil {
				return nil, err
			}
			if err := q.streaming.Send(&api.ReadResponse{
				Doc: s,
			}); ulog.E(err) {
				return nil, err
			}
		}
	}

	return &Response{}, nil
}
