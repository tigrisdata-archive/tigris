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
	"encoding/json"

	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"github.com/tigrisdata/tigrisdb/encoding"
	"github.com/tigrisdata/tigrisdb/query/filter"
	"github.com/tigrisdata/tigrisdb/server/transaction"
	"github.com/tigrisdata/tigrisdb/store/kv"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/protobuf/types/known/structpb"
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
	tx, err := q.txMgr.GetInheritedOrStartTx(ctx, api.GetTransaction(req.Request), false)
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

	if reqFilter := api.GetFilter(req); reqFilter != nil {
		txErr = q.iterateFilter(ctx, req, tx, reqFilter)
	} else {
		txErr = q.iterateDocument(ctx, req, tx)
	}

	if txErr != nil {
		return nil, txErr
	}

	return &Response{}, err
}

func (q *TxQueryRunner) iterateFilter(ctx context.Context, req *Request, tx transaction.Tx, reqFilter []byte) error {
	filters, err := filter.Build(reqFilter)
	if err != nil {
		return err
	}

	kb := filter.NewKeyBuilder(filter.NewStrictEqKeyComposer(req.collection.StorageName()))
	iKeys, err := kb.Build(filters, req.collection.PrimaryKeys())
	if err != nil {
		return err
	}

	for _, key := range iKeys {
		switch api.RequestType(req) {
		case api.Update:
			err = tx.Update(ctx, key, req.Request.(*api.UpdateRequest).Fields)
		case api.Delete:
			err = tx.Delete(ctx, key)
		}

		if err != nil {
			return err
		}
	}

	return err
}

func (q *TxQueryRunner) iterateDocument(ctx context.Context, req *Request, tx transaction.Tx) error {
	var err error
	for _, d := range req.documents {
		// ToDo: need to implement our own decoding to only extract custom keys
		var s = &structpb.Struct{}
		if err = json.Unmarshal(d, s); err != nil {
			return err
		}

		key, err := q.encoder.BuildKey(s.GetFields(), req.collection)
		if err != nil {
			return err
		}

		switch api.RequestType(req) {
		case api.Insert:
			err = tx.Insert(ctx, key, d)
		case api.Replace:
			err = tx.Replace(ctx, key, d)
		}

		if err != nil {
			return err
		}
	}

	return err
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

			if err := q.streaming.Send(&api.ReadResponse{
				Doc: v.Value,
			}); ulog.E(err) {
				return nil, err
			}
		}
	}

	return &Response{}, nil
}
