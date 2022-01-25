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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	userHTTP "github.com/tigrisdata/tigrisdb/api/client/v1/user"

	ulog "github.com/tigrisdata/tigrisdb/util/log"

	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	"google.golang.org/grpc"
)

type crudClient interface {
	Insert(ctx context.Context, docs ...interface{}) error // return rows affected and per doc error?
	Update(ctx context.Context, docs ...interface{}) error
	Delete(ctx context.Context, docs ...interface{}) error
	Replace(ctx context.Context, docs ...interface{}) error
	Read(ctx context.Context, docs ...interface{}) error
}

type tableClient interface {
	Create(ctx context.Context, db string, table string, key string) error
	Drop(ctx context.Context, db string, table string) error
	Use(db string, table string) crudClient
	BeginTx() (txClient, error)
}

type txClient interface {
	Use(db string, table string) crudClient
	Commit() error
	Rollback() error
}

type client interface {
	tableClient
	Close() error
	//txClient
}

func NewClient(ctx context.Context, host string, port int16) (client, error) {
	return newGRPCClient(ctx, host, port)
	//return newHTTPClient(ctx, host, port)
}

type grpcClient struct {
	api.TigrisDBClient
	conn *grpc.ClientConn
}

type grpcCRUDClient struct {
	c     api.TigrisDBClient
	db    string
	table string
}

/*
type grpcClientIface interface {
	api.TigrisDBClient
	Close() error
}
*/

func newGRPCClient(_ context.Context, host string, port int16) (*grpcClient, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", host, port), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatal().Err(err).Msg("grpc connect failed")
	}

	return &grpcClient{TigrisDBClient: api.NewTigrisDBClient(conn), conn: conn}, nil
}

func (c *grpcClient) Close() error {
	return c.conn.Close()
}

func (c *grpcClient) Create(ctx context.Context, db string, table string, key string) error {
	_, err := c.CreateTable(ctx, &api.TigrisDBRequest{
		Db:    db,
		Table: table,
		Key:   key,
	})
	return err
}

func (c *grpcClient) Drop(ctx context.Context, db string, table string) error {
	_, err := c.DropTable(ctx, &api.TigrisDBRequest{
		Db:    db,
		Table: table,
	})
	return err
}

func (c *grpcClient) Use(db string, table string) crudClient {
	return &grpcCRUDClient{c: c.TigrisDBClient, db: db, table: table}
}

func (c *grpcClient) BeginTx() (txClient, error) {
	return nil, fmt.Errorf("not implemented")
}

func marshalDocsGeneric(docs []interface{}, add func(doc []byte)) error {
	for _, v := range docs {
		switch reflect.TypeOf(v).Kind() {
		case reflect.Slice, reflect.Array:
			s := reflect.ValueOf(v)
			if s.Len() == 0 {
				continue
			}
			if s.Index(0).Kind() == reflect.Uint8 {
				add(s.Bytes())
				continue
			}
			for i := 0; i < s.Len(); i++ {
				switch s.Index(i).Kind() {
				case reflect.Map, reflect.Struct, reflect.Interface, reflect.Ptr:
				default:
					return fmt.Errorf("unsupported type %v", reflect.TypeOf(v).Kind())
				}
				data, err := json.Marshal(s.Index(i).Interface())
				if ulog.E(err) {
					return err
				}
				add(data)
			}
			continue
		case reflect.Map, reflect.Struct, reflect.Interface, reflect.Ptr:
		default:
			return fmt.Errorf("unsupported type %v", reflect.TypeOf(v).Kind())
		}
		data, err := json.Marshal(v)
		if ulog.E(err) {
			return err
		}
		add(data)
	}
	return nil
}

func marshalDocsGRPC(docs []interface{}) ([]*api.TigrisDBDoc, error) {
	res := make([]*api.TigrisDBDoc, 0, len(docs))
	err := marshalDocsGeneric(docs, func(doc []byte) {
		res = append(res, &api.TigrisDBDoc{Value: doc})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *grpcCRUDClient) Insert(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsGRPC(docs)
	if err != nil {
		return err
	}

	_, err = c.c.Insert(ctx, &api.TigrisDBCRUDRequest{
		Db:    c.db,
		Table: c.table,
		Docs:  bdocs,
	})

	return err
}

func (c *grpcCRUDClient) Delete(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsGRPC(docs)
	if err != nil {
		return err
	}

	_, err = c.c.Delete(ctx, &api.TigrisDBCRUDRequest{
		Db:    c.db,
		Table: c.table,
		Docs:  bdocs,
	})

	return err
}

func (c *grpcCRUDClient) Replace(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsGRPC(docs)
	if err != nil {
		return err
	}

	_, err = c.c.Replace(ctx, &api.TigrisDBCRUDRequest{
		Db:    c.db,
		Table: c.table,
		Docs:  bdocs,
	})

	return err
}

func (c *grpcCRUDClient) Update(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsGRPC(docs)
	if err != nil {
		return err
	}

	_, err = c.c.Update(ctx, &api.TigrisDBCRUDRequest{
		Db:    c.db,
		Table: c.table,
		Docs:  bdocs,
	})

	return err
}

func (c *grpcCRUDClient) Read(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsGRPC(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.Read(ctx, &api.TigrisDBCRUDRequest{
		Db:    c.db,
		Table: c.table,
		Docs:  bdocs,
	})

	i := 0
	for {
		d, err := resp.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = json.Unmarshal(d.Value, &docs[i])
		if err != nil {
			return err
		}
		i++
	}

	return nil
}

type httpClient struct {
	userHTTP.ClientWithResponsesInterface
}

type httpCRUDClient struct {
	c     userHTTP.ClientWithResponsesInterface
	db    string
	table string
}

func newHTTPClient(_ context.Context, host string, port int16) (*httpClient, error) {
	c, err := userHTTP.NewClientWithResponses(fmt.Sprintf("http://%s:%d", host, port))
	return &httpClient{c}, err
}

func (c *httpClient) Close() error {
	return nil
}

func HTTPError(err error, resp *http.Response) error {
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf(resp.Status)
	}
	return nil
}

func (c *httpClient) Create(ctx context.Context, db string, table string, key string) error {
	resp, err := c.TigrisDBCreateTableWithResponse(ctx, db, table, userHTTP.TigrisDBCreateTableJSONRequestBody{
		Key: aws.String(key),
	})
	return HTTPError(err, resp.HTTPResponse)
}

func (c *httpClient) Drop(ctx context.Context, db string, table string) error {
	resp, err := c.TigrisDBDropTableWithResponse(ctx, db, table, userHTTP.TigrisDBDropTableJSONRequestBody{})
	return HTTPError(err, resp.HTTPResponse)
}

func (c *httpClient) Use(db string, table string) crudClient {
	return &httpCRUDClient{c: c.ClientWithResponsesInterface, db: db, table: table}
}

func (c *httpClient) BeginTx() (txClient, error) {
	return nil, fmt.Errorf("not implemented")
}

func marshalDocsHTTP(docs []interface{}) ([]userHTTP.TigrisDBDoc, error) {
	res := make([]userHTTP.TigrisDBDoc, 0, len(docs))
	err := marshalDocsGeneric(docs, func(doc []byte) {
		res = append(res, userHTTP.TigrisDBDoc{Value: &doc})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *httpCRUDClient) Insert(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsHTTP(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.TigrisDBInsertWithResponse(ctx, c.db, c.table, userHTTP.TigrisDBInsertJSONRequestBody{
		Docs: &bdocs,
	})

	return HTTPError(err, resp.HTTPResponse)
}

func (c *httpCRUDClient) Delete(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsHTTP(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.TigrisDBDeleteWithResponse(ctx, c.db, c.table, userHTTP.TigrisDBDeleteJSONRequestBody{
		Docs: &bdocs,
	})

	return HTTPError(err, resp.HTTPResponse)
}

func (c *httpCRUDClient) Replace(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsHTTP(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.TigrisDBReplaceWithResponse(ctx, c.db, c.table, userHTTP.TigrisDBReplaceJSONRequestBody{
		Docs: &bdocs,
	})

	return HTTPError(err, resp.HTTPResponse)
}

func (c *httpCRUDClient) Update(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsHTTP(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.TigrisDBUpdateWithResponse(ctx, c.db, c.table, userHTTP.TigrisDBUpdateJSONRequestBody{
		Docs: &bdocs,
	})

	return HTTPError(err, resp.HTTPResponse)
}

func (c *httpCRUDClient) Read(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsHTTP(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.TigrisDBReadWithResponse(ctx, c.db, c.table, userHTTP.TigrisDBReadJSONRequestBody{
		Docs: &bdocs,
	})

	return HTTPError(err, resp.HTTPResponse)
}

type grpcAdminClient struct {
	api.IndexAPIClient
	conn *grpc.ClientConn
}

type grpcAdminClientIface interface {
	api.IndexAPIClient
	Close() error
}

func NewAdminGRPCClient(_ context.Context, host string, port int16) (grpcAdminClientIface, error) {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", host, port), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatal().Err(err).Msg("grpc connect failed")
	}

	return &grpcAdminClient{IndexAPIClient: api.NewIndexAPIClient(conn), conn: conn}, nil
}

func (c *grpcAdminClient) Close() error {
	return c.conn.Close()
}
