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

	"github.com/rs/zerolog/log"
	userHTTP "github.com/tigrisdata/tigrisdb/api/client/v1/user"
	api "github.com/tigrisdata/tigrisdb/api/server/v1"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type crudClient interface {
	Insert(ctx context.Context, docs ...interface{}) error
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

/*
func NewClient(ctx context.Context, host string, port int16) (client, error) {
	return newGRPCClient(ctx, host, port)
	//return newHTTPClient(ctx, host, port)
}
*/

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
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", host, port), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	if err != nil {
		log.Fatal().Err(err).Msg("grpc connect failed")
	}

	return &grpcClient{TigrisDBClient: api.NewTigrisDBClient(conn), conn: conn}, nil
}

func (c *grpcClient) Close() error {
	return c.conn.Close()
}

func (c *grpcClient) Create(ctx context.Context, db string, table string, _ string) error {
	_, err := c.CreateCollection(ctx, &api.CreateCollectionRequest{
		Db:         db,
		Collection: table,
	})
	return err
}

func (c *grpcClient) Drop(ctx context.Context, db string, table string) error {
	_, err := c.DropCollection(ctx, &api.DropCollectionRequest{
		Db:         db,
		Collection: table,
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

func marshalDocsGRPC(docs []interface{}) ([]*api.Document, error) {
	res := make([]*api.Document, 0, len(docs))
	err := marshalDocsGeneric(docs, func(doc []byte) {
		res = append(res, &api.Document{Doc: nil})
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

	_, err = c.c.Insert(ctx, &api.InsertRequest{
		Db:         c.db,
		Collection: c.table,
		Documents:  bdocs,
	})

	return err
}

func (c *grpcCRUDClient) Delete(ctx context.Context, docs ...interface{}) error {
	_, err := marshalDocsGRPC(docs)
	if err != nil {
		return err
	}

	_, err = c.c.Delete(ctx, &api.DeleteRequest{
		Db:         c.db,
		Collection: c.table,
	})

	return err
}

func (c *grpcCRUDClient) Replace(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsGRPC(docs)
	if err != nil {
		return err
	}

	_, err = c.c.Replace(ctx, &api.ReplaceRequest{
		Db:         c.db,
		Collection: c.table,
		Documents:  bdocs,
	})

	return err
}

func (c *grpcCRUDClient) Update(ctx context.Context, docs ...interface{}) error {
	_, err := marshalDocsGRPC(docs)
	if err != nil {
		return err
	}

	_, err = c.c.Update(ctx, &api.UpdateRequest{
		Db:         c.db,
		Collection: c.table,
	})

	return err
}

func (c *grpcCRUDClient) Read(ctx context.Context, docs ...interface{}) error {
	bdocs, err := marshalDocsGRPC(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.Read(ctx, &api.ReadRequest{
		Db:         c.db,
		Collection: c.table,
		Keys:       bdocs,
	})
	if err != nil {
		return err
	}

	i := 0
	for {
		_, err := resp.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		err = json.Unmarshal(nil, &docs[i])
		if err != nil {
			return err
		}
		i++
	}

	return nil
}

type httpClient struct {
	*userHTTP.ClientWithResponses
}

type httpCRUDClient struct {
	c     *userHTTP.ClientWithResponses
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

type httpStatus interface {
	StatusCode() int
	Status() string
}

func HTTPError(err error, resp httpStatus) error {
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusOK && resp.StatusCode() != http.StatusCreated {
		return fmt.Errorf(resp.Status())
	}
	return nil
}

func (c *httpClient) Create(ctx context.Context, db string, collection string, _ string) error {
	resp, err := c.TigrisDBCreateCollectionWithResponse(ctx, db, collection, userHTTP.TigrisDBCreateCollectionJSONRequestBody{})
	return HTTPError(err, resp)
}

func (c *httpClient) Drop(ctx context.Context, db string, table string) error {
	resp, err := c.TigrisDBDropCollectionWithResponse(ctx, db, table)
	return HTTPError(err, resp)
}

func (c *httpClient) Use(db string, table string) crudClient {
	return &httpCRUDClient{c: c.ClientWithResponses, db: db, table: table}
}

func (c *httpClient) BeginTx() (txClient, error) {
	return nil, fmt.Errorf("not implemented")
}

func marshalDocsHTTP(docs []interface{}) ([]userHTTP.Document, error) {
	res := make([]userHTTP.Document, 0, len(docs))
	err := marshalDocsGeneric(docs, func(doc []byte) {
		res = append(res, userHTTP.Document{Doc: nil})
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *httpCRUDClient) Insert(ctx context.Context, docs ...interface{}) error {
	_, err := marshalDocsHTTP(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.TigrisDBInsertWithResponse(ctx, c.db, c.table, userHTTP.TigrisDBInsertJSONRequestBody{})

	return HTTPError(err, resp)
}

func (c *httpCRUDClient) Delete(ctx context.Context, docs ...interface{}) error {
	_, err := marshalDocsHTTP(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.TigrisDBDeleteWithResponse(ctx, c.db, c.table, userHTTP.TigrisDBDeleteJSONRequestBody{})

	return HTTPError(err, resp)
}

func (c *httpCRUDClient) Replace(ctx context.Context, docs ...interface{}) error {
	_, err := marshalDocsHTTP(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.TigrisDBReplaceWithResponse(ctx, c.db, c.table, userHTTP.TigrisDBReplaceJSONRequestBody{})

	return HTTPError(err, resp)
}

func (c *httpCRUDClient) Update(ctx context.Context, docs ...interface{}) error {
	_, err := marshalDocsHTTP(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.TigrisDBUpdateWithResponse(ctx, c.db, c.table, userHTTP.TigrisDBUpdateJSONRequestBody{})

	return HTTPError(err, resp)
}

type ReadStruct struct {
	Result *api.ReadResponse
}

func (c *httpCRUDClient) Read(ctx context.Context, docs ...interface{}) error {
	_, err := marshalDocsHTTP(docs)
	if err != nil {
		return err
	}

	resp, err := c.c.TigrisDBRead(ctx, c.db, c.table, userHTTP.TigrisDBReadJSONRequestBody{})

	if ulog.E(err) {
		return err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf(resp.Status)
	}

	i := 0
	dec := json.NewDecoder(resp.Body)

	for dec.More() {
		td := ReadStruct{}
		if err := dec.Decode(&td); ulog.E(err) {
			return err
		}

		//FIXME: Can we unmarshall structpb directly to user struct?
		b, err := json.Marshal(td.Result.Doc)
		if ulog.E(err) {
			return err
		}
		err = json.Unmarshal(b, &docs[i])
		if ulog.E(err) {
			return err
		}
		i++
	}

	return nil
}
