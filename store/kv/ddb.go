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

package kv

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go/logging"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigrisdb/types"
	ulog "github.com/tigrisdata/tigrisdb/util/log"
)

// DynamodbConfig keeps DynamoDB configuration parameters
type DynamodbConfig struct {
	Region   string
	Endpoint string

	AccessKeyID     string `mapstructure:"access_key_id" yaml:"access_key_id" json:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key" yaml:"secret_access_key" json:"secret_access_key"`
	SessionToken    string `mapstructure:"session_token" yaml:"session_token" json:"session_token"`

	Timeout time.Duration
}

// ddb is an implementation of kv on top of DynamoDB
type ddb struct {
	svc *dynamodb.Client
}

type dbatch struct {
	d     *ddb
	batch *dynamodb.BatchWriteItemInput
}

type dtx struct {
	d  *ddb
	tx *dynamodb.TransactWriteItemsInput
}

// NewDynamoDB initializes instance of DynamoDB KV interface implementation
func NewDynamoDB(cfg *DynamodbConfig) (KV, error) {
	d := ddb{}
	if err := d.init(cfg); err != nil {
		return nil, err
	}
	return &d, nil
}

func (d *ddb) init(cfg *DynamodbConfig) error {
	endpointResolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		return aws.Endpoint{
			PartitionID:   "aws",
			URL:           cfg.Endpoint,
			SigningRegion: cfg.Region,
		}, nil
	})

	acfg, err := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithEndpointResolver(endpointResolver),
		awsconfig.WithRegion(cfg.Region),
		//	awsconfig.WithClientLogMode(aws.ClientLogMode(^uint64(0))),
		awsconfig.WithLogger(&awslog{}),
		//		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
	)
	if err != nil {
		return err
	}

	d.svc = dynamodb.NewFromConfig(acfg)

	return nil
}

func (d *ddb) readCond(ctx context.Context, table string, cond string, values map[string]dtypes.AttributeValue, limit int32) ([]Doc, error) {
	input := &dynamodb.QueryInput{
		TableName:                 &table,
		KeyConditionExpression:    &cond,
		ExpressionAttributeValues: values,
	}

	if limit != 0 {
		input.Limit = aws.Int32(limit)
	}

	resp, err := d.svc.Query(ctx, input)

	log.Err(err).Str("table", table).Str("cond", cond).Interface("values", values).Msg("Read")

	if err != nil {
		return nil, err
	}

	res := make([]Doc, 0, resp.Count)
	for _, v := range resp.Items {
		d := Doc{}

		primKey := make([]byte, 0)
		partKey := make([]byte, 0)

		// FIXME: This makes data copy
		if err = attributevalue.Unmarshal(v[PrimaryKey], &primKey); ulog.E(err) {
			return nil, err
		}
		if err = attributevalue.Unmarshal(v[PartitionKey], &partKey); ulog.E(err) {
			return nil, err
		}
		if err = attributevalue.Unmarshal(v[DataField], &d.Value); ulog.E(err) {
			return nil, err
		}

		d.Key = types.NewUserKey(primKey, partKey)

		res = append(res, d)
	}

	return res, err
}

// Read returns all the keys which has prefix equal to "key" parameter
func (d *ddb) Read(ctx context.Context, table string, key types.Key) ([]Doc, error) {
	cond := fmt.Sprintf("%s = :partKey AND begins_with (%s, :primKey)", PartitionKey, PrimaryKey)
	valCond := map[string]dtypes.AttributeValue{
		":partKey": &dtypes.AttributeValueMemberB{Value: key.Partition()},
		":primKey": &dtypes.AttributeValueMemberB{Value: key.Primary()},
	}
	return d.readCond(ctx, table, cond, valCond, 0)
}

// ReadRange reads range of the keys, both lkey and rkey should belong to the
// same partition.
// One of lkey or rkey may be nil in this case the bound is begin or end of the
// partition respectively.
func (d *ddb) ReadRange(ctx context.Context, table string, partitionKey []byte, lkey types.Key, rkey types.Key, limit int) ([]Doc, error) {
	var lcond, rcond string
	values := map[string]dtypes.AttributeValue{}

	if lkey != nil {
		if partitionKey != nil && bytes.Compare(partitionKey, lkey.Partition()) != 0 {
			return nil, fmt.Errorf("both bounds should belong to the same partition")
		}
		partitionKey = lkey.Partition()

		lcond = fmt.Sprintf(" AND %v >= :lbound", PrimaryKey)
		values[":lbound"] = &dtypes.AttributeValueMemberB{Value: lkey.Primary()}
	}

	if rkey != nil {
		if partitionKey != nil && bytes.Compare(partitionKey, rkey.Partition()) != 0 {
			return nil, fmt.Errorf("both bounds should belong to the same partition")
		}
		partitionKey = rkey.Partition()

		rcond = fmt.Sprintf(" AND %v < :rbound", PrimaryKey)
		values[":rbound"] = &dtypes.AttributeValueMemberB{Value: rkey.Primary()}
	}

	if partitionKey == nil {
		return nil, fmt.Errorf("partiton key or at least one bound of the range should be provided")
	}

	cond := fmt.Sprintf("%v = :partKey", PartitionKey)
	values[":partKey"] = &dtypes.AttributeValueMemberB{Value: partitionKey}

	return d.readCond(ctx, table, cond+lcond+rcond, values, int32(limit))
}

func (d *ddb) Replace(ctx context.Context, table string, key types.Key, data []byte) error {
	m := appendDBKey(nil, key)

	m[DataField] = &dtypes.AttributeValueMemberB{Value: data}

	_, err := d.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: &table,
		Item:      m,
	})

	log.Err(err).Str("table", table).Stringer("key", key).Msg("Upsert")

	return err
}

func (d *ddb) Insert(ctx context.Context, table string, key types.Key, data []byte) error {
	m := appendDBKey(nil, key)

	m[DataField] = &dtypes.AttributeValueMemberB{Value: data}

	_, err := d.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           &table,
		Item:                m,
		ConditionExpression: aws.String("attribute_not_exists(" + PrimaryKey + ")"),
	})

	log.Err(err).Str("table", table).Stringer("key", key).Msg("Insert")

	return err
}

func (d *ddb) Update(ctx context.Context, table string, key types.Key, data []byte) error {
	m := appendDBKey(nil, key)

	m[DataField] = &dtypes.AttributeValueMemberB{Value: data}

	_, err := d.svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           &table,
		Item:                m,
		ConditionExpression: aws.String("attribute_exists(" + PrimaryKey + ")"),
	})

	log.Err(err).Str("table", table).Stringer("key", key).Msg("Update")

	return err
}

func (d *ddb) Delete(ctx context.Context, table string, key types.Key) error {
	_, err := d.svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: &table,
		Key:       appendDBKey(nil, key),
	})

	log.Err(err).Str("table", table).Stringer("key", key).Msg("Delete")

	return err
}

func (d *ddb) CreateTable(ctx context.Context, name string) error {
	params := dynamodb.CreateTableInput{
		TableName:   &name,
		BillingMode: dtypes.BillingModePayPerRequest,
		AttributeDefinitions: []dtypes.AttributeDefinition{
			{
				AttributeName: &PartitionKey,
				AttributeType: "B",
			},
			{
				AttributeName: &PrimaryKey,
				AttributeType: "B",
			},
		},
		KeySchema: []dtypes.KeySchemaElement{
			{
				AttributeName: &PartitionKey,
				KeyType:       "HASH",
			},
			{
				AttributeName: &PrimaryKey,
				KeyType:       "RANGE",
			},
		},
	}

	_, err := d.svc.CreateTable(ctx, &params)
	if err != nil {
		return err
	}

	log.Debug().Str("name", name).Msg("Table created")

	return nil
}

func (d *ddb) DropTable(ctx context.Context, name string) error {
	params := dynamodb.DeleteTableInput{
		TableName: &name,
	}

	if _, err := d.svc.DeleteTable(ctx, &params); err != nil {
		return err
	}

	log.Debug().Str("name", name).Msg("Table dropped")

	return nil
}

func (d *ddb) Batch() Tx {
	b := &dbatch{d: d}
	b.batch = &dynamodb.BatchWriteItemInput{RequestItems: make(map[string][]dtypes.WriteRequest)}
	return b
}

func (b *dbatch) Replace(ctx context.Context, table string, key types.Key, data []byte) error {
	m := appendDBKey(nil, key)

	m[DataField] = &dtypes.AttributeValueMemberB{Value: data}

	b.batch.RequestItems[table] = append(b.batch.RequestItems[table], dtypes.WriteRequest{PutRequest: &dtypes.PutRequest{
		Item: m,
	}})

	log.Debug().Str("table", table).Stringer("key", key).Msg("Batch replace")

	return nil
}

func (b *dbatch) Delete(ctx context.Context, table string, key types.Key) error {
	b.batch.RequestItems[table] = append(b.batch.RequestItems[table], dtypes.WriteRequest{DeleteRequest: &dtypes.DeleteRequest{
		Key: appendDBKey(nil, key),
	}})

	log.Debug().Str("table", table).Stringer("key", key).Msg("Batch delete")

	return nil
}

func (b *dbatch) Commit(ctx context.Context) error {
	if len(b.batch.RequestItems) == 0 {
		return nil
	}

	_, err := b.d.svc.BatchWriteItem(ctx, b.batch)

	log.Err(err).Msg("Batch commit")

	return err
}

func (b *dbatch) Rollback(_ context.Context) error {
	b.batch.RequestItems = make(map[string][]dtypes.WriteRequest)

	log.Debug().Msg("Batch rollback")

	return nil
}

func (d *ddb) Tx() Tx {
	t := &dtx{d: d}
	return t
}

func (t *dtx) Replace(ctx context.Context, table string, key types.Key, data []byte) error {
	m := appendDBKey(nil, key)

	m[DataField] = &dtypes.AttributeValueMemberB{Value: data}

	t.tx.TransactItems = append(t.tx.TransactItems, dtypes.TransactWriteItem{
		Put: &dtypes.Put{TableName: &table, Item: m},
	})

	log.Debug().Str("table", table).Stringer("key", key).Msg("Tx Upsert")

	return nil
}

/*
func (t *dtx) Update(ctx context.Context, table string, key types.Key, data []byte) error {
	m := appendDBKey(nil, key)

	m[DataField] = &dtypes.AttributeValueMemberB{Value: data}
	//	attributevalue.MarshalMap()

	t.tx.TransactItems = append(t.tx.TransactItems, dtypes.TransactWriteItem{
		Put: &dtypes.Put{TableName: &table, Item: m},
	})

	log.Debug().Str("table", table).Stringer("key", key).Msg("Tx Upsert")

	return nil
}
*/

func (t *dtx) Delete(ctx context.Context, table string, key types.Key) error {
	t.tx.TransactItems = append(t.tx.TransactItems, dtypes.TransactWriteItem{
		Delete: &dtypes.Delete{TableName: &table, Key: appendDBKey(nil, key)},
	})

	log.Debug().Str("table", table).Stringer("key", key).Msg("Tx Delete")

	return nil
}

func (t *dtx) Commit(ctx context.Context) error {
	if len(t.tx.TransactItems) == 0 {
		return nil
	}

	_, err := t.d.svc.TransactWriteItems(ctx, t.tx)

	log.Err(err).Msg("Tx Commit")

	return err
}

func (t *dtx) Rollback(_ context.Context) error {
	t.tx.TransactItems = make([]dtypes.TransactWriteItem, 0)

	log.Debug().Msg("Tx Rollback")

	return nil
}

func appendDBKey(m map[string]dtypes.AttributeValue, k types.Key) map[string]dtypes.AttributeValue {
	if m == nil {
		m = make(map[string]dtypes.AttributeValue)
	}

	m[PartitionKey] = &dtypes.AttributeValueMemberB{Value: k.Partition()}
	m[PrimaryKey] = &dtypes.AttributeValueMemberB{Value: k.Primary()}

	return m
}

type awslog struct {
}

func (a *awslog) Logf(c logging.Classification, format string, v ...interface{}) {
	if c == logging.Warn {
		log.Warn().Msgf(format, v...)
	}
	log.Debug().CallerSkipFrame(3).Msgf(format, v...)
}
