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

package metadata

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata/encoding"
	"github.com/tigrisdata/tigris/server/transaction"
	ulog "github.com/tigrisdata/tigris/util/log"
	"google.golang.org/grpc/codes"
)

type NamespaceType string

const (
	// DefaultNamespaceName is for "default" namespace in the cluster which means all the databases created are under a single
	// namespace.
	// It is totally fine for a deployment to choose this and just have one namespace. The default assigned value for
	// this namespace is 1.
	DefaultNamespaceName string = "default_namespace"

	DefaultNamespaceId = uint32(1)
)

const (
	baseSchemaVersion = 1
)

// A Namespace is a logical grouping of databases.
type Namespace interface {
	// Id for the namespace is used by the cluster to append as the first element in the key.
	Id() uint32
	// Name is the name used for the lookup.
	Name() string
}

// DefaultNamespace is for "default" namespace in the cluster. This is useful when there is no need to logically group
// databases. All databases will be created under a single namespace. It is totally fine for a deployment to choose this
// and just have one namespace. The default assigned value for this namespace is 1.
type DefaultNamespace struct{}

func (n *DefaultNamespace) Name() string {
	return DefaultNamespaceName
}

// Id returns id assigned to the namespace
func (n *DefaultNamespace) Id() uint32 {
	return DefaultNamespaceId
}

func NewDefaultNamespace() *DefaultNamespace {
	return &DefaultNamespace{}
}

// TenantNamespace is used when there is a finer isolation of databases is needed. The caller provides a unique
// name and unique id to this namespace which is used by the cluster to create a namespace.
type TenantNamespace struct {
	lookupName string
	lookupId   uint32
}

func NewTenantNamespace(name string, id uint32) *TenantNamespace {
	return &TenantNamespace{
		lookupName: name,
		lookupId:   id,
	}
}

func (n *TenantNamespace) Name() string {
	return n.lookupName
}

// Id returns assigned id for the namespace
func (n *TenantNamespace) Id() uint32 {
	return n.lookupId
}

// TenantManager is to manage all the tenants
// ToDo: start a background thread to reload the mapping
type TenantManager struct {
	sync.RWMutex

	encoder *encoding.DictionaryEncoder
	tenants map[string]*Tenant
}

func NewTenantManager() *TenantManager {
	return &TenantManager{
		encoder: encoding.NewDictionaryEncoder(),
		tenants: make(map[string]*Tenant),
	}
}

// CreateOrGetTenant is a thread safe implementation of creating a new tenant. It returns the tenant if it already exists.
// This is mainly returning the tenant to avoid calling "Get" again after creating the tenant. This method is expensive
// as it reloads the existing tenants from the disk if it sees the tenant is not present in the cache.
func (m *TenantManager) CreateOrGetTenant(ctx context.Context, tx transaction.Tx, namespace Namespace) (*Tenant, error) {
	m.Lock()
	defer m.Unlock()

	tenant, ok := m.tenants[namespace.Name()]
	if ok {
		if tenant.namespace.Id() == namespace.Id() {
			// tenant was present
			return tenant, nil
		} else {
			return nil, api.Errorf(codes.InvalidArgument, "id is already assigned to '%s'", tenant.namespace.Name())
		}
	}

	if err := m.reload(ctx, tx); ulog.E(err) {
		// first reload
		return nil, err
	}
	if tenant, ok := m.tenants[namespace.Name()]; ok {
		// if it is created by some other thread in parallel then we should see it now.
		return tenant, nil
	}

	if err := m.encoder.ReserveNamespace(ctx, tx, namespace.Name(), namespace.Id()); ulog.E(err) {
		return nil, err
	}

	tenant = NewTenant(namespace, m.encoder)
	m.tenants[namespace.Name()] = tenant
	return tenant, nil
}

// GetTenant returns tenants if exists in the tenant map or nil.
func (m *TenantManager) GetTenant(namespace string) *Tenant {
	m.RLock()
	defer m.RUnlock()

	// ToDo: add a mechanism to reload on version changed.
	return m.tenants[namespace]
}

// Reload reads all the namespaces exists in the disk and build the in-memory map of the manager to track the tenants.
// As this is an expensive call, the reloading happens during start time or in background. It is possible that reloading
// fails during start time then we rely on background thread to reload all the mapping.
func (m *TenantManager) Reload(ctx context.Context, tx transaction.Tx) error {
	log.Debug().Msg("reloading tenants")
	m.Lock()
	defer m.Unlock()

	return m.reload(ctx, tx)
}

func (m *TenantManager) reload(ctx context.Context, tx transaction.Tx) error {
	namespaces, err := m.encoder.GetNamespaces(ctx, tx)
	if err != nil {
		return err
	}
	log.Debug().Interface("ns", namespaces).Msg("existing namespaces")

	for namespace, id := range namespaces {
		if _, ok := m.tenants[namespace]; !ok {
			m.tenants[namespace] = NewTenant(NewTenantNamespace(namespace, id), m.encoder)
		}
	}

	for _, tenant := range m.tenants {
		log.Debug().Interface("tenant", tenant.String()).Msg("reloading tenant")
		if err := tenant.reload(ctx, tx); err != nil {
			return err
		}
	}
	return nil
}

// Tenant is a logical grouping of databases. The tenant is used to manage all the databases that belongs to this tenant
// and the corresponding collections for these databases. Operations performed on the tenant object are thread-safe.
type Tenant struct {
	sync.RWMutex

	encoder     *encoding.DictionaryEncoder
	schemaStore *encoding.SchemaSubspace
	databases   map[string]*Database
	namespace   Namespace
}

func NewTenant(namespace Namespace, encoder *encoding.DictionaryEncoder) *Tenant {
	return &Tenant{
		namespace:   namespace,
		encoder:     encoder,
		schemaStore: &encoding.SchemaSubspace{},
		databases:   make(map[string]*Database),
	}
}

// reload will reload all the databases for this tenant. This is only reloading all the databases that exists in the disk.
// Loading all corresponding collections in this call is unnecessary and will be expensive. Rather that can happen
// during reloadDatabase API call.
func (tenant *Tenant) reload(ctx context.Context, tx transaction.Tx) error {
	tenant.Lock()
	defer tenant.Unlock()

	dbNameToId, err := tenant.encoder.GetDatabases(ctx, tx, tenant.namespace.Id())
	if err != nil {
		return err
	}

	for db, id := range dbNameToId {
		if _, ok := tenant.databases[db]; !ok {
			tenant.databases[db] = &Database{
				name:        db,
				id:          id,
				collections: make(map[string]*collectionHolder),
			}
		}
	}
	for _, db := range tenant.databases {
		if e := tenant.reloadDatabase(ctx, tx, db.name, db.id); e != nil {
			err = multierror.Append(err, e)
			continue
		}
	}

	return err
}

func (tenant *Tenant) GetNamespace() Namespace {
	tenant.RLock()
	defer tenant.RUnlock()

	return tenant.namespace
}

// CreateDatabase is responsible for first creating a dictionary encoding of the database and then adding an entry for
// this database in the tenant object.
func (tenant *Tenant) CreateDatabase(ctx context.Context, tx transaction.Tx, dbName string) error {
	tenant.Lock()
	defer tenant.Unlock()

	if _, ok := tenant.databases[dbName]; ok {
		log.Debug().Str("db", dbName).Msg("already exist, ignoring create request")
		return nil
	}

	// if there are concurrent requests on different workers then one of them will fail with duplicate entry and only
	// one will succeed.
	id, err := tenant.encoder.EncodeDatabaseName(ctx, tx, dbName, tenant.namespace.Id())
	if err != nil {
		return err
	}

	tenant.databases[dbName] = &Database{
		id:          id,
		name:        dbName,
		collections: make(map[string]*collectionHolder),
	}

	return nil
}

// DropDatabase is responsible for first dropping a dictionary encoding of the database and then adding a corresponding
// dropped encoding in the table.
func (tenant *Tenant) DropDatabase(ctx context.Context, tx transaction.Tx, dbName string) error {
	// reloading
	_, err := tenant.GetDatabase(ctx, tx, dbName)
	if err != nil {
		return err
	}

	tenant.Lock()
	defer tenant.Unlock()

	db, ok := tenant.databases[dbName]
	if !ok {
		return nil
	}

	// if there are concurrent requests on different workers then one of them will fail with duplicate entry and only
	// one will succeed.
	if err = tenant.encoder.EncodeDatabaseAsDropped(ctx, tx, dbName, tenant.namespace.Id(), db.id); err != nil {
		return err
	}

	for _, c := range db.collections {
		if err := tenant.dropCollection(ctx, tx, db, c.collection.Name); err != nil {
			return err
		}
	}

	delete(tenant.databases, db.name)

	return nil
}

func (tenant *Tenant) InvalidateDBCache(dbName string) {
	tenant.Lock()
	defer tenant.Unlock()

	delete(tenant.databases, dbName)
}

// GetDatabase returns the database object, or null if there is no database exist with the name passed in the param.
// This API is also responsible for reloading the tenant knowledge of the databases to ensure caller sees a consistent
// view of all the schemas. This is achieved by first checking the meta version and if it is changed then call reload.
func (tenant *Tenant) GetDatabase(ctx context.Context, tx transaction.Tx, dbName string) (*Database, error) {
	tenant.Lock()
	defer tenant.Unlock()

	// ToDo: if version has changed then reload
	if _, ok := tenant.databases[dbName]; !ok {
		id, err := tenant.encoder.GetDatabaseId(ctx, tx, dbName, tenant.namespace.Id())
		if ulog.E(err) {
			return nil, err
		}
		if id == encoding.InvalidId {
			// nothing found
			return nil, nil
		}

		if err := tenant.reloadDatabase(ctx, tx, dbName, id); ulog.E(err) {
			// this will be treated as not found because later attempts should fix it.
			return nil, err
		}
	}

	return tenant.databases[dbName], nil
}

func (tenant *Tenant) ListDatabases(ctx context.Context, tx transaction.Tx) []string {
	tenant.RLock()
	defer tenant.RUnlock()

	var databases []string
	for dbName := range tenant.databases {
		databases = append(databases, dbName)
	}

	return databases
}

func (tenant *Tenant) reloadDatabase(ctx context.Context, tx transaction.Tx, dbName string, dbId uint32) error {
	dbObj := &Database{
		id:          dbId,
		name:        dbName,
		collections: make(map[string]*collectionHolder),
	}
	tenant.databases[dbName] = dbObj

	collNameToId, err := tenant.encoder.GetCollections(ctx, tx, tenant.namespace.Id(), dbObj.id)
	if err != nil {
		return err
	}

	for coll, id := range collNameToId {
		idxNameToId, e := tenant.encoder.GetIndexes(ctx, tx, tenant.namespace.Id(), dbObj.id, id)
		if e != nil {
			err = multierror.Append(err, e)
			continue
		}

		userSch, _, e := tenant.schemaStore.GetLatest(ctx, tx, tenant.namespace.Id(), dbObj.id, id)
		if e != nil {
			err = multierror.Append(err, e)
			continue
		}
		dbObj.collections[coll] = &collectionHolder{
			name:        coll,
			id:          id,
			idxNameToId: idxNameToId,
		}

		if e = dbObj.collections[coll].set(userSch); e != nil {
			err = multierror.Append(err, e)
			continue
		}
	}

	return err
}

// CreateCollection is to create a collection inside tenant namespace.
func (tenant *Tenant) CreateCollection(ctx context.Context, tx transaction.Tx, dbObj *Database, schFactory *schema.Factory) error {
	tenant.Lock()
	defer tenant.Unlock()

	if dbObj == nil {
		return api.Errorf(codes.NotFound, "database missing")
	}
	if c, ok := dbObj.collections[schFactory.CollectionName]; ok {
		if !c.isFullyFormed() {
			// if it is not formed fully first time then reload schema and try building collection again
			userSch, _, err := tenant.schemaStore.GetLatest(ctx, tx, tenant.namespace.Id(), dbObj.id, c.id)
			if err != nil {
				return err
			}

			if err := c.set(userSch); err != nil {
				return errors.Wrap(err, "previous incompatible version detected, drop collection first")
			}
		}

		// if collection already exists, check if it is same or different schema
		equal, err := JSONSchemaEqual(c.schema, schFactory.Schema)
		if err != nil {
			return err
		}
		if !equal {
			return api.Errorf(codes.InvalidArgument, "schema changes are not supported")
		}
		return nil
	}

	collectionId, err := tenant.encoder.EncodeCollectionName(ctx, tx, schFactory.CollectionName, tenant.namespace.Id(), dbObj.id)
	if err != nil {
		return err
	}

	// encode indexes and add this back in the collection
	indexes := schFactory.Indexes.GetIndexes()
	idxNameToId := make(map[string]uint32)
	for _, i := range indexes {
		id, err := tenant.encoder.EncodeIndexName(ctx, tx, i.Name, tenant.namespace.Id(), dbObj.id, collectionId)
		if err != nil {
			return err
		}
		i.Id = id
		idxNameToId[i.Name] = id
	}

	// all good now persist the schema
	if err := tenant.schemaStore.Put(ctx, tx, tenant.namespace.Id(), dbObj.id, collectionId, schFactory.Schema, baseSchemaVersion); err != nil {
		return err
	}

	// store the collection to the databaseObject
	dbObj.collections[schFactory.CollectionName] = &collectionHolder{
		id:          collectionId,
		idxNameToId: idxNameToId,
		name:        schFactory.CollectionName,
		schema:      schFactory.Schema,
		collection:  schema.NewDefaultCollection(schFactory.CollectionName, collectionId, schFactory.Fields, schFactory.Indexes, schFactory.Schema),
	}

	return nil
}

func JSONSchemaEqual(s1, s2 []byte) (bool, error) {
	var j, j2 interface{}
	if err := jsoniter.Unmarshal(s1, &j); err != nil {
		return false, err
	}
	if err := jsoniter.Unmarshal(s2, &j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j2, j), nil
}

// DropCollection is to drop a collection and its associated indexes. It removes the "created" entry from the encoding
// subspace and adds a "dropped" entry for the same collection key.
func (tenant *Tenant) DropCollection(ctx context.Context, tx transaction.Tx, db *Database, collectionName string) error {
	tenant.Lock()
	defer tenant.Unlock()

	return tenant.dropCollection(ctx, tx, db, collectionName)
}

func (tenant *Tenant) dropCollection(ctx context.Context, tx transaction.Tx, db *Database, collectionName string) error {
	if db == nil {
		return api.Errorf(codes.NotFound, "database missing")
	}

	cHolder, ok := db.collections[collectionName]
	if !ok {
		return api.Errorf(codes.NotFound, "collection doesn't exists '%s'", collectionName)
	}

	if err := tenant.encoder.EncodeCollectionAsDropped(ctx, tx, cHolder.name, tenant.namespace.Id(), db.id, cHolder.id); err != nil {
		return err
	}

	for idxName, idxId := range cHolder.idxNameToId {
		if err := tenant.encoder.EncodeIndexAsDropped(ctx, tx, idxName, tenant.namespace.Id(), db.id, cHolder.id, idxId); err != nil {
			return err
		}
	}
	if err := tenant.schemaStore.Delete(ctx, tx, tenant.namespace.Id(), db.id, cHolder.id); err != nil {
		return err
	}

	delete(db.collections, cHolder.name)

	return nil
}

func (tenant *Tenant) String() string {
	return fmt.Sprintf("id: %d, name: %s", tenant.namespace.Id(), tenant.namespace.Name())
}

// Database is to manage the collections for this database. Check the Clone method before changing this struct.
type Database struct {
	sync.RWMutex

	id          uint32
	name        string
	collections map[string]*collectionHolder
}

// Clone is used to stage the database
func (d *Database) Clone() *Database {
	d.RLock()
	defer d.RUnlock()

	var copy Database
	copy.id = d.id
	copy.name = d.name
	copy.collections = make(map[string]*collectionHolder)
	for k, v := range d.collections {
		copy.collections[k] = v.clone()
	}

	return &copy
}

// Name returns the database name.
func (d *Database) Name() string {
	return d.name
}

// Id returns the dictionary encoded value of this collection.
func (d *Database) Id() uint32 {
	return d.id
}

// ListCollection returns the collection object of all the collections in this database.
func (d *Database) ListCollection() []*schema.DefaultCollection {
	d.RLock()
	defer d.RUnlock()

	var collections []*schema.DefaultCollection
	for _, c := range d.collections {
		collections = append(collections, c.collection)
	}
	return collections
}

// GetCollection returns the collection object, or null if the collection map contains no mapping for the database. At
// this point collection is fully formed and safe to use.
func (d *Database) GetCollection(cname string) *schema.DefaultCollection {
	d.RLock()
	defer d.RUnlock()

	if holder := d.collections[cname]; holder != nil {
		return holder.get()
	}

	return nil
}

// collectionHolder is to manage a single collection. Check the Clone method before changing this struct.
type collectionHolder struct {
	sync.RWMutex

	// id is the dictionary encoded value of this collection
	id uint32
	// name of the collection
	name string
	// collection
	collection *schema.DefaultCollection
	// idxNameToId is a map storing dictionary encoding values of all the indexes that are part of this collection.
	idxNameToId map[string]uint32
	// latest schema
	schema jsoniter.RawMessage
	// if collection is not properly formed during restart then this is false and we need to reload it during request
	fullFormed bool
}

// clone is used to stage the collectionHolder
func (c *collectionHolder) clone() *collectionHolder {
	c.RLock()
	defer c.RUnlock()

	var copy collectionHolder
	copy.id = c.id
	copy.name = c.name
	copy.schema = c.schema

	copy.collection, _ = c.createCollection(c.schema)
	copy.idxNameToId = make(map[string]uint32)
	for k, v := range c.idxNameToId {
		copy.idxNameToId[k] = v
	}

	return &copy
}

// get returns the collection managed by this holder. At this point, a Collection object is safely constructed
// with all encoded values assigned to all the attributed i.e. collection, index has assigned the encoded
// values.
func (c *collectionHolder) get() *schema.DefaultCollection {
	c.RLock()
	defer c.RUnlock()

	return c.collection
}

// set recreates the collection object from the schema fetched from the disk. First it recreates the schema factory
// from the schema, then it uses idxNameToId map to assign dictionary encoded values to the indexes and finally create
// the collection. This API is responsible for setting the dictionary values to the collection and to the corresponding
// indexes.
func (c *collectionHolder) set(revision []byte) error {
	c.Lock()
	defer c.Unlock()

	collection, err := c.createCollection(revision)
	if err != nil {
		return err
	}
	c.collection = collection
	c.schema = revision
	c.fullFormed = true
	return nil
}

func (c *collectionHolder) createCollection(revision []byte) (*schema.DefaultCollection, error) {
	schFactory, err := schema.Build(c.name, revision)
	if err != nil {
		return nil, err
	}

	indexes := schFactory.Indexes.GetIndexes()
	for _, index := range indexes {
		id, ok := c.idxNameToId[index.Name]
		if !ok {
			return nil, api.Errorf(codes.NotFound, "dictionary encoding is missing for index '%s'", index.Name)
		}
		index.Id = id
	}

	return schema.NewDefaultCollection(c.name, c.id, schFactory.Fields, schFactory.Indexes, revision), nil
}

func (c *collectionHolder) isFullyFormed() bool {
	c.RLock()
	defer c.RUnlock()

	return c.fullFormed
}
