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
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata/encoding"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
	tsApi "github.com/typesense/typesense-go/typesense/api"
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

	encoder        *encoding.DictionaryEncoder
	schemaStore    *encoding.SchemaSubspace
	tenants        map[string]*Tenant
	idToTenantMap  map[uint32]string
	version        Version
	versionH       *VersionHandler
	mdNameRegistry encoding.MDNameRegistry
}

func NewTenantManager() *TenantManager {
	mdNameRegistry := &encoding.DefaultMDNameRegistry{}
	return newTenantManager(mdNameRegistry)
}

func newTenantManager(mdNameRegistry encoding.MDNameRegistry) *TenantManager {
	return &TenantManager{
		encoder:        encoding.NewDictionaryEncoder(mdNameRegistry),
		schemaStore:    encoding.NewSchemaStore(mdNameRegistry),
		tenants:        make(map[string]*Tenant),
		idToTenantMap:  make(map[uint32]string),
		versionH:       &VersionHandler{},
		mdNameRegistry: mdNameRegistry,
	}
}

// CreateOrGetTenant is a thread safe implementation of creating a new tenant. It returns the tenant if it already exists.
// This is mainly returning the tenant to avoid calling "Get" again after creating the tenant. This method is expensive
// as it reloads the existing tenants from the disk if it sees the tenant is not present in the cache.
func (m *TenantManager) CreateOrGetTenant(ctx context.Context, txMgr *transaction.Manager, namespace Namespace) (tenant *Tenant, err error) {
	m.Lock()
	defer m.Unlock()

	var ok bool
	if tenant, ok = m.tenants[namespace.Name()]; ok {
		if tenant.namespace.Id() == namespace.Id() {
			// tenant was present
			log.Debug().Str("ns", tenant.String()).Msg("tenant found")
			return tenant, nil
		} else {
			return nil, api.Errorf(api.Code_INVALID_ARGUMENT, "id is already assigned to '%s'", tenant.namespace.Name())
		}
	}

	tx, e := txMgr.StartTx(ctx)
	if ulog.E(e) {
		return nil, e
	}

	defer func() {
		if err == nil {
			if err = tx.Commit(ctx); err == nil {
				// commit succeed, so we can safely cache it now, for other workers it may happen as part of the
				// first call in query lifecycle
				m.tenants[namespace.Name()] = tenant
				m.idToTenantMap[namespace.Id()] = namespace.Name()
			}
		} else {
			_ = tx.Rollback(ctx)
		}
	}()

	return m.createOrGetTenantInternal(ctx, tx, namespace)
}

// CreateTenant is a thread safe implementation of creating a new tenant. It returns the error if it already exists.
func (m *TenantManager) CreateTenant(ctx context.Context, tx transaction.Tx, namespace Namespace) error {
	m.Lock()
	defer m.Unlock()
	namespaces, err := m.encoder.GetNamespaces(ctx, tx)
	if err != nil {
		return err
	}

	if id, found := namespaces[namespace.Name()]; found {
		return api.Errorf(api.Code_CONFLICT, "namespace with same name already exists with id '%s'", fmt.Sprint(id))
	}
	for name, id := range namespaces {
		if id == namespace.Id() {
			return api.Errorf(api.Code_CONFLICT, "namespace with same id already exists with name '%s'", name)
		}
	}
	if err := m.versionH.Increment(ctx, tx); ulog.E(err) {
		return err
	}

	if err := m.encoder.ReserveNamespace(ctx, tx, namespace.Name(), namespace.Id()); ulog.E(err) {
		return err
	}

	return nil
}

func (m *TenantManager) createOrGetTenantInternal(ctx context.Context, tx transaction.Tx, namespace Namespace) (*Tenant, error) {
	namespaces, err := m.encoder.GetNamespaces(ctx, tx)
	if err != nil {
		return nil, err
	}
	log.Debug().Interface("ns", namespaces).Msg("existing namespaces")
	if _, ok := namespaces[namespace.Name()]; ok {
		// only read the version if tenant already exists otherwise, we need to increment it.
		currentVersion, err := m.versionH.Read(ctx, tx)
		if ulog.E(err) {
			return nil, err
		}

		tenant := NewTenant(namespace, m.encoder, m.schemaStore, m.versionH, currentVersion)
		tenant.Lock()
		err = tenant.reload(ctx, tx, currentVersion)
		tenant.Unlock()
		return tenant, err
	}

	log.Debug().Str("tenant", namespace.Name()).Msg("tenant not found, creating")

	// bump the version first
	if err := m.versionH.Increment(ctx, tx); ulog.E(err) {
		return nil, err
	}

	if err := m.encoder.ReserveNamespace(ctx, tx, namespace.Name(), namespace.Id()); ulog.E(err) {
		return nil, err
	}

	return NewTenant(namespace, m.encoder, m.schemaStore, m.versionH, nil), nil
}

// GetTableNameFromId returns tenant name, database name, collection name corresponding to their encoded ids.
func (m *TenantManager) GetTableNameFromId(tenantId uint32, dbId uint32, collId uint32) (string, string, string, bool) {
	m.RLock()
	defer m.RUnlock()

	// get tenant info
	tenantName, ok := m.idToTenantMap[tenantId]
	if !ok {
		return "", "", "", ok
	}
	tenant, ok := m.tenants[tenantName]
	if !ok {
		return "", "", "", ok
	}

	// get db info
	dbName, ok := tenant.idToDatabaseMap[dbId]
	if !ok {
		return tenantName, "", "", ok
	}
	db, ok := tenant.databases[dbName]
	if !ok {
		return tenantName, "", "", ok
	}

	// finally, the collection
	collName, ok := db.idToCollectionMap[collId]
	if !ok {
		return tenantName, dbName, "", ok
	}
	return tenantName, dbName, collName, ok
}

// Reload reads all the namespaces exists in the disk and build the in-memory map of the manager to track the tenants.
// As this is an expensive call, the reloading happens during start time for now. It is possible that reloading
// fails during start time then we rely on lazily reloading cache during serving user requests.
func (m *TenantManager) Reload(ctx context.Context, tx transaction.Tx) error {
	log.Debug().Msg("reloading tenants")
	m.Lock()
	defer m.Unlock()

	currentVersion, err := m.versionH.Read(ctx, tx)
	if ulog.E(err) {
		return err
	}

	if err = m.reload(ctx, tx, currentVersion); ulog.E(err) {
		return err
	}
	m.version = currentVersion
	log.Debug().Msgf("latest meta version %v", m.version)
	return err
}

func (m *TenantManager) reload(ctx context.Context, tx transaction.Tx, currentVersion Version) error {
	namespaces, err := m.encoder.GetNamespaces(ctx, tx)
	if err != nil {
		return err
	}
	log.Debug().Interface("ns", namespaces).Msg("existing reserved namespaces")

	for namespace, id := range namespaces {
		if _, ok := m.tenants[namespace]; !ok {
			m.tenants[namespace] = NewTenant(NewTenantNamespace(namespace, id), m.encoder, m.schemaStore, m.versionH, currentVersion)
			m.idToTenantMap[id] = namespace
		}
	}

	for _, tenant := range m.tenants {
		log.Debug().Interface("tenant", tenant.String()).Msg("reloading tenant")
		tenant.Lock()
		err := tenant.reload(ctx, tx, currentVersion)
		tenant.Unlock()
		if err != nil {
			return err
		}
	}
	return nil
}

// Tenant is a logical grouping of databases. The tenant is used to manage all the databases that belongs to this tenant
// and the corresponding collections for these databases. Operations performed on the tenant object are thread-safe.
type Tenant struct {
	sync.RWMutex

	encoder         *encoding.DictionaryEncoder
	schemaStore     *encoding.SchemaSubspace
	databases       map[string]*Database
	idToDatabaseMap map[uint32]string
	namespace       Namespace
	version         Version
	versionH        *VersionHandler
}

func NewTenant(namespace Namespace, encoder *encoding.DictionaryEncoder, schemaStore *encoding.SchemaSubspace, versionH *VersionHandler, currentVersion Version) *Tenant {
	return &Tenant{
		namespace:       namespace,
		encoder:         encoder,
		schemaStore:     schemaStore,
		databases:       make(map[string]*Database),
		idToDatabaseMap: make(map[uint32]string),
		versionH:        versionH,
		version:         currentVersion,
	}
}

// reload will reload all the databases for this tenant. This is only reloading all the databases that exists in the disk.
// Loading all corresponding collections in this call is unnecessary and will be expensive. Rather that can happen
// during reloadDatabase API call.
func (tenant *Tenant) reload(ctx context.Context, tx transaction.Tx, currentVersion Version) error {
	// reset
	tenant.databases = make(map[string]*Database)
	tenant.idToDatabaseMap = make(map[uint32]string)

	dbNameToId, err := tenant.encoder.GetDatabases(ctx, tx, tenant.namespace.Id())
	if err != nil {
		return err
	}

	for db, id := range dbNameToId {
		database, err := tenant.reloadDatabase(ctx, tx, db, id)
		if ulog.E(err) {
			return err
		}

		tenant.databases[database.name] = database
		tenant.idToDatabaseMap[database.id] = database.name
	}

	tenant.version = currentVersion
	return nil
}

func (tenant *Tenant) ReloadUsingOutsideVersion(ctx context.Context, tx transaction.Tx, version Version, id string) error {
	tenant.Lock()
	defer tenant.Unlock()

	if bytes.Equal(version, tenant.version) {
		return nil
	}

	log.Debug().Str("tx_id", id).Msgf("reloading tenants")
	return tenant.reload(ctx, tx, version)
}

func (tenant *Tenant) ReloadUsingTxVersion(ctx context.Context, tx transaction.Tx, id string) error {
	tenant.Lock()
	defer tenant.Unlock()

	currentVersion, err := tenant.versionH.Read(ctx, tx)
	if err != nil {
		return err
	}

	if bytes.Equal(currentVersion, tenant.version) {
		return nil
	}

	log.Debug().Str("tx_id", id).Msgf("reloading tenants")
	return tenant.reload(ctx, tx, currentVersion)
}

func (tenant *Tenant) GetNamespace() Namespace {
	tenant.RLock()
	defer tenant.RUnlock()

	return tenant.namespace
}

// CreateDatabase is responsible for creating a dictionary encoding of the database. This method is not adding the
// entry to the tenant because the outer layer may still rollback the transaction. The query lifecycle is also bumping
// the metadata version so reloading happens at the next call when the tenant version is stale. This applies to the
// reloading mechanism on all the tigris workers.
// Returns "true" If the database already exists, else "false" and the error
func (tenant *Tenant) CreateDatabase(ctx context.Context, tx transaction.Tx, dbName string) (bool, error) {
	tenant.Lock()
	defer tenant.Unlock()

	if _, ok := tenant.databases[dbName]; ok {
		return true, nil
	}

	// otherwise, proceed to create the database if there are concurrent requests on different workers then one of
	// them will fail with duplicate entry and only one will succeed.
	_, err := tenant.encoder.EncodeDatabaseName(ctx, tx, dbName, tenant.namespace.Id())
	return false, err
}

// DropDatabase is responsible for first dropping a dictionary encoding of the database and then adding a corresponding
// dropped encoding in the table. Drop returns "false" if database doesn't exist so that caller can reason about.
func (tenant *Tenant) DropDatabase(ctx context.Context, tx transaction.Tx, dbName string, searchStore search.Store) (bool, error) {
	tenant.Lock()
	defer tenant.Unlock()

	// check first if it exists
	db, ok := tenant.databases[dbName]
	if !ok {
		return false, nil
	}

	// if there are concurrent requests on different workers then one of them will fail with duplicate entry and only
	// one will succeed.
	if err := tenant.encoder.EncodeDatabaseAsDropped(ctx, tx, dbName, tenant.namespace.Id(), db.id); err != nil {
		return true, err
	}

	for _, c := range db.collections {
		if err := tenant.dropCollection(ctx, tx, db, c.collection.Name, searchStore); err != nil {
			return true, err
		}
	}

	return true, nil
}

func (tenant *Tenant) InvalidateDBCache(dbName string) {
	tenant.Lock()
	defer tenant.Unlock()

	delete(tenant.databases, dbName)
}

// GetDatabase returns the database object, or null if there is no database exist with the name passed in the param.
// This API is also responsible for reloading the tenant knowledge of the databases to ensure caller sees a consistent
// view of all the schemas. This is achieved by first checking the meta version and if it is changed then call reload.
func (tenant *Tenant) GetDatabase(_ context.Context, _ transaction.Tx, dbName string) (*Database, error) {
	tenant.Lock()
	defer tenant.Unlock()

	return tenant.databases[dbName], nil
}

func (tenant *Tenant) ListDatabases(_ context.Context, _ transaction.Tx) []string {
	tenant.RLock()
	defer tenant.RUnlock()

	var databases []string
	for dbName := range tenant.databases {
		databases = append(databases, dbName)
	}

	return databases
}

func (tenant *Tenant) reloadDatabase(ctx context.Context, tx transaction.Tx, dbName string, dbId uint32) (*Database, error) {
	database := NewDatabase(dbId, dbName)

	collNameToId, err := tenant.encoder.GetCollections(ctx, tx, tenant.namespace.Id(), database.id)
	if err != nil {
		return nil, err
	}

	for coll, id := range collNameToId {
		idxNameToId, err := tenant.encoder.GetIndexes(ctx, tx, tenant.namespace.Id(), database.id, id)
		if err != nil {
			database.needFixingCollections[coll] = struct{}{}
			log.Debug().Err(err).Str("collection", coll).Msg("skipping loading collection")
			continue
		}

		userSchema, version, err := tenant.schemaStore.GetLatest(ctx, tx, tenant.namespace.Id(), database.id, id)
		if err != nil {
			database.needFixingCollections[coll] = struct{}{}
			log.Debug().Err(err).Str("collection", coll).Msg("skipping loading collection")
			continue
		}

		collection, err := createCollection(id, version, coll, userSchema, idxNameToId, tenant.getSearchCollName(dbName, coll))
		if err != nil {
			database.needFixingCollections[coll] = struct{}{}
			log.Debug().Err(err).Str("collection", coll).Msg("skipping loading collection")
			continue
		}

		database.collections[coll] = NewCollectionHolder(id, coll, collection, idxNameToId)
		database.idToCollectionMap[id] = coll
	}

	return database, nil
}

func (tenant *Tenant) GetCollection(db string, collection string) *schema.DefaultCollection {
	tenant.RLock()
	defer tenant.RUnlock()

	if db := tenant.databases[db]; db != nil {
		return db.GetCollection(collection)
	}

	return nil
}

// CreateCollection is to create a collection inside tenant namespace.
func (tenant *Tenant) CreateCollection(ctx context.Context, tx transaction.Tx, database *Database, schFactory *schema.Factory, searchStore search.Store) error {
	tenant.Lock()
	defer tenant.Unlock()

	if database == nil {
		return api.Errorf(api.Code_NOT_FOUND, "database missing")
	}

	// first check if we need to run update collection
	if c, ok := database.collections[schFactory.Name]; ok {
		if eq, err := IsSchemaEq(c.collection.Schema, schFactory.Schema); eq || err != nil {
			// shortcut to just check if schema is eq then return early
			return err
		}
		return tenant.updateCollection(ctx, tx, database, c, schFactory, searchStore)
	}

	collectionId, err := tenant.encoder.EncodeCollectionName(ctx, tx, schFactory.Name, tenant.namespace.Id(), database.id)
	if err != nil {
		return err
	}

	// encode indexes and add this back in the collection
	indexes := schFactory.Indexes.GetIndexes()
	idxNameToId := make(map[string]uint32)
	for _, i := range indexes {
		id, err := tenant.encoder.EncodeIndexName(ctx, tx, i.Name, tenant.namespace.Id(), database.id, collectionId)
		if err != nil {
			return err
		}
		i.Id = id
		idxNameToId[i.Name] = id
	}

	// all good now persist the schema
	if err := tenant.schemaStore.Put(ctx, tx, tenant.namespace.Id(), database.id, collectionId, schFactory.Schema, baseSchemaVersion); err != nil {
		return err
	}

	// store the collection to the databaseObject, this is actually cloned database object passed by the query runner.
	// So failure of the transaction won't impact the consistency of the cache
	collection := schema.NewDefaultCollection(schFactory.Name, collectionId, baseSchemaVersion, schFactory.Fields, schFactory.Indexes, schFactory.Schema, tenant.getSearchCollName(database.name, schFactory.Name))
	database.collections[schFactory.Name] = NewCollectionHolder(collectionId, schFactory.Name, collection, idxNameToId)
	if err := searchStore.CreateCollection(ctx, collection.Search); err != nil {
		if err != search.ErrDuplicateEntity {
			return err
		}
	}

	return nil
}

func (tenant *Tenant) updateCollection(ctx context.Context, tx transaction.Tx, database *Database, c *collectionHolder, schFactory *schema.Factory, searchStore search.Store) error {
	var newIndexes []*schema.Index
	for _, idx := range schFactory.Indexes.GetIndexes() {
		if _, ok := c.idxNameToId[idx.Name]; !ok {
			newIndexes = append(newIndexes, idx)
		}
	}

	for _, idx := range newIndexes {
		// these are the new indexes present in the new collection
		id, err := tenant.encoder.EncodeIndexName(ctx, tx, idx.Name, tenant.namespace.Id(), database.id, c.id)
		if err != nil {
			return err
		}
		idx.Id = id
		c.addIndex(idx.Name, idx.Id)
	}

	for _, idx := range schFactory.Indexes.GetIndexes() {
		// now we have all indexes with dictionary encoded values, set it in the index struct
		if id, ok := c.idxNameToId[idx.Name]; ok {
			idx.Id = id
		}
	}

	// now validate if the new collection(schema) conforms to the backward compatibility rules.
	if err := schema.ApplySchemaRules(c.collection, schFactory); err != nil {
		return err
	}

	schRevision := c.collection.SchVer + 1
	if err := tenant.schemaStore.Put(ctx, tx, tenant.namespace.Id(), database.id, c.id, schFactory.Schema, schRevision); err != nil {
		return err
	}

	deltaFields := schema.GetSearchDeltaFields(c.collection.QueryableFields, schFactory.Fields)

	// store the collection to the databaseObject, this is actually cloned database object passed by the query runner.
	// So failure of the transaction won't impact the consistency of the cache
	collection := schema.NewDefaultCollection(schFactory.Name, c.id, schRevision, schFactory.Fields, schFactory.Indexes, schFactory.Schema, tenant.getSearchCollName(database.name, schFactory.Name))

	// recreating collection holder is fine because we are working on databaseClone and also has a lock on the tenant
	database.collections[schFactory.Name] = NewCollectionHolder(c.id, schFactory.Name, collection, c.idxNameToId)
	if err := searchStore.UpdateCollection(ctx, collection.Search.Name, &tsApi.CollectionUpdateSchema{
		Fields: deltaFields,
	}); err != nil {
		return err
	}
	return nil

}

// DropCollection is to drop a collection and its associated indexes. It removes the "created" entry from the encoding
// subspace and adds a "dropped" entry for the same collection key.
func (tenant *Tenant) DropCollection(ctx context.Context, tx transaction.Tx, db *Database, collectionName string, searchStore search.Store) error {
	tenant.Lock()
	defer tenant.Unlock()

	err := tenant.dropCollection(ctx, tx, db, collectionName, searchStore)
	if err != nil {
		return err
	}

	// the passed database object is cloned copy, so cleanup the entries from the cloned copy as this cloned database
	// may be used in further operations if it is an explicit transaction.
	delete(db.idToCollectionMap, db.collections[collectionName].id)
	delete(db.collections, collectionName)
	return err
}

func (tenant *Tenant) dropCollection(ctx context.Context, tx transaction.Tx, db *Database, collectionName string, searchStore search.Store) error {
	if db == nil {
		return api.Errorf(api.Code_NOT_FOUND, "database missing")
	}

	cHolder, ok := db.collections[collectionName]
	if !ok {
		return api.Errorf(api.Code_NOT_FOUND, "collection doesn't exists '%s'", collectionName)
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

	if err := searchStore.DropCollection(ctx, cHolder.collection.SearchCollectionName()); err != nil {
		if err != search.ErrNotFound {
			return err
		}
	}

	return nil
}

func (tenant *Tenant) getSearchCollName(dbName string, collName string) string {
	return fmt.Sprintf("%s-%s-%s", tenant.namespace.Name(), dbName, collName)
}

func (tenant *Tenant) String() string {
	return fmt.Sprintf("id: %d, name: %s", tenant.namespace.Id(), tenant.namespace.Name())
}

// Database is to manage the collections for this database. Check the Clone method before changing this struct.
type Database struct {
	sync.RWMutex

	id                    uint32
	name                  string
	collections           map[string]*collectionHolder
	needFixingCollections map[string]struct{}
	idToCollectionMap     map[uint32]string
}

func NewDatabase(id uint32, name string) *Database {
	return &Database{
		id:                    id,
		name:                  name,
		collections:           make(map[string]*collectionHolder),
		idToCollectionMap:     make(map[uint32]string),
		needFixingCollections: make(map[string]struct{}),
	}
}

// Clone is used to stage the database
func (d *Database) Clone() *Database {
	d.Lock()
	defer d.Unlock()

	var copyDB Database
	copyDB.id = d.id
	copyDB.name = d.name
	copyDB.collections = make(map[string]*collectionHolder)
	for k, v := range d.collections {
		copyDB.collections[k] = v.clone()
	}
	copyDB.idToCollectionMap = make(map[uint32]string)
	for k, v := range d.idToCollectionMap {
		copyDB.idToCollectionMap[k] = v
	}

	return &copyDB
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
}

func NewCollectionHolder(id uint32, name string, collection *schema.DefaultCollection, idxNameToId map[string]uint32) *collectionHolder {
	return &collectionHolder{
		id:          id,
		name:        name,
		collection:  collection,
		idxNameToId: idxNameToId,
	}
}

// clone is used to stage the collectionHolder
func (c *collectionHolder) clone() *collectionHolder {
	c.Lock()
	defer c.Unlock()

	var copyC collectionHolder
	copyC.id = c.id
	copyC.name = c.name

	var err error
	copyC.collection, err = createCollection(c.id, c.collection.SchVer, c.name, c.collection.Schema, c.idxNameToId, c.collection.SearchCollectionName())
	if err != nil {
		panic(err)
	}
	copyC.idxNameToId = make(map[string]uint32)
	for k, v := range c.idxNameToId {
		copyC.idxNameToId[k] = v
	}

	return &copyC
}

func (c *collectionHolder) addIndex(name string, id uint32) {
	c.Lock()
	defer c.Unlock()

	c.idxNameToId[name] = id
}

// get returns the collection managed by this holder. At this point, a Collection object is safely constructed
// with all encoded values assigned to all the attributed i.e. collection, index has assigned the encoded
// values.
func (c *collectionHolder) get() *schema.DefaultCollection {
	c.RLock()
	defer c.RUnlock()

	return c.collection
}

func createCollection(id uint32, schVer int, name string, revision []byte, idxNameToId map[string]uint32, searchCollectionName string) (*schema.DefaultCollection, error) {
	schFactory, err := schema.Build(name, revision)
	if err != nil {
		return nil, err
	}

	indexes := schFactory.Indexes.GetIndexes()
	for _, index := range indexes {
		id, ok := idxNameToId[index.Name]
		if !ok {
			return nil, api.Errorf(api.Code_NOT_FOUND, "dictionary encoding is missing for index '%s'", index.Name)
		}
		index.Id = id
	}

	return schema.NewDefaultCollection(name, id, schVer, schFactory.Fields, schFactory.Indexes, revision, searchCollectionName), nil
}

func IsSchemaEq(s1, s2 []byte) (bool, error) {
	var j, j2 interface{}
	if err := jsoniter.Unmarshal(s1, &j); err != nil {
		return false, err
	}
	if err := jsoniter.Unmarshal(s2, &j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j2, j), nil
}
