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
	"math/rand"
	"reflect"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/defaults"
	"github.com/tigrisdata/tigris/server/transaction"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/store/search"
	ulog "github.com/tigrisdata/tigris/util/log"
	tsApi "github.com/typesense/typesense-go/typesense/api"
)

type NamespaceType string

const (
	baseSchemaVersion = 1
)

// A Namespace is a logical grouping of databases.
type Namespace interface {
	// Id for the namespace is used by the cluster to append as the first element in the key.
	Id() uint32
	// StrId is the name used for the lookup.
	StrId() string
	// Metadata for the namespace
	Metadata() NamespaceMetadata
}

// NamespaceMetadata - This structure is persisted as the namespace in DB.
type NamespaceMetadata struct {
	// unique namespace Id
	Id uint32
	// unique namespace name StrId
	StrId string
	// displayName for the namespace
	Name string
}

// DefaultNamespace is for "default" namespace in the cluster. This is useful when there is no need to logically group
// databases. All databases will be created under a single namespace. It is totally fine for a deployment to choose this
// and just have one namespace. The default assigned value for this namespace is 1.
type DefaultNamespace struct{}

type TenantGetter interface {
	GetTenant(ctx context.Context, id string) (*Tenant, error)
}

// StrId returns id assigned to the namespace.
func (n *DefaultNamespace) StrId() string {
	return defaults.DefaultNamespaceName
}

// Id returns id assigned to the namespace.
func (n *DefaultNamespace) Id() uint32 {
	return defaults.DefaultNamespaceId
}

// Metadata returns metadata assigned to the namespace.
func (n *DefaultNamespace) Metadata() NamespaceMetadata {
	return NewNamespaceMetadata(defaults.DefaultNamespaceId, defaults.DefaultNamespaceName, defaults.DefaultNamespaceName)
}

func NewDefaultNamespace() *DefaultNamespace {
	return &DefaultNamespace{}
}

// TenantNamespace is used when there is a finer isolation of databases is needed. The caller provides a unique
// id and strId to this namespace which is used by the cluster to create a namespace.
type TenantNamespace struct {
	lookupStrId string
	lookupId    uint32
	metadata    NamespaceMetadata
}

func NewNamespaceMetadata(id uint32, name string, displayName string) NamespaceMetadata {
	return NamespaceMetadata{
		Id:    id,
		StrId: name,
		Name:  displayName,
	}
}

func NewTenantNamespace(name string, metadata NamespaceMetadata) *TenantNamespace {
	return &TenantNamespace{
		lookupStrId: name,
		lookupId:    metadata.Id,
		metadata:    metadata,
	}
}

// StrId returns assigned id for the namespace.
func (n *TenantNamespace) StrId() string {
	return n.lookupStrId
}

// Id returns assigned code for the namespace.
func (n *TenantNamespace) Id() uint32 {
	return n.lookupId
}

// Metadata returns assigned metadata for the namespace.
func (n *TenantNamespace) Metadata() NamespaceMetadata {
	return n.metadata
}

// TenantManager is to manage all the tenants
// ToDo: start a background thread to reload the mapping.
type TenantManager struct {
	sync.RWMutex

	metaStore         *MetadataDictionary
	schemaStore       *SchemaSubspace
	namespaceStore    *NamespaceSubspace
	kvStore           kv.KeyValueStore
	searchStore       search.Store
	tenants           map[string]*Tenant
	idToTenantMap     map[uint32]string
	version           Version
	versionH          *VersionHandler
	mdNameRegistry    MDNameRegistry
	encoder           Encoder
	tableKeyGenerator *TableKeyGenerator
	txMgr             *transaction.Manager
}

func (m *TenantManager) GetNamespaceStore() *NamespaceSubspace {
	return m.namespaceStore
}

func NewTenantManager(kvStore kv.KeyValueStore, searchStore search.Store, txMgr *transaction.Manager) *TenantManager {
	mdNameRegistry := &DefaultMDNameRegistry{}
	return newTenantManager(kvStore, searchStore, mdNameRegistry, txMgr)
}

func newTenantManager(kvStore kv.KeyValueStore, searchStore search.Store, mdNameRegistry MDNameRegistry, txMgr *transaction.Manager) *TenantManager {
	return &TenantManager{
		kvStore:           kvStore,
		searchStore:       searchStore,
		encoder:           NewEncoder(),
		metaStore:         NewMetadataDictionary(mdNameRegistry),
		schemaStore:       NewSchemaStore(mdNameRegistry),
		namespaceStore:    NewNamespaceStore(mdNameRegistry),
		tenants:           make(map[string]*Tenant),
		idToTenantMap:     make(map[uint32]string),
		versionH:          &VersionHandler{},
		mdNameRegistry:    mdNameRegistry,
		tableKeyGenerator: NewTableKeyGenerator(),
		txMgr:             txMgr,
	}
}

func (m *TenantManager) EnsureDefaultNamespace() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := m.CreateOrGetTenant(ctx, NewDefaultNamespace())
	return err
}

// CreateOrGetTenant is a thread safe implementation of creating a new tenant. It returns the tenant if it already exists.
// This is mainly returning the tenant to avoid calling "Get" again after creating the tenant. This method is expensive
// as it reloads the existing tenants from the disk if it sees the tenant is not present in the cache.
func (m *TenantManager) CreateOrGetTenant(ctx context.Context, namespace Namespace) (tenant *Tenant, err error) {
	m.Lock()
	defer m.Unlock()

	var ok bool
	if tenant, ok = m.tenants[namespace.StrId()]; ok {
		if tenant.namespace.Id() == namespace.Id() {
			// tenant was present
			log.Debug().Str("ns", tenant.String()).Msg("tenant found")
			return tenant, nil
		} else {
			return nil, errors.InvalidArgument("id is already assigned to strId='%s'", tenant.namespace.StrId())
		}
	}

	tx, e := m.txMgr.StartTx(ctx)
	if ulog.E(e) {
		return nil, e
	}

	defer func() {
		if err == nil {
			if err = tx.Commit(ctx); err == nil {
				// commit succeed, so we can safely cache it now, for other workers it may happen as part of the
				// first call in query lifecycle
				m.tenants[namespace.StrId()] = tenant
				m.idToTenantMap[namespace.Id()] = namespace.StrId()
			}
		} else {
			_ = tx.Rollback(ctx)
		}
	}()

	return m.createOrGetTenantInternal(ctx, tx, namespace)
}

func (m *TenantManager) GetEncoder() Encoder {
	return m.encoder
}

// CreateTenant is a thread safe implementation of creating a new tenant. It returns an error if it already exists.
func (m *TenantManager) CreateTenant(ctx context.Context, tx transaction.Tx, namespace Namespace) (Namespace, error) {
	m.Lock()
	defer m.Unlock()
	namespaces, err := m.metaStore.GetNamespaces(ctx, tx)
	if err != nil {
		return nil, err
	}

	if metadata, found := namespaces[namespace.StrId()]; found {
		return nil, errors.AlreadyExists("namespace with same name already exists with id '%d'", metadata.Id)
	}
	for name, metadata := range namespaces {
		if metadata.Id == namespace.Id() {
			return nil, errors.AlreadyExists("namespace with same id already exists with name '%s'", name)
		}
	}
	if err := m.metaStore.ReserveNamespace(ctx, tx, namespace.StrId(), namespace.Metadata()); ulog.E(err) {
		return nil, err
	}
	if err := m.versionH.Increment(ctx, tx); ulog.E(err) {
		return nil, err
	}
	return namespace, nil
}

func (m *TenantManager) getTenantFromCache(namespaceName string) (tenant *Tenant) {
	m.RLock()
	defer m.RUnlock()
	if tenant, found := m.tenants[namespaceName]; found {
		return tenant
	}
	return nil
}

func (m *TenantManager) GetNamespaceNames() []string {
	res := make([]string, 0, len(m.tenants))
	for name := range m.tenants {
		res = append(res, name)
	}
	return res
}

func (m *TenantManager) GetNamespaceId(namespaceName string) (uint32, error) {
	tenant := m.getTenantFromCache(namespaceName)
	if tenant == nil {
		return 0, errors.NotFound("Namespace not found")
	}
	return tenant.namespace.Id(), nil
}

// GetTenant is responsible for returning the tenant from the cache. If the tenant is not available in the cache then
// this method will attempt to load it from the database and will update the tenant manager cache accordingly.
func (m *TenantManager) GetTenant(ctx context.Context, namespaceName string) (tenant *Tenant, err error) {
	if tenant = m.getTenantFromCache(namespaceName); tenant != nil {
		return
	}

	m.Lock()
	defer m.Unlock()
	if tenant, found := m.tenants[namespaceName]; found {
		return tenant, nil
	}

	collectionsInSearch, err := m.searchStore.AllCollections(ctx)
	if err != nil {
		return nil, err
	}

	// this will never create new namespace
	// when the authn/authz is setup correctly
	// this is for reading namespaces from storage into cache
	tx, err := m.txMgr.StartTx(ctx)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err == nil {
			if err = tx.Commit(ctx); err == nil && tenant != nil {
				m.tenants[tenant.namespace.StrId()] = tenant
				m.idToTenantMap[tenant.namespace.Id()] = tenant.namespace.StrId()
			}
		} else {
			log.Err(err).Str("ns", namespaceName).Msg("Could not get namespace")
			_ = tx.Rollback(ctx)
		}
	}()

	var namespaces map[string]NamespaceMetadata
	if namespaces, err = m.metaStore.GetNamespaces(ctx, tx); err != nil {
		return nil, err
	}
	metadata, ok := namespaces[namespaceName]
	if !ok {
		return nil, fmt.Errorf("namespace not found: %s", namespaceName)
	}

	currentVersion, err := m.versionH.Read(ctx, tx, false)
	if err != nil {
		return nil, err
	}

	namespace := NewTenantNamespace(namespaceName, metadata)
	tenant = NewTenant(namespace, m.kvStore, m.searchStore, m.metaStore, m.schemaStore, m.namespaceStore, m.encoder, m.versionH, currentVersion, m.tableKeyGenerator)
	if err = tenant.reload(ctx, tx, currentVersion, collectionsInSearch); err != nil {
		return nil, err
	}
	return
}

// ListNamespaces returns all the namespaces(tenants) exist in this cluster.
func (m *TenantManager) ListNamespaces(ctx context.Context, tx transaction.Tx) ([]Namespace, error) {
	m.RLock()
	defer m.RUnlock()
	namespaces, err := m.metaStore.GetNamespaces(ctx, tx)
	if err != nil {
		_ = tx.Rollback(ctx)
		log.Warn().Err(err).Msg("Could not list namespaces")
		return nil, err
	}
	result := make([]Namespace, 0, len(namespaces))
	for k, v := range namespaces {
		result = append(result, NewTenantNamespace(k, v))
	}
	return result, nil
}

func (m *TenantManager) createOrGetTenantInternal(ctx context.Context, tx transaction.Tx, namespace Namespace) (*Tenant, error) {
	namespaces, err := m.metaStore.GetNamespaces(ctx, tx)
	if err != nil {
		return nil, err
	}
	log.Debug().Interface("ns", namespaces).Msg("existing namespaces")
	if _, ok := namespaces[namespace.StrId()]; ok {
		// only read the version if tenant already exists otherwise, we need to increment it.
		currentVersion, err := m.versionH.Read(ctx, tx, false)
		if ulog.E(err) {
			return nil, err
		}

		collectionsInSearch, err := m.searchStore.AllCollections(ctx)
		if err != nil {
			return nil, err
		}
		tenant := NewTenant(namespace, m.kvStore, m.searchStore, m.metaStore, m.schemaStore, m.namespaceStore, m.encoder, m.versionH, currentVersion, m.tableKeyGenerator)
		tenant.Lock()
		err = tenant.reload(ctx, tx, currentVersion, collectionsInSearch)
		tenant.Unlock()
		return tenant, err
	}

	log.Debug().Str("tenant", namespace.StrId()).Msg("tenant not found, creating")

	// bump the version first
	if err := m.versionH.Increment(ctx, tx); ulog.E(err) {
		return nil, err
	}

	if err := m.metaStore.ReserveNamespace(ctx, tx, namespace.StrId(), namespace.Metadata()); ulog.E(err) {
		return nil, err
	}

	return NewTenant(namespace, m.kvStore, m.searchStore, m.metaStore, m.schemaStore, m.namespaceStore, m.encoder, m.versionH, nil, m.tableKeyGenerator), nil
}

// GetTableNameFromIds returns tenant name, database name, collection name corresponding to their encoded ids.
func (m *TenantManager) GetTableNameFromIds(tenantId uint32, dbId uint32, collId uint32) (string, string, string, bool) {
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

// GetDatabaseAndCollectionId returns the id of db and c in the default namespace. This is just a temporary API for
// the streams to know if database and collection exists at the start of streaming and their corresponding IDs.
func (m *TenantManager) GetDatabaseAndCollectionId(db string, c string) (uint32, uint32) {
	m.RLock()
	defer m.RUnlock()

	tenant, ok := m.tenants[defaults.DefaultNamespaceName]
	if !ok {
		return 0, 0
	}
	database, ok := tenant.databases[db]
	if !ok {
		return 0, 0
	}

	coll, ok := database.collections[c]
	if !ok {
		return 0, 0
	}

	return database.id, coll.id
}

func (m *TenantManager) DecodeTableName(tableName []byte) (string, string, string, bool) {
	n, d, c, ok := m.encoder.DecodeTableName(tableName)
	if !ok {
		return "", "", "", false
	}

	return m.GetTableNameFromIds(n, d, c)
}

// Reload reads all the tenants exist in the database and builds an in-memory view of the manager to track the tenants.
// As this is an expensive call, the reloading happens only during the start of the server. It is possible that reloading
// fails during start time then we rely on each transaction to detect it and trigger reload. The consistency shouldn’t
// be impacted if we fail to load the in-memory view.
func (m *TenantManager) Reload(ctx context.Context, tx transaction.Tx, collectionsInSearch map[string]*tsApi.CollectionResponse) error {
	log.Debug().Msg("reloading tenants")
	m.Lock()
	defer m.Unlock()

	currentVersion, err := m.versionH.Read(ctx, tx, false)
	if ulog.E(err) {
		return err
	}

	if err = m.reload(ctx, tx, currentVersion, collectionsInSearch); ulog.E(err) {
		return err
	}
	m.version = currentVersion
	log.Debug().Msgf("latest meta version %v", m.version)
	return err
}

func (m *TenantManager) reload(ctx context.Context, tx transaction.Tx, currentVersion Version, collectionsInSearch map[string]*tsApi.CollectionResponse) error {
	namespaces, err := m.metaStore.GetNamespaces(ctx, tx)
	if err != nil {
		return err
	}
	log.Debug().Interface("ns", namespaces).Msg("existing reserved namespaces")

	for namespace, metadata := range namespaces {
		if _, ok := m.tenants[namespace]; !ok {
			m.tenants[namespace] = NewTenant(NewTenantNamespace(namespace, metadata), m.kvStore, m.searchStore, m.metaStore, m.schemaStore, m.namespaceStore, m.encoder, m.versionH, currentVersion, m.tableKeyGenerator)
			m.idToTenantMap[metadata.Id] = namespace
		}
	}

	for _, tenant := range m.tenants {
		log.Debug().Interface("tenant", tenant.String()).Msg("reloading tenant")
		tenant.Lock()
		err := tenant.reload(ctx, tx, currentVersion, collectionsInSearch)
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

	kvStore           kv.KeyValueStore
	searchStore       search.Store
	schemaStore       *SchemaSubspace
	namespaceStore    *NamespaceSubspace
	metaStore         *MetadataDictionary
	Encoder           Encoder
	databases         map[string]*Database
	idToDatabaseMap   map[uint32]string
	namespace         Namespace
	version           Version
	versionH          *VersionHandler
	TableKeyGenerator *TableKeyGenerator
}

func NewTenant(namespace Namespace, kvStore kv.KeyValueStore, searchStore search.Store, dict *MetadataDictionary, schemaStore *SchemaSubspace, namespaceStore *NamespaceSubspace, encoder Encoder, versionH *VersionHandler, currentVersion Version, _ *TableKeyGenerator) *Tenant {
	return &Tenant{
		kvStore:         kvStore,
		searchStore:     searchStore,
		namespace:       namespace,
		metaStore:       dict,
		schemaStore:     schemaStore,
		namespaceStore:  namespaceStore,
		databases:       make(map[string]*Database),
		idToDatabaseMap: make(map[uint32]string),
		versionH:        versionH,
		version:         currentVersion,
		Encoder:         encoder,
	}
}

// reload will reload all the databases for this tenant. This is only reloading all the databases that exists in the
// tenant.
func (tenant *Tenant) reload(ctx context.Context, tx transaction.Tx, currentVersion Version, collectionsInSearch map[string]*tsApi.CollectionResponse) error {
	// reset
	tenant.databases = make(map[string]*Database)
	tenant.idToDatabaseMap = make(map[uint32]string)

	dbNameToId, err := tenant.metaStore.GetDatabases(ctx, tx, tenant.namespace.Id())
	if err != nil {
		return err
	}

	for db, id := range dbNameToId {
		database, err := tenant.reloadDatabase(ctx, tx, db, id, collectionsInSearch)
		if ulog.E(err) {
			return err
		}

		tenant.databases[database.Name()] = database
		tenant.idToDatabaseMap[database.id] = database.Name()
	}

	tenant.version = currentVersion
	return nil
}

// Reload is used to reload this tenant. The reload method compares the currently attached version to the tenant to the
// version passed in the API call to detect whether reloading is needed. This check is needed to ensure only a single
// thread will actually perform reload. This is a blocking API which means if most of the requests detected that the
// tenant state is stale then they all will block till one of them will reload the tenant state from the database. All
// the blocking transactions will be restarted to ensure they see the latest view of the tenant.
func (tenant *Tenant) Reload(ctx context.Context, tx transaction.Tx, version Version) error {
	if !tenant.shouldReload(version) {
		return nil
	}

	tenant.Lock()
	defer tenant.Unlock()
	if bytes.Compare(version, tenant.version) < 1 {
		// do not reload if version retrogressed
		return nil
	}

	collectionsInSearch, err := tenant.searchStore.AllCollections(ctx)
	if err != nil {
		return err
	}
	return tenant.reload(ctx, tx, version, collectionsInSearch)
}

func (tenant *Tenant) shouldReload(currentVersion Version) bool {
	tenant.RLock()
	defer tenant.RUnlock()

	return bytes.Compare(currentVersion, tenant.version) > 0
}

// GetNamespace returns the namespace of this tenant.
func (tenant *Tenant) GetNamespace() Namespace {
	tenant.RLock()
	defer tenant.RUnlock()

	return tenant.namespace
}

// CreateDatabase is responsible for creating a dictionary encoding of the database name. This method is not adding the
// entry to the tenant because the outer layer may still roll back the transaction. The session manager is bumping the
// metadata version once the commit is successful so reloading happens at the next call when a transaction sees a stale
// tenant version. This applies to the reloading mechanism on all the servers. It returns "true" If the database already
// exists, else "false" and the error.
func (tenant *Tenant) CreateDatabase(ctx context.Context, tx transaction.Tx, dbName string, dbMetadata *DatabaseMetadata) (bool, error) {
	tenant.Lock()
	defer tenant.Unlock()

	if _, ok := tenant.databases[dbName]; ok {
		return true, nil
	}

	// otherwise, proceed to create the database if there are concurrent requests on different workers then one of
	// them will fail with duplicate entry and only one will succeed.
	dbId, err := tenant.metaStore.CreateDatabase(ctx, tx, dbName, tenant.namespace.Id())
	if dbMetadata != nil {
		dbMetadata.SetDatabaseId(dbId)
		err = tenant.namespaceStore.InsertDatabaseMetadata(ctx, tx, tenant.namespace.Id(), dbName, dbMetadata)
		if err != nil {
			log.Err(err).Msg("Failed to insert database metadata")
			return false, errors.Internal("Failed to setup db metadata")
		}
	}
	return false, err
}

func (tenant *Tenant) CreateBranch(ctx context.Context, tx transaction.Tx, dbBranch *DatabaseBranch) error {
	tenant.Lock()
	defer tenant.Unlock()

	// Create a database branch only if parent database exists
	mainDb, ok := tenant.databases[dbBranch.Db()]
	if !ok {
		return NewDatabaseNotFoundErr(dbBranch.Db())
	}

	// Check if a branch with same name exists
	dbName := dbBranch.Name()
	if _, ok := tenant.databases[dbName]; ok {
		return NewDatabaseBranchExistsErr(dbBranch.Branch())
	}

	// Create a database
	id, err := tenant.metaStore.CreateDatabase(ctx, tx, dbName, tenant.namespace.Id())
	if err != nil {
		return err
	}

	branch := NewDatabase(id, dbName)

	// Create collections inside the new database branch
	for _, coll := range mainDb.ListCollection() {
		schFactory, err := schema.Build(coll.Name, coll.Schema)
		if err != nil {
			return err
		}

		if err := tenant.createCollection(ctx, tx, branch, schFactory); err != nil {
			return err
		}
	}

	return err
}

// DropDatabase is responsible for first dropping a dictionary encoding of the database and then adding a corresponding
// dropped encoding entry in the encoding table. Drop returns "false" if the database doesn't exist so that caller can
// reason about it. DropDatabase is more involved than CreateDatabase as with Drop we also need to iterate over all the
// collections present in this database and call drop collection on each one of them.
func (tenant *Tenant) DropDatabase(ctx context.Context, tx transaction.Tx, dbName string) (bool, error) {
	tenant.Lock()
	defer tenant.Unlock()

	// check first if it exists
	db, found := tenant.databases[dbName]
	if !found {
		return false, nil
	}

	// Only main branch can be deleted, use DeleteBranch instead, TODO: return an error here
	if db.IsBranch() {
		return false, nil
	}

	// Get all the branches for deletion
	branches := tenant.getBranches(ctx, db, false)
	// iterate over each branch to delete it
	for _, branch := range branches {
		if err := tenant.deleteBranch(ctx, tx, NewDatabaseBranch(branch.DbName(), branch.BranchName())); err != nil {
			return false, err
		}
	}

	// delete the main branch, collections and associated metadata if there are concurrent requests on different workers
	// then one of them will fail with duplicate entry and only one will succeed.
	if err := tenant.metaStore.DropDatabase(ctx, tx, db.Name(), tenant.namespace.Id(), db.Id()); err != nil {
		return true, err
	}

	for _, c := range db.collections {
		if err := tenant.dropCollection(ctx, tx, db, c.collection.Name); err != nil {
			return true, err
		}
	}

	// drop metadata entry
	if err := tenant.namespaceStore.DeleteDatabaseMetadata(ctx, tx, tenant.namespace.Id(), dbName); err != nil {
		log.Err(err).Msg("Failed to delete database metadata")
		return false, errors.Internal("Failed to delete database metadata")
	}

	return true, nil
}

func (tenant *Tenant) DeleteBranch(ctx context.Context, tx transaction.Tx, dbBranch *DatabaseBranch) error {
	tenant.Lock()
	defer tenant.Unlock()
	if dbBranch.IsMain() {
		return MainBranchCannotBeDeletedErr
	}
	return tenant.deleteBranch(ctx, tx, dbBranch)
}

func (tenant *Tenant) deleteBranch(ctx context.Context, tx transaction.Tx, dbBranch *DatabaseBranch) error {

	dbName := dbBranch.Name()
	// check first if it exists
	db, found := tenant.databases[dbName]
	if !found {
		return NewBranchNotFoundErr(dbBranch.Branch())
	}

	// if there are concurrent requests on different workers then one of them will fail with duplicate entry and only
	// one will succeed.
	if err := tenant.metaStore.DropDatabase(ctx, tx, db.Name(), tenant.namespace.Id(), db.Id()); err != nil {
		return err
	}

	for _, c := range db.collections {
		if err := tenant.dropCollection(ctx, tx, db, c.collection.Name); err != nil {
			return err
		}
	}
	return nil
}

// GetDatabase returns the database object, or null if there is no database existing with the name passed in the param.
// As reloading of tenant state is happening at the session manager layer so GetDatabase calls assume that the caller
// just needs the state from the cache.
func (tenant *Tenant) GetDatabase(_ context.Context, dbBranch *DatabaseBranch) (*Database, error) {
	tenant.Lock()
	defer tenant.Unlock()

	return tenant.databases[dbBranch.Name()], nil
}

//TODO: Add docs
func (tenant *Tenant) GetBranches(ctx context.Context, mainDb *Database) []*Database {
	tenant.Lock()
	defer tenant.Unlock()

	return tenant.getBranches(ctx, mainDb, true)
}

// TODO: add docs with -- includes main branch
func (tenant *Tenant) getBranches(_ context.Context, mainDb *Database, includeMain bool) []*Database {
	var branches []*Database

	for _, db := range tenant.databases {
		if (includeMain || db.IsBranch()) && (db.DbName() == mainDb.Name()) {
			branches = append(branches, db)
		}
	}
	return branches
}

// ListDatabasesOnly is used to list all databases (only the main branch) available for this tenant.
func (tenant *Tenant) ListDatabasesOnly(_ context.Context) []string {
	tenant.RLock()
	defer tenant.RUnlock()

	var databases []string
	for dbName := range tenant.databases {
		// do not list database branches
		if !tenant.databases[dbName].IsBranch() {
			databases = append(databases, dbName)
		}
	}

	return databases
}

func (tenant *Tenant) ListDatabaseWithBranches(_ context.Context) []string {
	tenant.RLock()
	defer tenant.RUnlock()

	var branches []string
	for name := range tenant.databases {
		branches = append(branches, name)
	}

	return branches
}

// reloadDatabase is called by tenant to reload the database state.
func (tenant *Tenant) reloadDatabase(ctx context.Context, tx transaction.Tx, dbName string, dbId uint32, searchCollections map[string]*tsApi.CollectionResponse) (*Database, error) {
	database := NewDatabase(dbId, dbName)

	collNameToId, err := tenant.metaStore.GetCollections(ctx, tx, tenant.namespace.Id(), database.id)
	if err != nil {
		return nil, err
	}

	for coll, id := range collNameToId {
		idxNameToId, err := tenant.metaStore.GetIndexes(ctx, tx, tenant.namespace.Id(), database.id, id)
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

		var fieldsInSearch []tsApi.Field
		searchCollectionName := tenant.getSearchCollName(dbName, coll)
		if searchSchema, ok := searchCollections[searchCollectionName]; ok {
			fieldsInSearch = searchSchema.Fields
		}
		if len(fieldsInSearch) == 0 {
			log.Error().Str("search_collection", searchCollectionName).Msg("fields are not present in search")
		}

		collection, err := createCollection(id, version, coll, userSchema, idxNameToId, searchCollectionName, fieldsInSearch)
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
func (tenant *Tenant) CreateCollection(ctx context.Context, tx transaction.Tx, database *Database, schFactory *schema.Factory) error {
	tenant.Lock()
	defer tenant.Unlock()

	return tenant.createCollection(ctx, tx, database, schFactory)
}

func (tenant *Tenant) createCollection(ctx context.Context, tx transaction.Tx, database *Database, schFactory *schema.Factory) error {
	if database == nil {
		return errors.NotFound("database missing")
	}

	// first check if we need to run update collection
	if c, ok := database.collections[schFactory.Name]; ok {
		if eq, err := isSchemaEq(c.collection.Schema, schFactory.Schema); eq || err != nil {
			// shortcut to just check if schema is eq then return early
			return err
		}
		return tenant.updateCollection(ctx, tx, database, c, schFactory)
	}

	// add indexing version here in the name, because this is a fresh create collection request
	if err := schema.SetIndexingVersion(schFactory); err != nil {
		return err
	}
	schFactory.IndexingVersion = schema.DefaultIndexingSchemaVersion

	collectionId, err := tenant.metaStore.CreateCollection(ctx, tx, schFactory.Name, tenant.namespace.Id(), database.id)
	if err != nil {
		return err
	}

	// encode indexes and add this back in the collection
	indexes := schFactory.Indexes.GetIndexes()
	idxNameToId := make(map[string]uint32)
	for _, i := range indexes {
		id, err := tenant.metaStore.CreateIndex(ctx, tx, i.Name, tenant.namespace.Id(), database.id, collectionId)
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
	collection := schema.NewDefaultCollection(schFactory.Name, collectionId, baseSchemaVersion, schFactory.CollectionType, schFactory, tenant.getSearchCollName(database.Name(), schFactory.Name), nil)
	database.collections[schFactory.Name] = NewCollectionHolder(collectionId, schFactory.Name, collection, idxNameToId)

	if config.DefaultConfig.Search.WriteEnabled {
		if err := tenant.searchStore.CreateCollection(ctx, collection.Search); err != nil {
			if err != search.ErrDuplicateEntity {
				return err
			}
		}
	}

	return nil
}

func (tenant *Tenant) updateCollection(ctx context.Context, tx transaction.Tx, database *Database, c *collectionHolder, schFactory *schema.Factory) error {
	var newIndexes []*schema.Index
	for _, idx := range schFactory.Indexes.GetIndexes() {
		if _, ok := c.idxNameToId[idx.Name]; !ok {
			newIndexes = append(newIndexes, idx)
		}
	}

	for _, idx := range newIndexes {
		// these are the new indexes present in the new collection
		id, err := tenant.metaStore.CreateIndex(ctx, tx, idx.Name, tenant.namespace.Id(), database.id, c.id)
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

	schRevision := int(c.collection.GetVersion()) + 1
	if err := tenant.schemaStore.Put(ctx, tx, tenant.namespace.Id(), database.id, c.id, schFactory.Schema, schRevision); err != nil {
		return err
	}

	searchCollectionName := tenant.getSearchCollName(database.Name(), schFactory.Name)
	existingSearch, err := tenant.searchStore.DescribeCollection(ctx, searchCollectionName)
	if err != nil {
		return err
	}

	// store the collection to the databaseObject, this is actually cloned database object passed by the query runner.
	// So failure of the transaction won't impact the consistency of the cache
	collection := schema.NewDefaultCollection(schFactory.Name, c.id, schRevision, schFactory.CollectionType, schFactory, searchCollectionName, existingSearch.Fields)

	// recreating collection holder is fine because we are working on databaseClone and also has a lock on the tenant
	database.collections[schFactory.Name] = NewCollectionHolder(c.id, schFactory.Name, collection, c.idxNameToId)

	// update indexing store schema if there is a change
	if deltaFields := schema.GetSearchDeltaFields(c.collection.QueryableFields, schFactory.Fields, existingSearch.Fields); len(deltaFields) > 0 {
		if err := tenant.searchStore.UpdateCollection(ctx, collection.Search.Name, &tsApi.CollectionUpdateSchema{
			Fields: deltaFields,
		}); err != nil {
			return err
		}
	}
	return nil
}

// DropCollection is to drop a collection and its associated indexes. It removes the "created" entry from the encoding
// subspace and adds a "dropped" entry for the same collection key.
func (tenant *Tenant) DropCollection(ctx context.Context, tx transaction.Tx, db *Database, collectionName string) error {
	tenant.Lock()
	defer tenant.Unlock()

	err := tenant.dropCollection(ctx, tx, db, collectionName)
	if err != nil {
		return err
	}

	// the passed database object is cloned copy, so cleanup the entries from the cloned copy as this cloned database
	// may be used in further operations if it is an explicit transaction.
	delete(db.idToCollectionMap, db.collections[collectionName].id)
	delete(db.collections, collectionName)
	return err
}

func (tenant *Tenant) dropCollection(ctx context.Context, tx transaction.Tx, db *Database, collectionName string) error {
	if db == nil {
		return errors.NotFound("database missing")
	}

	cHolder, ok := db.collections[collectionName]
	if !ok {
		return errors.NotFound("collection doesn't exists '%s'", collectionName)
	}

	if err := tenant.metaStore.DropCollection(ctx, tx, cHolder.name, tenant.namespace.Id(), db.id, cHolder.id); err != nil {
		return err
	}

	for idxName, idxId := range cHolder.idxNameToId {
		if err := tenant.metaStore.DropIndex(ctx, tx, idxName, tenant.namespace.Id(), db.id, cHolder.id, idxId); err != nil {
			return err
		}
	}
	if err := tenant.schemaStore.Delete(ctx, tx, tenant.namespace.Id(), db.id, cHolder.id); err != nil {
		return err
	}

	tableName, err := tenant.Encoder.EncodeTableName(tenant.namespace, db, cHolder.collection)
	if err != nil {
		return err
	}
	if err := tenant.TableKeyGenerator.removeCounter(ctx, tx, tableName); err != nil {
		return err
	}

	// TODO: Move actual deletion out of the mutex
	if config.DefaultConfig.Server.FDBHardDrop {
		tableName, err := tenant.Encoder.EncodeTableName(tenant.namespace, db, cHolder.collection)
		if err != nil {
			return err
		}

		if err = tenant.kvStore.DropTable(ctx, tableName); err != nil {
			return err
		}
	}

	if config.DefaultConfig.Search.WriteEnabled {
		if err := tenant.searchStore.DropCollection(ctx, cHolder.collection.SearchCollectionName()); err != nil {
			if err != search.ErrNotFound {
				return err
			}
		}
	}

	return nil
}

func (tenant *Tenant) getSearchCollName(dbName string, collName string) string {
	return fmt.Sprintf("%s-%s-%s", tenant.namespace.StrId(), dbName, collName)
}

func (tenant *Tenant) String() string {
	return fmt.Sprintf("id: %d, name: %s", tenant.namespace.Id(), tenant.namespace.StrId())
}

// Size returns approximate data size on disk for all the collections, databases for this tenant.
func (tenant *Tenant) Size(ctx context.Context) (int64, error) {
	tenant.Lock()
	nsName, _ := tenant.Encoder.EncodeTableName(tenant.namespace, nil, nil)
	tenant.Unlock()

	return tenant.kvStore.TableSize(ctx, nsName)
}

// DatabaseSize returns approximate data size on disk for all the database for this tenant.
func (tenant *Tenant) DatabaseSize(ctx context.Context, db *Database) (int64, error) {
	tenant.Lock()
	nsName, _ := tenant.Encoder.EncodeTableName(tenant.namespace, db, nil)
	tenant.Unlock()

	return tenant.kvStore.TableSize(ctx, nsName)
}

// CollectionSize returns approximate data size on disk for all the collections for the database provided by the caller.
func (tenant *Tenant) CollectionSize(ctx context.Context, db *Database, coll *schema.DefaultCollection) (int64, error) {
	tenant.Lock()

	nsName, _ := tenant.Encoder.EncodeTableName(tenant.namespace, db, coll)
	tenant.Unlock()

	return tenant.kvStore.TableSize(ctx, nsName)
}

// Database is to manage the collections for this database. Check the Clone method before changing this struct.
type Database struct {
	sync.RWMutex

	id                    uint32
	name                  *DatabaseBranch
	collections           map[string]*collectionHolder
	needFixingCollections map[string]struct{}
	idToCollectionMap     map[uint32]string
}

func NewDatabase(id uint32, name string) *Database {
	return &Database{
		id:                    id,
		name:                  NewBranchFromDbName(name),
		collections:           make(map[string]*collectionHolder),
		idToCollectionMap:     make(map[uint32]string),
		needFixingCollections: make(map[string]struct{}),
	}
}

// Clone is used to stage the database.
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

// Name returns the internal database name.
func (d *Database) Name() string {
	return d.name.Name()
}

// Id returns the dictionary encoded value of this collection.
func (d *Database) Id() uint32 {
	return d.id
}

// ListCollection returns the collection object of all the collections in this database.
func (d *Database) ListCollection() []*schema.DefaultCollection {
	d.RLock()
	defer d.RUnlock()

	collections := make([]*schema.DefaultCollection, 0, len(d.collections))
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

func (d *Database) DbName() string {
	return d.name.Db()
}

func (d *Database) BranchName() string {
	return d.name.Branch()
}

func (d *Database) IsBranch() bool {
	return !d.name.IsMain()
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

// clone is used to stage the collectionHolder.
func (c *collectionHolder) clone() *collectionHolder {
	c.Lock()
	defer c.Unlock()

	var copyC collectionHolder
	copyC.id = c.id
	copyC.name = c.name

	var err error
	copyC.collection, err = createCollection(c.id, int(c.collection.SchVer), c.name, c.collection.Schema, c.idxNameToId, c.collection.SearchCollectionName(), c.collection.FieldsInSearch)
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

func createCollection(id uint32, schVer int, name string, revision []byte, idxNameToId map[string]uint32, searchCollectionName string, fieldsInSearch []tsApi.Field) (*schema.DefaultCollection, error) {
	schFactory, err := schema.Build(name, revision)
	if err != nil {
		return nil, err
	}

	indexes := schFactory.Indexes.GetIndexes()
	for _, index := range indexes {
		id, ok := idxNameToId[index.Name]
		if !ok {
			return nil, errors.NotFound("dictionary encoding is missing for index '%s'", index.Name)
		}
		index.Id = id
	}

	schFactory.Schema = revision

	return schema.NewDefaultCollection(name, id, schVer, schFactory.CollectionType, schFactory, searchCollectionName, fieldsInSearch), nil
}

func isSchemaEq(s1, s2 []byte) (bool, error) {
	var j, j2 interface{}
	if err := jsoniter.Unmarshal(s1, &j); err != nil {
		return false, err
	}
	if err := jsoniter.Unmarshal(s2, &j2); err != nil {
		return false, err
	}
	return reflect.DeepEqual(j2, j), nil
}

// NewTestTenantMgr creates new TenantManager for tests.
func NewTestTenantMgr(kvStore kv.KeyValueStore) (*TenantManager, context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	m := newTenantManager(kvStore, &search.NoopStore{}, &TestMDNameRegistry{
		ReserveSB:  fmt.Sprintf("test_tenant_reserve_%x", rand.Uint64()),  //nolint:golint,gosec
		EncodingSB: fmt.Sprintf("test_tenant_encoding_%x", rand.Uint64()), //nolint:golint,gosec
		SchemaSB:   fmt.Sprintf("test_tenant_schema_%x", rand.Uint64()),   //nolint:golint,gosec
	},
		transaction.NewManager(kvStore),
	)

	_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.SchemaSubspaceName())

	return m, ctx, cancel
}
