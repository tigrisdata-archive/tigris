// Copyright 2022-2023 Tigris Data, Inc.
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
	"testing"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/errors"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/config"
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

type TenantGetter interface {
	GetTenant(ctx context.Context, id string) (*Tenant, error)
}

// TenantNamespace is used when there is a finer isolation of databases is needed. The caller provides a unique
// id and strId to this namespace which is used by the cluster to create a namespace.
type TenantNamespace struct {
	lookupStrId string
	lookupId    uint32
	metadata    NamespaceMetadata
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

	metaStore         *Dictionary
	kvStore           kv.KeyValueStore
	searchStore       search.Store
	tenants           map[string]*Tenant
	idToTenantMap     map[uint32]string
	version           Version
	versionH          *VersionHandler
	mdNameRegistry    *NameRegistry
	encoder           Encoder
	tableKeyGenerator *TableKeyGenerator
	txMgr             *transaction.Manager
}

func (m *TenantManager) GetNamespaceStore() *NamespaceSubspace {
	return m.metaStore.Namespace()
}

func (m *TenantManager) GetVersionHandler() *VersionHandler {
	return m.versionH
}

func NewTenantManager(kvStore kv.KeyValueStore, searchStore search.Store, txMgr *transaction.Manager) *TenantManager {
	return newTenantManager(kvStore, searchStore, DefaultNameRegistry, txMgr)
}

func newTenantManager(kvStore kv.KeyValueStore, searchStore search.Store, mdNameRegistry *NameRegistry, txMgr *transaction.Manager) *TenantManager {
	return &TenantManager{
		kvStore:           kvStore,
		searchStore:       searchStore,
		encoder:           NewEncoder(),
		metaStore:         NewMetadataDictionary(mdNameRegistry),
		tenants:           make(map[string]*Tenant),
		idToTenantMap:     make(map[uint32]string),
		versionH:          newVersionHandler(mdNameRegistry.GetVersionKey()),
		mdNameRegistry:    mdNameRegistry,
		tableKeyGenerator: NewTableKeyGenerator(),
		txMgr:             txMgr,
	}
}

func (m *TenantManager) EnsureDefaultNamespace() error {
	var err error
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = m.CreateOrGetTenant(ctx, NewDefaultNamespace())
		cancel()
		if err != kv.ErrConflictingTransaction {
			return err
		}
		time.Sleep(1 * time.Second)
	}
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

	return m.tenants[namespaceName]
}

func (m *TenantManager) GetNamespaceNames() []string {
	m.RLock()
	defer m.RUnlock()

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
func (m *TenantManager) GetTenant(ctx context.Context, namespaceName string) (*Tenant, error) {
	var (
		tenant *Tenant
		err    error
	)

	if tenant = m.getTenantFromCache(namespaceName); tenant != nil {
		return tenant, nil
	}

	m.Lock()
	defer m.Unlock()
	var found bool

	if tenant, found = m.tenants[namespaceName]; found {
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
	tenant = NewTenant(namespace, m.kvStore, m.searchStore,
		m.metaStore, m.encoder, m.versionH, currentVersion, m.tableKeyGenerator)
	if err = tenant.reload(ctx, tx, currentVersion, collectionsInSearch); err != nil {
		return nil, err
	}

	return tenant, nil
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
		tenant := NewTenant(namespace, m.kvStore, m.searchStore, m.metaStore, m.encoder, m.versionH, currentVersion, m.tableKeyGenerator)
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

	return NewTenant(namespace, m.kvStore, m.searchStore, m.metaStore, m.encoder, m.versionH, nil, m.tableKeyGenerator), nil
}

// GetTableFromIds returns tenant name, database object, collection name corresponding to their encoded ids.
func (m *TenantManager) GetTableFromIds(tenantId uint32, dbId uint32, collId uint32) (string, *Database, string, bool) {
	m.RLock()
	defer m.RUnlock()

	// get tenant info
	tenantName, ok := m.idToTenantMap[tenantId]
	if !ok {
		return "", nil, "", ok
	}
	tenant, ok := m.tenants[tenantName]
	if !ok {
		return "", nil, "", ok
	}

	// get db info
	dbObj, ok := tenant.idToDatabaseMap[dbId]
	if !ok {
		return tenantName, nil, "", ok
	}

	// finally, the collection
	collName, ok := dbObj.idToCollectionMap[collId]
	if !ok {
		return tenantName, dbObj, "", ok
	}
	return tenantName, dbObj, collName, ok
}

func (m *TenantManager) DecodeTableName(tableName []byte) (string, *Database, string, bool) {
	n, d, c, ok := m.encoder.DecodeTableName(tableName)
	if !ok {
		return "", nil, "", false
	}
	tenantName, db, collName, ok := m.GetTableFromIds(n, d, c)
	if !ok {
		return "", nil, "", false
	}
	return tenantName, db, collName, ok
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
			m.tenants[namespace] = NewTenant(NewTenantNamespace(namespace, metadata), m.kvStore, m.searchStore, m.metaStore, m.encoder, m.versionH, currentVersion, m.tableKeyGenerator)
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
	searchSchemaStore *SearchSchemaSubspace
	namespaceStore    *NamespaceSubspace
	metaStore         *Dictionary
	Encoder           Encoder
	namespace         Namespace
	version           Version
	versionH          *VersionHandler
	TableKeyGenerator *TableKeyGenerator
	// projects keeps a mapping of project name to project
	projects map[string]*Project
	// idToDatabaseMap is a mapping of dictionary encoded ids to Database object. This includes all the database branches
	// as well. This is needed because in a row we have database id which may be for a database branch so just keeping
	// the projects mapping above is not sufficient for us.
	idToDatabaseMap map[uint32]*Database
}

func NewTenant(namespace Namespace, kvStore kv.KeyValueStore, searchStore search.Store, dict *Dictionary,
	encoder Encoder, versionH *VersionHandler, currentVersion Version, _ *TableKeyGenerator,
) *Tenant {
	return &Tenant{
		kvStore:           kvStore,
		searchStore:       searchStore,
		namespace:         namespace,
		metaStore:         dict,
		schemaStore:       dict.Schema(),
		searchSchemaStore: dict.SearchSchema(),
		namespaceStore:    dict.Namespace(),
		projects:          make(map[string]*Project),
		idToDatabaseMap:   make(map[uint32]*Database),
		versionH:          versionH,
		version:           currentVersion,
		Encoder:           encoder,
	}
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

	indexesInSearchStore, err := tenant.searchStore.AllCollections(ctx)
	if err != nil {
		return err
	}
	return tenant.reload(ctx, tx, version, indexesInSearchStore)
}

func (tenant *Tenant) shouldReload(currentVersion Version) bool {
	tenant.RLock()
	defer tenant.RUnlock()

	return bytes.Compare(currentVersion, tenant.version) > 0
}

// reload is a single point of reloading/refreshing all the resources for a tenant. This method first reads all the
// databases for this tenant and then link these databases back to the project. Note, a project only has a single
// database, but we support database branches which means there can be more than one database inside a project. Once it
// loads all the databases, it loads the resources for each one. Once databases are reloaded then it performs the same
// logic for search indexes. Once search indexes are loaded it links back the search indexes to the Tigris Collection
// if the source for these search indexes is Tigris.
func (tenant *Tenant) reload(ctx context.Context, tx transaction.Tx, currentVersion Version, indexesInSearchStore map[string]*tsApi.CollectionResponse) error {
	// reset
	tenant.projects = make(map[string]*Project)
	tenant.idToDatabaseMap = make(map[uint32]*Database)

	dbs, err := tenant.metaStore.GetDatabases(ctx, tx, tenant.namespace.Id())
	if err != nil {
		return err
	}

	// load projects
	for name, meta := range dbs {
		databaseName := NewDatabaseName(name)
		if databaseName.IsMainBranch() {
			// we don't care about branches here so the main database here means a project.
			tenant.projects[databaseName.Name()] = NewProject(meta.ID, name)
		}
	}

	// Iterate one more time on all the databases and now add branches and main database to the Project object
	for name, meta := range dbs {
		database, err := tenant.reloadDatabase(ctx, tx, name, meta.ID, indexesInSearchStore)
		if ulog.E(err) {
			return err
		}

		project := tenant.projects[database.DbName()] // get the parent project or parent db using DbName()
		if database.IsBranch() {
			project.databaseBranches[database.Name()] = database
		} else {
			project.database = database
		}

		tenant.idToDatabaseMap[meta.ID] = database
	}

	// load search indexes, this is essentially loading all the search indexes created by the user and attaching it to
	// the project object.
	for _, p := range tenant.projects {
		var err error
		if p.search, err = tenant.reloadSearch(ctx, tx, p, indexesInSearchStore); err != nil {
			return err
		}
		for _, index := range p.search.indexes {
			// we maintain a back pointer inside the collection object for the indexes that have the source as Tigris.
			if index.Source.Type == schema.SearchSourceTigris {
				database := p.database
				if len(index.Source.DatabaseBranch) > 0 {
					database = p.databaseBranches[index.Source.DatabaseBranch]
				}

				if database != nil {
					if collection := database.GetCollection(index.Source.CollectionName); collection != nil {
						collection.AddSearchIndex(index)
					}
				}
			}
		}
	}

	tenant.version = currentVersion

	return nil
}

// reloadDatabase is called by tenant to reload the database state. This also loads all the collections that are part of
// this database and implicit search index for these collections.
func (tenant *Tenant) reloadDatabase(ctx context.Context, tx transaction.Tx, dbName string, dbId uint32,
	indexesInSearchStore map[string]*tsApi.CollectionResponse,
) (*Database, error) {
	database := NewDatabase(dbId, dbName)

	colls, err := tenant.metaStore.GetCollections(ctx, tx, tenant.namespace.Id(), database.id)
	if err != nil {
		return nil, err
	}

	for coll, meta := range colls {
		idxMeta, err := tenant.metaStore.GetIndexes(ctx, tx, tenant.namespace.Id(), database.id, meta.ID)
		if err != nil {
			database.needFixingCollections[coll] = struct{}{}
			log.Debug().Err(err).Str("collection", coll).Msg("skipping loading collection")
			continue
		}

		schemas, err := tenant.schemaStore.Get(ctx, tx, tenant.namespace.Id(), database.id, meta.ID)
		if err != nil {
			database.needFixingCollections[coll] = struct{}{}
			log.Debug().Err(err).Str("collection", coll).Msg("skipping loading collection")
			continue
		}

		var fieldsInSearch []tsApi.Field
		searchCollectionName := tenant.getSearchCollName(dbName, coll)
		if searchSchema, ok := indexesInSearchStore[searchCollectionName]; ok {
			fieldsInSearch = searchSchema.Fields
		}
		if len(fieldsInSearch) == 0 {
			log.Error().Str("search_collection", searchCollectionName).Msg("fields are not present in search")
		}

		collection, err := createCollection(meta.ID, coll, schemas, idxMeta, searchCollectionName, fieldsInSearch)
		if err != nil {
			database.needFixingCollections[coll] = struct{}{}
			log.Debug().Err(err).Str("collection", coll).Msg("skipping loading collection")
			continue
		}

		encName, err := tenant.Encoder.EncodeTableName(tenant.namespace, database, collection)
		if err != nil {
			return nil, err
		}
		collection.EncodedName = encName

		database.collections[coll] = newCollectionHolder(meta.ID, coll, collection, idxMeta)
		database.idToCollectionMap[meta.ID] = coll
	}

	return database, nil
}

// reloadSearch is responsible for reloading all the search indexes inside a single project.
func (tenant *Tenant) reloadSearch(ctx context.Context, tx transaction.Tx, project *Project, indexesInSearchStore map[string]*tsApi.CollectionResponse) (*Search, error) {
	projMetadata, err := tenant.namespaceStore.GetProjectMetadata(ctx, tx, tenant.namespace.Id(), project.Name())
	if err != nil {
		return nil, errors.Internal("failed to get project metadata for project %s", project.Name())
	}

	searchObj := NewSearch()

	for _, searchMD := range projMetadata.SearchMetadata {
		schV, err := tenant.searchSchemaStore.GetLatest(ctx, tx, tenant.namespace.Id(), project.id, searchMD.Name)
		if err != nil {
			return nil, err
		}

		searchFactory, err := schema.BuildSearch(searchMD.Name, schV.Schema)
		if err != nil {
			log.Err(err).Msgf("seeing rebuilding failure for search index %s", searchMD.Name)
			continue
		}

		var fieldsInSearchStore []tsApi.Field
		searchStoreIndexName := tenant.Encoder.EncodeSearchTableName(tenant.namespace.Id(), project.Id(), searchMD.Name)
		if searchIndexInStore, ok := indexesInSearchStore[searchStoreIndexName]; ok {
			fieldsInSearchStore = searchIndexInStore.Fields
		}
		if len(fieldsInSearchStore) == 0 {
			log.Error().Str("search_collection", searchStoreIndexName).Msg("fields are not present in search")
		}
		searchObj.indexes[searchMD.Name] = schema.NewSearchIndex(schV.Version, searchStoreIndexName, searchFactory, fieldsInSearchStore)
	}

	return searchObj, nil
}

// GetNamespace returns the namespace of this tenant.
func (tenant *Tenant) GetNamespace() Namespace {
	tenant.RLock()
	defer tenant.RUnlock()

	return tenant.namespace
}

func (tenant *Tenant) CreateSearchIndex(ctx context.Context, tx transaction.Tx, project *Project, factory *schema.SearchFactory) error {
	tenant.Lock()
	defer tenant.Unlock()

	return tenant.createSearchIndex(ctx, tx, project, factory)
}

func (tenant *Tenant) createSearchIndex(ctx context.Context, tx transaction.Tx, project *Project, factory *schema.SearchFactory) error {
	if index, ok := project.search.GetIndex(factory.Name); ok {
		if eq, err := isSchemaEq(index.Schema, factory.Schema); eq || err != nil {
			// shortcut to just check if schema is eq then return early
			return err
		}
		return tenant.updateSearchIndex(ctx, tx, project, factory, index)
	}

	metadata, err := tenant.namespaceStore.GetProjectMetadata(ctx, tx, tenant.namespace.Id(), project.Name())
	if err != nil {
		return errors.Internal("failed to get project metadata for project %s", project.Name())
	}

	metadata.SearchMetadata = append(metadata.SearchMetadata, SearchMetadata{
		Name:      factory.Name,
		Creator:   factory.Sub,
		CreatedAt: time.Now().Unix(),
	})

	if err = tenant.namespaceStore.UpdateProjectMetadata(ctx, tx, tenant.namespace.Id(), project.Name(), metadata); err != nil {
		return errors.Internal("failed to update project metadata for index creation")
	}

	// store schema now in searchSchemaStore
	if err := tenant.searchSchemaStore.Put(ctx, tx, tenant.namespace.Id(), project.id, factory.Name, factory.Schema, baseSchemaVersion); err != nil {
		return err
	}

	indexNameInStore := tenant.Encoder.EncodeSearchTableName(tenant.namespace.Id(), project.id, factory.Name)
	index := schema.NewSearchIndex(baseSchemaVersion, indexNameInStore, factory, nil)
	if err := tenant.searchStore.CreateCollection(ctx, index.StoreSchema); err != nil {
		if !search.IsErrDuplicateEntity(err) {
			return err
		}
	}

	project.search.AddIndex(index)

	return nil
}

func (tenant *Tenant) updateSearchIndex(ctx context.Context, tx transaction.Tx, project *Project, factory *schema.SearchFactory, index *schema.SearchIndex) error {
	// first apply schema change whether it conforms to the backward compatibility rules.
	if err := schema.ApplySearchIndexSchemaRules(index, factory); err != nil {
		return err
	}

	version := index.Version + 1
	if err := tenant.searchSchemaStore.Put(ctx, tx, tenant.namespace.Id(), project.id, factory.Name, factory.Schema, version); err != nil {
		return err
	}

	previousIndexInStore, err := tenant.searchStore.DescribeCollection(ctx, index.StoreIndexName())
	if err != nil {
		return err
	}

	updatedIndex := schema.NewSearchIndex(version, index.StoreIndexName(), factory, previousIndexInStore.Fields)

	// update indexing store schema if there is a change
	if deltaFields := schema.GetSearchDeltaFields(true, index.QueryableFields, updatedIndex.Fields, previousIndexInStore.Fields); len(deltaFields) > 0 {
		if err := tenant.searchStore.UpdateCollection(ctx, updatedIndex.StoreIndexName(), &tsApi.CollectionUpdateSchema{
			Fields: deltaFields,
		}); err != nil {
			return err
		}
	}

	project.search.AddIndex(updatedIndex)
	return nil
}

func (tenant *Tenant) GetSearchIndex(_ context.Context, _ transaction.Tx, project *Project, indexName string) (*schema.SearchIndex, error) {
	tenant.Lock()
	defer tenant.Unlock()

	index, ok := project.search.GetIndex(indexName)
	if !ok {
		return nil, NewSearchIndexNotFoundErr(indexName)
	}

	return index, nil
}

func (tenant *Tenant) DeleteSearchIndex(ctx context.Context, tx transaction.Tx, project *Project, indexName string) error {
	tenant.Lock()
	defer tenant.Unlock()

	index, ok := project.search.GetIndex(indexName)
	if !ok {
		return NewSearchIndexNotFoundErr(indexName)
	}

	return tenant.deleteSearchIndex(ctx, tx, project, index)
}

func (tenant *Tenant) deleteSearchIndex(ctx context.Context, tx transaction.Tx, project *Project, index *schema.SearchIndex) error {
	metadata, err := tenant.namespaceStore.GetProjectMetadata(ctx, tx, tenant.namespace.Id(), project.name)
	if err != nil {
		return errors.Internal("failed to get project metadata for project %s", project.name)
	}

	foundIdx := -1
	for i := range metadata.SearchMetadata {
		if metadata.SearchMetadata[i].Name == index.Name {
			foundIdx = i
			break
		}
	}
	if foundIdx == -1 {
		return NewSearchIndexNotFoundErr(index.Name)
	}

	metadata.SearchMetadata[foundIdx] = metadata.SearchMetadata[len(metadata.SearchMetadata)-1]
	metadata.SearchMetadata = metadata.SearchMetadata[:len(metadata.SearchMetadata)-1]
	if err = tenant.namespaceStore.UpdateProjectMetadata(ctx, tx, tenant.namespace.Id(), project.name, metadata); err != nil {
		return errors.Internal("failed to update project metadata for cache deletion")
	}

	// cleanup all the schemas
	if err = tenant.searchSchemaStore.Delete(ctx, tx, tenant.namespace.Id(), project.Id(), index.Name); err != nil {
		return errors.Internal(err.Error())
	}

	// clean up from the underlying search store
	if err := tenant.searchStore.DropCollection(ctx, index.StoreIndexName()); err != nil && !search.IsErrNotFound(err) {
		return err
	}

	return nil
}

func (tenant *Tenant) ListSearchIndexes(_ context.Context, _ transaction.Tx, project *Project) ([]*schema.SearchIndex, error) {
	tenant.Lock()
	defer tenant.Unlock()

	indexes := make([]*schema.SearchIndex, len(project.search.indexes))
	i := 0
	for _, idx := range project.search.indexes {
		indexes[i] = idx
		i++
	}
	return indexes, nil
}

func (tenant *Tenant) CreateCache(ctx context.Context, tx transaction.Tx, project string, cache string, currentSub string) (bool, error) {
	tenant.Lock()
	defer tenant.Unlock()

	projMetadata, err := tenant.namespaceStore.GetProjectMetadata(ctx, tx, tenant.namespace.Id(), project)
	if err != nil {
		return false, errors.Internal("Failed to get project metadata for project %s", project)
	}
	if projMetadata.CachesMetadata == nil {
		projMetadata.CachesMetadata = []CacheMetadata{}
	}
	for i := range projMetadata.CachesMetadata {
		if projMetadata.CachesMetadata[i].Name == cache {
			return false, NewCacheExistsErr(cache)
		}
	}
	projMetadata.CachesMetadata = append(projMetadata.CachesMetadata, CacheMetadata{
		Name:      cache,
		Creator:   currentSub,
		CreatedAt: time.Now().Unix(),
	})
	err = tenant.namespaceStore.UpdateProjectMetadata(ctx, tx, tenant.namespace.Id(), project, projMetadata)
	if err != nil {
		return false, errors.Internal("Failed to update project metadata for cache creation")
	}
	return true, nil
}

func (tenant *Tenant) ListCaches(ctx context.Context, tx transaction.Tx, project string) ([]string, error) {
	tenant.Lock()
	defer tenant.Unlock()
	projMetadata, err := tenant.namespaceStore.GetProjectMetadata(ctx, tx, tenant.namespace.Id(), project)
	if err != nil {
		return nil, errors.Internal("Failed to get project metadata for project %s", project)
	}
	if projMetadata.CachesMetadata == nil {
		return []string{}, nil
	}
	caches := make([]string, len(projMetadata.CachesMetadata))
	for i := range projMetadata.CachesMetadata {
		caches[i] = projMetadata.CachesMetadata[i].Name
	}
	return caches, nil
}

func (tenant *Tenant) DeleteCache(ctx context.Context, tx transaction.Tx, project string, cache string) (bool, error) {
	tenant.Lock()
	defer tenant.Unlock()

	projMetadata, err := tenant.namespaceStore.GetProjectMetadata(ctx, tx, tenant.namespace.Id(), project)
	if err != nil {
		return false, errors.Internal("Failed to get project metadata for project %s", project)
	}
	if projMetadata.CachesMetadata == nil {
		projMetadata.CachesMetadata = []CacheMetadata{}
	}
	var tempCachesMetadata []CacheMetadata
	var found bool
	for i := range projMetadata.CachesMetadata {
		if projMetadata.CachesMetadata[i].Name != cache {
			tempCachesMetadata = append(tempCachesMetadata, projMetadata.CachesMetadata[i])
		} else {
			found = true
		}
	}
	if !found {
		return false, NewCacheNotFoundErr(cache)
	}
	projMetadata.CachesMetadata = tempCachesMetadata

	err = tenant.namespaceStore.UpdateProjectMetadata(ctx, tx, tenant.namespace.Id(), project, projMetadata)
	if err != nil {
		return false, errors.Internal("Failed to update project metadata for cache deletion")
	}

	return true, nil
}

// CreateProject is responsible for creating a Project. This includes creating a dictionary encoding entry for the main
// database that will be attached to this project. This method is not adding the entry to the tenant because the outer
// layer may still roll back the transaction. The session manager is bumping the metadata version once the commit is
// successful so reloading happens at the next call when a transaction sees a stale tenant version. This applies to the
// reloading mechanism on all the servers. It returns "true" If the project already exists, else "false" and an error. The
// project metadata if not nil is also added inside this transaction.
func (tenant *Tenant) CreateProject(ctx context.Context, tx transaction.Tx, projName string, projMetadata *ProjectMetadata) error {
	tenant.Lock()
	defer tenant.Unlock()

	return tenant.createProject(ctx, tx, projName, projMetadata)
}

func (tenant *Tenant) createProject(ctx context.Context, tx transaction.Tx, projName string, projMetadata *ProjectMetadata) error {
	if _, ok := tenant.projects[projName]; ok {
		return kv.ErrDuplicateKey
	}

	// otherwise, proceed to create the database if there are concurrent requests on different workers then one of
	// them will fail with duplicate entry and only one will succeed.
	meta, err := tenant.metaStore.CreateDatabase(ctx, tx, projName, tenant.namespace.Id())
	if err != nil {
		return err
	}

	if projMetadata == nil {
		projMetadata = &ProjectMetadata{}
	}

	// add id to the project, which is same as main database id of this project.
	projMetadata.ID = meta.ID
	if err = tenant.namespaceStore.InsertProjectMetadata(ctx, tx, tenant.namespace.Id(), projName, projMetadata); err != nil {
		log.Err(err).Msg("failed to insert database metadata")
		return errors.Internal("failed to setup project metadata")
	}

	return err
}

// DeleteProject is responsible for first dropping a dictionary encoding of the main database attached to this project
// and then adding a corresponding dropped encoding entry in the encoding table. This API returns "false" if the project
// doesn't exist so that caller can reason about it. DeleteProject is more involved than CreateProject as with deletion
// we also need to iterate over all the collections present in the main database and database branches and call drop
// collection on each one of them. Returns "False" if the project doesn't exist.
func (tenant *Tenant) DeleteProject(ctx context.Context, tx transaction.Tx, projName string) (bool, error) {
	tenant.Lock()
	defer tenant.Unlock()

	// check first if the project exists
	proj, found := tenant.projects[projName]
	if !found {
		return false, nil
	}

	// iterate over each branch to delete it
	for _, branch := range proj.databaseBranches {
		if err := tenant.deleteBranch(ctx, tx, proj, NewDatabaseNameWithBranch(branch.DbName(), branch.BranchName())); err != nil {
			return true, err
		}
	}

	// delete the main branch, collections and associated metadata if there are concurrent requests on different workers
	// then one of them will fail with duplicate entry and only one will succeed.
	if err := tenant.metaStore.DropDatabase(ctx, tx, proj.Name(), tenant.namespace.Id()); err != nil {
		return true, err
	}

	for _, c := range proj.database.collections {
		if err := tenant.dropCollection(ctx, tx, proj.database, c.collection.Name); err != nil {
			return true, err
		}
	}

	for key := range proj.search.indexes {
		if err := tenant.deleteSearchIndex(ctx, tx, proj, proj.search.indexes[key]); err != nil {
			return true, err
		}
	}

	// drop metadata entry
	if err := tenant.namespaceStore.DeleteProjectMetadata(ctx, tx, tenant.namespace.Id(), projName); err != nil {
		log.Err(err).Msg("failed to delete project metadata")
		return false, errors.Internal("failed to delete project metadata")
	}

	return true, nil
}

// GetProject returns the project object, or null if there is no project with the name passed in the param.
// As reloading of tenant state is happening at the session manager layer so GetProject calls assume that the caller
// just needs the state from the cache.
func (tenant *Tenant) GetProject(projName string) (*Project, error) {
	tenant.RLock()
	defer tenant.RUnlock()

	proj, ok := tenant.projects[projName]
	if !ok {
		return nil, NewProjectNotFoundErr(projName)
	}

	return proj, nil
}

// ListProjects is used to list all projects available for this tenant.
func (tenant *Tenant) ListProjects(_ context.Context) []string {
	tenant.RLock()
	defer tenant.RUnlock()

	projects := make([]string, len(tenant.projects))
	i := 0
	for name := range tenant.projects {
		projects[i] = name
		i++
	}

	return projects
}

// CreateBranch is used to create a database branch. A database branch is essentially a schema-only copy of a database.
// A new database is created in the tenant namespace and all the collection schemas from primary database are created
// in this branch. A branch may drift overtime from the primary database.
func (tenant *Tenant) CreateBranch(ctx context.Context, tx transaction.Tx, projName string, dbName *DatabaseName) error {
	tenant.Lock()
	defer tenant.Unlock()

	// first get the project
	proj, ok := tenant.projects[projName]
	if !ok {
		return NewProjectNotFoundErr(projName)
	}

	if _, ok = proj.databaseBranches[dbName.Name()]; ok {
		return NewDatabaseBranchExistsErr(dbName.Branch())
	}

	// Create a database branch
	branchMeta, err := tenant.metaStore.CreateDatabase(ctx, tx, dbName.Name(), tenant.namespace.Id())
	if err != nil {
		return err
	}

	// Create collections inside the new database branch
	branch := NewDatabase(branchMeta.ID, dbName.Name())
	for _, coll := range proj.database.ListCollection() {
		schFactory, err := schema.Build(coll.Name, coll.Schema)
		if err != nil {
			return err
		}

		if err = tenant.createCollection(ctx, tx, branch, schFactory); err != nil {
			return err
		}
	}

	return nil
}

// DeleteBranch is responsible for deleting a database branch. Throws error if database/branch does not exist
// or if 'main' branch is being deleted.
func (tenant *Tenant) DeleteBranch(ctx context.Context, tx transaction.Tx, projName string, dbBranch *DatabaseName) error {
	tenant.Lock()
	defer tenant.Unlock()

	if dbBranch.IsMainBranch() {
		return NewMetadataError(ErrCodeCannotDeleteBranch, "'main' database cannot be deleted.")
	}

	proj, found := tenant.projects[projName]
	if !found {
		return NewProjectNotFoundErr(projName)
	}

	return tenant.deleteBranch(ctx, tx, proj, dbBranch)
}

func (tenant *Tenant) deleteBranch(ctx context.Context, tx transaction.Tx, project *Project, dbBranch *DatabaseName) error {
	// check first if it exists
	branch, ok := project.databaseBranches[dbBranch.Name()]
	if !ok {
		return NewBranchNotFoundErr(dbBranch.Name())
	}

	// drop the dictionary encoding for this database branch.
	if err := tenant.metaStore.DropDatabase(ctx, tx, branch.Name(), tenant.namespace.Id()); err != nil {
		return err
	}

	// cleanup all the collections
	for _, c := range branch.collections {
		if err := tenant.dropCollection(ctx, tx, branch, c.collection.Name); err != nil {
			return err
		}

		for _, index := range c.collection.SearchIndexes {
			if err := tenant.deleteSearchIndex(ctx, tx, project, index); err != nil {
				return err
			}
		}
	}
	return nil
}

// ListDatabaseBranches returns an array of branch names associated with this database including "main" branch.
func (tenant *Tenant) ListDatabaseBranches(projName string) []string {
	tenant.Lock()
	defer tenant.Unlock()

	project, ok := tenant.projects[projName]
	if !ok {
		return nil
	}

	branchNames := make([]string, len(project.databaseBranches)+1)
	branchNames[0] = project.database.BranchName()

	i := 1
	for name := range project.databaseBranches {
		branchNames[i] = project.databaseBranches[name].BranchName()
		i++
	}
	return branchNames
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

	collMeta, err := tenant.metaStore.CreateCollection(ctx, tx, schFactory.Name, tenant.namespace.Id(), database.id)
	if err != nil {
		return err
	}

	// encode indexes and add this back in the collection
	indexes := schFactory.Indexes.GetIndexes()
	idxMeta := make(map[string]*IndexMetadata)
	for _, i := range indexes {
		imeta, err := tenant.metaStore.CreateIndex(ctx, tx, i.Name, tenant.namespace.Id(), database.id, collMeta.ID)
		if err != nil {
			return err
		}

		idxMeta[i.Name] = imeta
		i.Id = imeta.ID
	}

	// all good now persist the schema
	if err = tenant.schemaStore.Put(ctx, tx, tenant.namespace.Id(), database.id, collMeta.ID, schFactory.Schema, baseSchemaVersion); err != nil {
		return err
	}

	// store the collection to the databaseObject, this is actually cloned database object passed by the query runner.
	// So failure of the transaction won't impact the consistency of the cache
	implicitSearchIndex := schema.NewImplicitSearchIndex(
		schFactory.Name,
		tenant.getSearchCollName(database.Name(), schFactory.Name),
		schFactory.Fields,
		nil,
	)

	collection, err := schema.NewDefaultCollection(
		collMeta.ID,
		baseSchemaVersion,
		schFactory,
		nil,
		implicitSearchIndex,
	)
	if err != nil {
		return err
	}

	encName, err := tenant.Encoder.EncodeTableName(tenant.namespace, database, collection)
	if err != nil {
		return err
	}

	collection.EncodedName = encName

	database.collections[schFactory.Name] = newCollectionHolder(collMeta.ID, schFactory.Name, collection, idxMeta)
	if config.DefaultConfig.Search.WriteEnabled {
		// only creating implicit index here
		if err := tenant.searchStore.CreateCollection(ctx, implicitSearchIndex.StoreSchema); err != nil {
			if !search.IsErrDuplicateEntity(err) {
				return err
			}
		}
	}
	return nil
}

func (tenant *Tenant) updateCollection(ctx context.Context, tx transaction.Tx, database *Database, c *collectionHolder, schFactory *schema.Factory) error {
	var err error

	for _, idx := range schFactory.Indexes.GetIndexes() {
		meta, ok := c.idxMeta[idx.Name]
		if !ok {
			// these are the new indexes present in the new collection
			meta, err = tenant.metaStore.CreateIndex(ctx, tx, idx.Name, tenant.namespace.Id(), database.id, c.id)
			if err != nil {
				return err
			}
			c.addIndex(idx.Name, meta)
		}

		idx.Id = meta.ID
	}

	existingCollection := c.collection

	// now validate if the new collection(schema) conforms to the backward compatibility rules.
	if err := schema.ApplySchemaRules(existingCollection, schFactory); err != nil {
		return err
	}

	schRevision := int(c.collection.GetVersion()) + 1
	if err := tenant.schemaStore.Put(ctx, tx, tenant.namespace.Id(), database.id, c.id, schFactory.Schema, schRevision); err != nil {
		return err
	}

	allSchemas, err := tenant.schemaStore.Get(ctx, tx, tenant.namespace.Id(), database.id, c.id)
	if err != nil {
		return err
	}

	existingSearch := &tsApi.CollectionResponse{}
	if config.DefaultConfig.Search.WriteEnabled {
		existingSearch, err = tenant.searchStore.DescribeCollection(ctx, existingCollection.ImplicitSearchIndex.StoreIndexName())
		if err != nil {
			return err
		}
	}

	updatedSearchIndex := schema.NewImplicitSearchIndex(
		schFactory.Name,
		existingCollection.ImplicitSearchIndex.StoreIndexName(),
		schFactory.Fields,
		existingSearch.Fields,
	)

	// store the collection to the databaseObject, this is actually cloned database object passed by the query runner.
	// So failure of the transaction won't impact the consistency of the cache
	collection, err := schema.NewDefaultCollection(
		c.id,
		schRevision,
		schFactory,
		allSchemas,
		updatedSearchIndex,
	)
	if err != nil {
		return err
	}

	encName, err := tenant.Encoder.EncodeTableName(tenant.namespace, database, collection)
	if err != nil {
		return err
	}

	collection.EncodedName = encName

	// recreating collection holder is fine because we are working on databaseClone and also has a lock on the tenant
	database.collections[schFactory.Name] = newCollectionHolder(c.id, schFactory.Name, collection, c.idxMeta)

	if config.DefaultConfig.Search.WriteEnabled {
		// update indexing store schema if there is a change
		if deltaFields := schema.GetSearchDeltaFields(false, existingCollection.ImplicitSearchIndex.QueryableFields, schFactory.Fields, existingSearch.Fields); len(deltaFields) > 0 {
			if err := tenant.searchStore.UpdateCollection(ctx, collection.ImplicitSearchIndex.StoreIndexName(), &tsApi.CollectionUpdateSchema{
				Fields: deltaFields,
			}); err != nil {
				return err
			}
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

	if err := tenant.metaStore.DropCollection(ctx, tx, cHolder.name, tenant.namespace.Id(), db.id); err != nil {
		return err
	}

	for idxName := range cHolder.idxMeta {
		if err := tenant.metaStore.DropIndex(ctx, tx, idxName, tenant.namespace.Id(), db.id, cHolder.id); err != nil {
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
		if err := tenant.searchStore.DropCollection(ctx, cHolder.collection.ImplicitSearchIndex.StoreIndexName()); err != nil {
			if !search.IsErrNotFound(err) {
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

type Project struct {
	sync.RWMutex

	// project shares the same id as the main database
	id               uint32
	name             string
	search           *Search
	database         *Database
	databaseBranches map[string]*Database
}

// NewProject is to create a project, this is only done during reloading from the database as tenant attaches the main
// database and branches to this object.
func NewProject(id uint32, name string) *Project {
	return &Project{
		id:               id,
		name:             name,
		databaseBranches: make(map[string]*Database),
	}
}

// Name returns the project name.
func (p *Project) Name() string {
	return p.name
}

// Id returns the dictionary encoded value of the main database of this project.
func (p *Project) Id() uint32 {
	return p.id
}

// GetDatabaseWithBranches returns main database and all the corresponding database branches.
func (p *Project) GetDatabaseWithBranches() []*Database {
	databases := make([]*Database, len(p.databaseBranches)+1)
	databases[0] = p.database

	i := 1
	for _, database := range p.databaseBranches {
		databases[i] = database
		i++
	}

	return databases
}

// GetMainDatabase returns the main database of this project.
func (p *Project) GetMainDatabase() *Database {
	return p.database
}

// GetSearch returns the search for this project which will have all search indexes.
func (p *Project) GetSearch() *Search {
	return p.search
}

// GetDatabase returns either the main database or a database branch. This depends on the DatabaseName object.
func (p *Project) GetDatabase(databaseName *DatabaseName) (*Database, error) {
	if databaseName.IsMainBranch() {
		return p.database, nil
	}

	branch, ok := p.databaseBranches[databaseName.Name()]
	if !ok {
		return nil, NewBranchNotFoundErr(databaseName.Branch())
	}

	return branch, nil
}

// Database is to manage the collections for this database. Check the Clone method before changing this struct.
type Database struct {
	sync.RWMutex

	id                    uint32
	name                  *DatabaseName
	collections           map[string]*collectionHolder
	needFixingCollections map[string]struct{}
	idToCollectionMap     map[uint32]string
}

func NewDatabase(id uint32, name string) *Database {
	return &Database{
		id:                    id,
		name:                  NewDatabaseName(name),
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

func (d *Database) SameBranch(branch string) bool {
	return d.BranchName() == branch || branch == "" && !d.IsBranch()
}

func (d *Database) IsBranch() bool {
	return !d.name.IsMainBranch()
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
	// idxMeta is a map storing metadata of all the indexes that are part of this collection.
	idxMeta map[string]*IndexMetadata
}

func newCollectionHolder(id uint32, name string, collection *schema.DefaultCollection, idxMeta map[string]*IndexMetadata) *collectionHolder {
	return &collectionHolder{
		id:         id,
		name:       name,
		collection: collection,
		idxMeta:    idxMeta,
	}
}

// clone is used to stage the collectionHolder.
func (c *collectionHolder) clone() *collectionHolder {
	c.Lock()
	defer c.Unlock()

	var copyC collectionHolder
	copyC.id = c.id
	copyC.name = c.name

	implicitIndex := c.collection.ImplicitSearchIndex
	var err error
	copyC.collection, err = createCollection(
		c.id,
		c.name,
		schema.Versions{{Version: c.collection.SchVer, Schema: c.collection.Schema}},
		c.idxMeta,
		implicitIndex.StoreIndexName(),
		implicitIndex.StoreSchema.Fields,
	)
	if err != nil {
		panic(err)
	}

	copyC.collection.SchemaDeltas = c.collection.SchemaDeltas
	copyC.collection.EncodedName = c.collection.EncodedName

	copyC.idxMeta = make(map[string]*IndexMetadata)
	for k, v := range c.idxMeta {
		m := *v
		copyC.idxMeta[k] = &m
	}

	copyC.collection.SearchIndexes = make(map[string]*schema.SearchIndex)
	for _, index := range c.collection.SearchIndexes {
		copyC.collection.AddSearchIndex(index)
	}

	return &copyC
}

func (c *collectionHolder) addIndex(name string, idx *IndexMetadata) {
	c.Lock()
	defer c.Unlock()

	c.idxMeta[name] = idx
}

// get returns the collection managed by this holder. At this point, a Collection object is safely constructed
// with all encoded values assigned to all the attributed i.e. collection, index has assigned the encoded
// values.
func (c *collectionHolder) get() *schema.DefaultCollection {
	c.RLock()
	defer c.RUnlock()

	return c.collection
}

func createCollection(id uint32, name string, schemas schema.Versions, idxMeta map[string]*IndexMetadata,
	searchCollectionName string, fieldsInSearch []tsApi.Field,
) (*schema.DefaultCollection, error) {
	schFactory, err := schema.Build(name, schemas.Latest().Schema)
	if err != nil {
		return nil, err
	}

	indexes := schFactory.Indexes.GetIndexes()
	for _, index := range indexes {
		idx, ok := idxMeta[index.Name]
		if !ok {
			return nil, errors.NotFound("dictionary encoding is missing for index '%s'", index.Name)
		}

		index.Id = idx.ID
	}

	schFactory.Schema = schemas.Latest().Schema

	implicitSearchIndex := schema.NewImplicitSearchIndex(name, searchCollectionName, schFactory.Fields, fieldsInSearch)

	c, err := schema.NewDefaultCollection(id, schemas.Latest().Version, schFactory, schemas, implicitSearchIndex)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// Search is to manage all the search indexes that are explicitly created by the user.
type Search struct {
	sync.RWMutex

	indexes map[string]*schema.SearchIndex
}

func NewSearch() *Search {
	return &Search{
		indexes: make(map[string]*schema.SearchIndex),
	}
}

func (s *Search) AddIndex(index *schema.SearchIndex) {
	s.Lock()
	defer s.Unlock()

	s.indexes[index.Name] = index
}

func (s *Search) GetIndex(name string) (*schema.SearchIndex, bool) {
	s.RLock()
	defer s.RUnlock()

	index, ok := s.indexes[name]
	return index, ok
}

func (s *Search) GetIndexes() []*schema.SearchIndex {
	s.RLock()
	defer s.RUnlock()

	indexes := make([]*schema.SearchIndex, len(s.indexes))
	i := 0
	for _, index := range s.indexes {
		indexes[i] = index
		i++
	}

	return indexes
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
func NewTestTenantMgr(t *testing.T, kvStore kv.KeyValueStore) (*TenantManager, context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)

	m := newTenantManager(kvStore, &search.NoopStore{}, newTestNameRegistry(t), transaction.NewManager(kvStore))

	_ = kvStore.DropTable(ctx, m.mdNameRegistry.ReservedSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.EncodingSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.SchemaSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.NamespaceSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.SearchSchemaSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.UserSubspaceName())
	_ = kvStore.DropTable(ctx, m.mdNameRegistry.ClusterSubspaceName())

	return m, ctx, cancel
}
