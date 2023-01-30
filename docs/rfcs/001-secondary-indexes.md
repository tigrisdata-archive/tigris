## Introduction

This document describes the data model and index management for building and querying of Tigris secondary indexes.

## Background

The Tigris Database is a typed transactional document data store. It is built on top of FoundationDB which is a distributed, transaction-ordered key-value store. Because of FoundationDB, all Tigris database operations are consistent and serializable.

## Goals of secondary indexes

The main aim of secondary indexes for Tigris are:

- Index all (or a fair amount of) fields in a document
  - We will not index byte fields
- Keep an index per collection
- Index the documents in the transaction that the document is modified in so that indexes are always consistent with the primary data

## Index Design

The secondary indexes will follow the design of an inverted index pattern with the full key path of the value included. At a high level an example of how this will look for a document in a `user` collection:

```js
{
	id: "my-id",
	name: "Magnus",
	age: 20,
	location: {
		country: "RSA",
		home: {
		   address: "24 Sunny Road"
		}
	},
	interests: ["rugby", "music", "climbing"]
}
```

Tigris will turn this into an index that looks like this:

```jsx
(..)(index)(kvs)("name"), ("magnus")(0)(docId)
(..)(index)(kvs)("age.number"),(20)(0)(docId)
(..)(index)(kvs)("location.country")("RSA")(0)(docId)
(..)(index)(kvs)("location.home.address"),("24 Sunny Road")(0)(docId)
(..)(index)(kvs)("interests")("rugby")(1)(docId)
(..)(index)(kvs)("interests")("music")(2)(docId)
(..)(index)(kvs)("interests")("climbing")(3)(docId)
```

I've left out the full database path and the field index version to make it easier to read.

From the above, you can see that the document is broken up into a key/value. We keep the full JSON path for each key/value and store it as a keypath. This is similar to how Postgres's `json_path_ops` and CouchDB's map/reduce index design work.

The above index design will make it easy to perform rich queries against it. For example, if we want to run a query for all users with `age` `20`. The query would create a range key set as follows:

Find all keys that are great than or equal to:

```jsx
(..)(index)(kvs)("age")(20)
```

And smaller than:

```jsx
(..)(index)(kvs)("age")("age")(20)(0xFF)
```

## Data Model

The full data model for all the different parts of the index:

```jsx
# index metadata top level
(tenant)(database)(collection)(index)(info)(count) = number of rows
(tenant)(database)(collection)(index)(info)(size) = Size of index

# key path count
(tenant)(database)(collection)(index)(path_count)(...key_path) = number of rows

# Document key values
(tenant)(database)(collection)(index)(kvs)(...key_path)(field_version)(value)(duplicate_key)(doc_id)
```

Reference:

- (`tenant`) compressed tenant key
- `(database)` compressed database or project key
- (`collection`) the compressed key for the collection
- (`index`) the compressed key for the index and a version value
- (`info`) The key space to record information about the index
- (`path_count`) The key space to store the number of rows for a specific key path
- (`…key_path`) A documents value expanded to its full key_path
- (`kvs`) The key space to store the key values for the index
- (`field_version`) The field version for a field
- (`value`) The value for the field
- (`duplicate_key`) Only used for arrays, it is the position of the field in the array
- (`doc_id`) The document id that the key belongs to.

## Data Encoding and Field Version

### Field Version

Tigris supports schema migration and field-level type changes. For example, a field that was originally an integer could be migrated to a string. On reading and updates of documents, Tigris will update the existing primary document to the new schema. A Tigris collection will keep a `Field Version` for every field schema change. This value will be added to the key in the secondary indexes. Tigris will use this value when querying to first fetch the fields with the latest `Field Version` and then fetch the keys from the secondary index with the older `Field Version`. This does mean that when a field has multiple `Field Version`'s that the sort order will be split into two with the latest fields sorted first and then the older versions sorted afterwards.

A background job will also be running to upgrade all older fields to the latest `Field Version`, so having more than one `Field Version` for a field in the secondary index will be temporary.

### String collation

By default, FoundationDB will order keys based on byte order. This will work for the base types `boolean`, `number` and `null`. However, for strings, we need to support Unicode collation for better sorting. Using Unicode ordering will take into account capitalization, punctuation and special characters in letters for e.g `ö` or `ø`. Initially, we can support the default `locale` for string encoding. Future work would be to build the index using specific `locale` settings.

This means that when we generate a key in the index that contains a string, instead of using the string in the key, we will create a Unicode collation key. We will do this using the std [collate](https://pkg.go.dev/golang.org/x/text/collate) library that Golang has.

For Unicode collation, the collation keys are not stable across versions. If the Golang library ever upgrades the library, it will mean that any new keys in an index with the new version of Golang could be different. This could be a problem. We would most likely have to vendor the current version we are using, and then look to upgrade our indexes in the background. Looking through the Golang release history for Collation (6 years), the Unicode version has never been upgraded. If it does, we can address this

For a detailed explanation of this [CockroachDB](https://www.cockroachlabs.com/blog/unicode-collation-in-cockroachdb/) has a good blog post.

## String Length limits

Large strings will be difficult to store in the index and from the user's perspective a very large string would not be that useful. We will set a size limit for a string that we index up to. If a string is larger than that, we index the prefix part. Tigris will still use the prefix for queries, a prefix could potentially match multiple values, Tigris will then use in-memory filtering with the complete value of the string to get an accurate result. Tigris also offers text search that will be better suited for larger string queries.

### Arrays

Arrays can support duplicate values in them for example:

```jsx
let arr1 = ["rugby", "music", "rugby"];
let arr2 = [{ name: "mary" }, { name: "mary" }];
```

To account for this, we need to add a position value in the key so that we can support duplicate values. Splitting the array out into individual keys allows Tigris to support complex queries into arrays.

## Nested Arrays

For the first version of the secondary indexes, Tigris will not index into nested arrays - An array inside of an array. We will store a key path stub to the start of the nested array and when a query requires using a range scan on a nested index, Tigris will use the key path stub to get a key range and then filter the full document in memory.

## Object Type

Tigris supports an `Object` type that is a JSON object with the option of defining some of the fields in the object. If the fields are defined in the schema, Tigris will add them to the index for any undefined fields that are part of the document at indexing time, Tigris will index the top-level fields.

## Null and missing fields

If a field is explicitly set as `null` that will be added to the index however if a field is defined in the schema but not included in the document at insert. That field will not be added to the index.

## Index Metadata

At the top level of the index, we will have an FDB row that keeps track of the number of rows for the full index and another row that contains the size of the index in bytes. Both of these rows will be updated via atomic operations so that if multiple indexes are updating the index at the same time there won’t be a write conflict.
The metadata will look like this:

```jsx
(DATABASE)(COLLECTION)(INDEX_AND_VERSION)(INDEX_INFO)(COUNT) = number of rows
(DATABASE)(COLLECTION)(INDEX_AND_VERSION)(INDEX_INFO)(SIZE) = Size of index
```

### Keypath count and cost estimation

Tigris will also keep a row count for top-level key paths. This could potentially be used to make a smarter query planner. In a situation where there are multiple range scans in a query, Tigris can read the key path row count to try and pick the range scan with the least number of rows in it. Leading to a faster query response.

The key row count will be stored in the following format:

```jsx
(DATABASE)(COLLECTION)(INDEX_AND_VERSION)(INDEX_PATH_COUNT)(KEY_PATH) = number of rows
```

## Building the Index

Secondary indexes will only be built and updated in the transaction the document is added or modified. This will allow the secondary index to always be consistent with the primary index.
We will implement background workers to handle `FieldVersion` upgrades - this will be covered in a later RFC.

The algorithm for adding a new document:

```jsx
1. Break the document down into:
    1. key/value pairs
    2. row count and row size
    3. row count at certain key paths for cost estimation
2. Add Key/Value pairs to fdb
3. Atomic updates to update row count, row size and key path counts
```

Algorithm for deleting a document from the index:

```jsx
1. Read the current document from FDB
2. Break the document down into:
    1. key/value pairs
    2. row count and row size
    3. row count at certain key paths for cost estimation
3. Delete all key/value pairs from the index
4. Atomic remove to remove row count, row size and key path counts
```

The algorithm for updating a document would be performing both the above operations

```jsx
1. Delete current index key/values
2. Add new keys/values
```

For the update, if possible, we could perform a diff and only update the changed fields. This would be a lot more efficient.

## **Querying and Filtering the index**

When querying the index, Tigris will analyze the filter parameters to generate FDB key ranges. Tigris will read the keys from the secondary index, fetch the main document and then apply the filters will to the documents in memory.

From the docs, we currently support the following Filters:

- **SelectorFilterOperator.EQ**: equal to is used for exact matching.
- **SelectorFilterOperator.LT:** less than is used for matching documents using less than criteria.
- **SelectorFilterOperator.LTE**: less than or equal to is similar to **SelectorFilterOperator.LT** but also matches for equality.
- **SelectorFilterOperator.GT**: greater than is used for matching documents using greater than criteria.
- **SelectorFilterOperator.GTE**

All of the above filters can be converted to FoundationDB range key scans and used with the Secondary index.

The algorithm for a query would be:

```python
1. Generate Key Range from query filters
2. Fetch keys from the index
3. Use the id field from the key to fetch the documents
4. Filter the documents with the filter
5. If the document passes the filter return to the user
```

**Cost estimation**

We will also investigate using the key path row count to do a basic cost estimation when there are multiple key ranges to choose from. This could help Tigris choose the key range with the least number of rows for a faster query.

### Returning results across transaction limits

FoundationDB has a 5-second transaction limit. Tigris will support streaming results across multiple transactions. When Tigris is still fetching results from FoundationDB and the transaction is ended, Tigris will use the last key fetched as the starting point for the read for the next transaction.

This can lead to inconsistent or duplicate results if a document has been updated while the previous transaction was running. We will investigate the best user experience for this. Our initial idea is to make this an option a user adds to the request so that they understand this could happen

## Sorting

The first version of this will only support sorting on a single field for a query. If a user does:

```js
collection.find({age: {gt: 3}, val2: {gt: 20}}, sort: {age: desc})
```

Tigris will use the `age` query for the range query.

## Explain query and Execution statistics

[MongoDB](https://www.percona.com/blog/2018/09/06/mongodb-investigate-queries-with-explain-index-usage-part-2/) and [CouchDB](https://docs.couchdb.org/en/3.2.2-docs/api/database/find.html#db-explain) both have an `explain` API endpoint to see what the query planner would be for a query. This is a very helpful addition for helping users with complex queries. Tigris will add a similar endpoint.

[CouchDB](https://docs.couchdb.org/en/3.2.2-docs/api/database/find.html#execution-statistics) also has the option to enable execution statistics for a query. This would then return information on how many documents and keys were examined to return the results. This was also really useful in helping users improve complex queries. Tigris will add something similar. Users can use this to see if they are scanning too many keys in FDB and if they can add another filter to reduce the number of documents scanned to improve the query performance.

## **Index limits**

The initial limitations for secondary indexes will be:

- Limit the depth of indexing to 4 or 5 levels of the document
  - We can find the best size by running some perf tests.
- The index will be kept up to date with a new database or new collection but cannot be added to an existing database. This will be fixed later with async builds
- If a document key path exceeds the size of an FDB key, it will not be indexed.

## **Future work**

1.  Allow a user to specify certain fields that should be excluded from the index
2.  Background async building of an index for batch updates
3.  Index Aggregations - Might be better to handle this outside of FoundationDB4. The depth is somewhat naturally limited by the 100,000-byte value size in fdb. We could investigate storing subdocuments under a different fdb key and the original document would only contain the id of the subdocument. It could solve both the size limit and the indexing depth issues, but opens up the avenue for valid user-generated large transactions.
