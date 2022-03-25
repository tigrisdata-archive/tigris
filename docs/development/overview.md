# Development

## Building and Testing with Docker Containers

The first step is to run `make run` which would bring dependencies and server up
in the docker containers with all your changes.

## OSX

It is recommended to have a package manager such as `brew` installed

### Building and Testing on Apple M1

Due to various issues with Docker on Apple M1, it is recommended to install
FoundationDB on the host and run the server on the host directly as well.

To run the server on the host, required dependencies need to be installed by
running:

```shell
sh scripts/install_build_deps.sh
sh scripts/install_test_deps.sh
```

Once the dependencies are installed, you can start up the server on the host as
follows:

```shell
make osx_run
```

## Defining the schema

The first part is to define the schema for the collection that will be stored in
TigrisDB. In the schema define the fields and their types and then define the
primary key fields.

```json
{
  "schema": {
    "title": "Some record",
    "description": "This document stores a record",
    "properties": {
      "pkey_int": {
        "type": "int"
      },
      "str_value": {
        "type": "string"
      },
      "int_value": {
        "type": "int"
      }
    },
    "primary_key": [
      "pkey_int"
    ]
  }
}
```

Below is an example curl request with the JSON schema. The collection "t1" will 
get created in the database "db1".

```shell
curl -X POST 'localhost:8081/api/v1/databases/db1/collections/t1/create' -d '
{
  "schema": {
    "title": "Some record",
    "description": "This document stores a record",
    "properties": {
      "pkey_int": {
        "type": "int"
      },
      "str_value": {
        "type": "string"
      },
      "int_value": {
        "type": "int"
      }
    },
    "primary_key": ["pkey_int"]
  } 
}'
```

## Insert a document

To insert the document use a JSON document similar to the one below

```json
{
  "documents": [
    {
      "pkey_int": 1,
      "int_value": 2,
      "str_value": "foo"
    }
  ],
  "options": {}
}
```

The insert request can be issued to the server using the following curl

```shell
curl -X POST 'localhost:8081/api/v1/databases/db1/collections/t1/documents/insert' -d '
{
  "documents": [
    {
      "pkey_int": 1,
      "int_value": 2,
      "str_value": "foo"
    }
  ],
  "options": {}
}'
```

## Read the document

To read the above document, define a filter with the primary key as below

```json
{
  "filter": {
    "pkey_int": 1
  }
}
```

The read request looks like below,

```shell
curl -X POST 'localhost:8081/api/v1/databases/db1/collections/t1/documents/read' -d '
{
  "filter": {
    "pkey_int": 1
  }
}'
```
