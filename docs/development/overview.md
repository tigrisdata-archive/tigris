---
id: overview
title: "Developing on TigrisDB"
sidebar_label: "Developing on TigrisDB"
---

<!--
  ~ Copyright 2022 Tigris Data, Inc.
  ~ 
  ~ Licensed under the Apache License, Version 2.0 (the "License");
  ~ you may not use this file except in compliance with the License.
  ~ You may obtain a copy of the License at
  ~ 
  ~     http://www.apache.org/licenses/LICENSE-2.0
  ~  
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  -->

# Development

## Building and Testing with Docker Containers

The first step is to run `make run` which would bring dependencies and server up in the docker
containers with all your changes.

**Note** Running the server locally or in editor will cause connectivity issues with FDB. For now, use make run to start the tigris server in a container.

The above command will inject your changes into the tigris server container.

## OSX
It is recommended to have a package manager such as `brew` installed

### Building and Testing on Apple M1

Due to various issues with Docker on Apple M1, it is recommended to install FoundationDB on the host and the run
the server on the host directly as well.

To run the server on the host, required dependencies need to be installed by running:

```shell
sh scripts/install_build_deps.sh
sh scripts/install_test_deps.sh
```

Once the dependencies are installed, you can start up the server on the host as follows:

```shell
make osx_run
```

## Defining the schema

The first part is to define the schema for the collection that will be stored in TigrisDB. In the schema define the fields and their types and then define the primary key fields. 

Below is an example curl request with the JSON schema payload. The schema has two fields and the primary key is a single field "pkey_int".
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
    "primary_key": ["pkey_int"]
  } 
}
```

Use the similar payload as above in the create collection request. The collection "t1" will get created in the database "db1".
```shell
curl -X POST 'localhost:8081/api/v1/databases/db1/collections/t1/create' -d '<above payload>'
```

## Insert a document
To insert the document use a similar JSON document as below.
```json
{
        "documents": [{
                "pkey_int": 1,
                "int_value": 2,
                "str_value": "foo"
        }],
        "options": {}
}
```

The insert request can be issued to the server using the following curl. 
```shell
curl -X POST 'localhost:8081/api/v1/databases/db1/collections/t1/documents/insert' -d '<above payload'
```

## Read the document
To read the above document, a filter can be sent to the server with the primary key.
```json
{
	"filter": {
		"pkey_int": 1
	}
}
```

The read request looks like below,
```shell
curl -X POST 'localhost:8081/api/v1/databases/db1/collections/t1/documents/read' -d '<above payload'
```
