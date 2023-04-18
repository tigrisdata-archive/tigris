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

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/tigrisdata/tigris/keys"
	"github.com/tigrisdata/tigris/schema"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/store/kv"
	"github.com/tigrisdata/tigris/util"
)

var (
	ns         int
	project    int
	collection int
)

type TableData struct {
	Table       string          `json:"table"`
	Key         kv.Key          `json:"key,omitempty"`
	Ver         int32           `json:"ver,omitempty"`
	Encoding    int32           `json:"encoding,omitempty"`
	CreatedAt   time.Time       `json:",omitempty"`
	UpdatedAt   time.Time       `json:",omitempty"`
	RawData     json.RawMessage `json:"raw_data"`
	TotalChunks *int32          `json:"total_chunks,omitempty"`
}

func dumpMetadata(subspace []byte, key ...any) {
	ctx := context.TODO()

	tx, err := Mgr.Tx.StartTx(ctx)
	util.Fatal(err, "starting tx")

	k := keys.NewKey(subspace, key...)
	it, err := tx.Read(ctx, k)
	if err != nil {
		panic(err)
	}

	var v kv.KeyValue
	for it.Next(&v) {
		td := TableData{
			Table:       string(subspace),
			Key:         v.Key,
			Ver:         v.Data.Ver,
			Encoding:    v.Data.Encoding,
			RawData:     v.Data.RawData,
			TotalChunks: v.Data.TotalChunks,
		}
		if v.Data.CreatedAt != nil {
			td.CreatedAt = v.Data.CreatedAt.GetProtoTS().AsTime()
		}
		if v.Data.UpdatedAt != nil {
			td.UpdatedAt = v.Data.UpdatedAt.GetProtoTS().AsTime()
		}
		b, err := json.MarshalIndent(td, "", "\t")
		util.Fatal(err, "marshal key value")

		_, _ = fmt.Fprintf(os.Stdout, "%v\n", string(b))
	}
}

var namespacesCmd = &cobra.Command{
	Use:   "namespace",
	Short: "Namespace metadata",
	Run: func(cmd *cobra.Command, args []string) {
		dumpMetadata(Mgr.MetaStore.ReservedSubspaceName(), "namespace")
	},
}

var namespacesMetaCmd = &cobra.Command{
	Use:   "namespace_meta",
	Short: "Namespace user metadata",
	Run: func(cmd *cobra.Command, args []string) {
		dumpMetadata(Mgr.MetaStore.NamespaceSubspaceName())
	},
}

var databaseCmd = &cobra.Command{
	Use:   "database",
	Short: "Database metadata",
	Run: func(cmd *cobra.Command, args []string) {
		dumpMetadata(Mgr.MetaStore.EncodingSubspaceName(), []byte{1})
	},
}

var projectCmd = &cobra.Command{
	Use:   "project",
	Short: "Project metadata",
	Run: func(cmd *cobra.Command, args []string) {
		dumpMetadata(Mgr.MetaStore.ReservedSubspaceName())
	},
}

var userCmd = &cobra.Command{
	Use:   "user",
	Short: "User metadata",
	Run: func(cmd *cobra.Command, args []string) {
		dumpMetadata(Mgr.MetaStore.UserSubspaceName())
	},
}

var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "Schema commands",
	Run: func(cmd *cobra.Command, args []string) {
		dumpMetadata(Mgr.MetaStore.SchemaSubspaceName())
	},
}

var searchSchemaCmd = &cobra.Command{
	Use:   "search_schema",
	Short: "Search schema commands",
	Run: func(cmd *cobra.Command, args []string) {
		dumpMetadata(Mgr.MetaStore.SchemaSubspaceName())
	},
}

var queueCmd = &cobra.Command{
	Use:   "queue",
	Short: "Queue commands",
	Run: func(cmd *cobra.Command, args []string) {
		dumpMetadata(Mgr.MetaStore.QueueSubspaceName())
	},
}

func dumpData() {
	ctx := context.TODO()

	tx, err := Mgr.Tx.StartTx(ctx)
	util.Fatal(err, "starting tx")

	enc, err := metadata.NewEncoder().EncodeTableName(metadata.NewTenantNamespace("", metadata.NewNamespaceMetadata(uint32(ns), "", "")),
		metadata.NewDatabase(uint32(project), ""),
		&schema.DefaultCollection{Id: uint32(collection)},
	)

	util.Fatal(err, "read")

	k := keys.NewKey(enc)
	it, err := tx.Read(ctx, k)
	util.Fatal(err, "read")

	var v kv.KeyValue
	for it.Next(&v) {
		td := TableData{
			Table:       fmt.Sprintf("%d %d %d", ns, project, collection),
			Key:         v.Key,
			Ver:         v.Data.Ver,
			Encoding:    v.Data.Encoding,
			RawData:     v.Data.RawData,
			TotalChunks: v.Data.TotalChunks,
		}
		if v.Data.CreatedAt != nil {
			td.CreatedAt = v.Data.CreatedAt.GetProtoTS().AsTime()
		}
		if v.Data.UpdatedAt != nil {
			td.UpdatedAt = v.Data.UpdatedAt.GetProtoTS().AsTime()
		}
		b, err := json.MarshalIndent(td, "", "\t")
		util.Fatal(err, "marshal key value")

		fmt.Fprintf(os.Stdout, "%v\n", string(b))
	}
}

var dumpDataCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump document keys",
	Run: func(cmd *cobra.Command, args []string) {
		dumpData()
	},
}

var metadataCmd = &cobra.Command{
	Use:   "metadata",
	Short: "Metadata commands",
}

var dataCmd = &cobra.Command{
	Use:   "data",
	Short: "Data commands",
}

func init() {
	metadataCmd.AddCommand(namespacesCmd)
	metadataCmd.AddCommand(namespacesMetaCmd)
	metadataCmd.AddCommand(schemaCmd)
	metadataCmd.AddCommand(searchSchemaCmd)
	metadataCmd.AddCommand(databaseCmd)
	metadataCmd.AddCommand(projectCmd)
	metadataCmd.AddCommand(userCmd)
	metadataCmd.AddCommand(queueCmd)

	dumpDataCmd.Flags().IntVar(&ns, "namespace", 0, "")
	dumpDataCmd.Flags().IntVar(&project, "project", 0, "")
	dumpDataCmd.Flags().IntVar(&collection, "collection", 0, "")
	dataCmd.AddCommand(dumpDataCmd)

	rootCmd.AddCommand(metadataCmd)
	rootCmd.AddCommand(dataCmd)
}
