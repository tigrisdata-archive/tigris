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

package defaults

const (
	// DefaultNamespaceName is for "default" namespace in the cluster which means all the databases created are under a single
	// namespace.
	// It is totally fine for a deployment to choose this and just have one namespace. The default assigned value for
	// this namespace is 1.
	DefaultNamespaceName string = "default_namespace"
	DefaultNamespaceId          = uint32(1)
	UnknownValue                = "unknown"
)
