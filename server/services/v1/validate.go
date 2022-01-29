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

package v1

import "fmt"

func isValidCollection(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("invalid collection name")
	}

	return nil
}

func isValidDatabase(name string) error {
	if len(name) == 0 {
		return fmt.Errorf("invalid database name")
	}

	return nil
}

func isValidCollectionAndDatabase(c string, db string) error {
	if err := isValidCollection(c); err != nil {
		return err
	}

	if err := isValidDatabase(db); err != nil {
		return err
	}

	return nil
}
