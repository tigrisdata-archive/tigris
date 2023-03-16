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

package auth

import (
	"testing"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"github.com/tigrisdata/tigris/lib/container"
	"github.com/tigrisdata/tigris/server/config"
)

func TestClientCredentialsCharacters(t *testing.T) {
	g := &gotrue{
		AuthConfig: config.AuthConfig{
			Gotrue: config.Gotrue{
				ClientIdLength:     100,
				ClientSecretLength: 1000,
			},
		},
		userStore: nil,
		txMgr:     nil,
	}
	idCharSet := container.NewHashSet()
	for _, v := range idChars {
		idCharSet.Insert(string(v))
	}

	secretCharSet := container.NewHashSet()
	for _, v := range secretChars {
		secretCharSet.Insert(string(v))
	}

	// generate 100 random creds and inspect them
	for i := 0; i < 100; i++ {
		id := generateClientId(g)
		secret := generateClientSecret(g)
		require.Truef(t, containsFromTheseChars(id, idCharSet), "Invalid character in id found, id=%s", id)
		require.Truef(t, containsFromTheseChars(secret, secretCharSet), "Invalid character in secret found, id=%s", secret)
	}
}

func containsFromTheseChars(mainStr string, charSet container.HashSet) bool {
	for _, char := range mainStr {
		if !charSet.Contains(string(char)) {
			log.Warn().Str("invalid_char", string(char)).Msg("Invalid char found")
			return false
		}
	}
	return true
}
