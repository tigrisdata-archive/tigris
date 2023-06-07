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

//go:build darwin

package util

import (
	"fmt"
	"net"

	"golang.org/x/sys/unix"
)

func ReadPeerCreds(c net.Conn) (uint32, error) {
	var cred *unix.Xucred

	uc, ok := c.(*net.UnixConn)
	if !ok {
		return 0, ErrNotUnixConn
	}

	raw, err := uc.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("error getting raw connection: %s", err)
	}

	err1 := raw.Control(func(fd uintptr) {
		cred, err = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
	})

	if err != nil {
		return 0, fmt.Errorf("getsockoptxucred error: %s", err)
	}

	if err1 != nil {
		return 0, fmt.Errorf("control error: %s", err1)
	}

	return cred.Uid, nil
}
