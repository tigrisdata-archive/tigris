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

package util

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"text/template"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
	"github.com/rs/zerolog/log"
	"github.com/tigrisdata/tigris/lib/container"
	ulog "github.com/tigrisdata/tigris/util/log"
	"golang.org/x/sys/unix"
)

const (
	ObjFlattenDelimiter = "."
)

var ErrNotUnixConn = fmt.Errorf("expected unix socket connection")

// Version of this build.
var Version string

// Service program name used in logging and monitoring.
var Service = "tigris-server"

func ExecTemplate(w io.Writer, tmpl string, vars any) error {
	t, err := template.New("exec_template").Funcs(template.FuncMap{"repeat": strings.Repeat}).Parse(tmpl)
	if ulog.E(err) {
		return err
	}

	if err = t.Execute(w, vars); ulog.E(err) {
		return err
	}

	return nil
}

func MapToJSON(data map[string]any) ([]byte, error) {
	var buffer bytes.Buffer

	encoder := jsoniter.NewEncoder(&buffer)

	if err := encoder.Encode(data); ulog.E(err) {
		return nil, err
	}

	return buffer.Bytes(), nil
}

func JSONToMap(data []byte) (map[string]any, error) {
	var decoded map[string]any

	decoder := jsoniter.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	if err := decoder.Decode(&decoded); ulog.E(err) {
		return nil, err
	}

	return decoded, nil
}

func FlatMap(data map[string]any, notFlat container.HashSet) map[string]any {
	resp := make(map[string]any)
	flatMap("", data, resp, notFlat)

	return resp
}

func flatMap(key string, obj map[string]any, resp map[string]any, notFlat container.HashSet) {
	if key != "" {
		key += ObjFlattenDelimiter
	}

	for k, v := range obj {
		switch vMap := v.(type) {
		case map[string]any:
			if notFlat.Contains(key + k) {
				resp[key+k] = v
			} else {
				flatMap(key+k, vMap, resp, notFlat)
			}
		default:
			resp[key+k] = v
		}
	}
}

func UnFlatMap(flat map[string]any, ignoreExtra bool) map[string]any {
	result := make(map[string]any)

	for k, v := range flat {
		keys := strings.Split(k, ObjFlattenDelimiter)
		m := result

		for i := 0; i < len(keys)-1; i++ {
			if m[keys[i]] == nil {
				m[keys[i]] = make(map[string]any)
			}

			if ignoreExtra {
				if _, ok := m[keys[i]].(map[string]any); ok {
					m = m[keys[i]].(map[string]any)
				}
			} else {
				m = m[keys[i]].(map[string]any)
			}
		}

		if v != nil {
			m[keys[len(keys)-1]] = v
		}
	}

	return result
}

func IsTTY(f *os.File) bool {
	fileInfo, _ := f.Stat()

	return (fileInfo.Mode() & os.ModeCharDevice) != 0
}

func PrettyJSON(s any) error {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}

	Stdoutf("%s\n", string(b))

	return nil
}

func Stdoutf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stdout, format, args...)
}

func PrintError(err error) {
	_, _ = fmt.Fprintf(os.Stderr, "%s\n", err.Error())
}

func Error(err error, msg string, args ...any) error {
	log.Err(err).CallerSkipFrame(3).Msgf(msg, args...)

	if err == nil {
		return nil
	}

	return err
}

func Fatal(err error, msg string, args ...any) {
	if err == nil {
		_ = Error(err, msg, args...)
		return
	}

	PrintError(err)

	_ = Error(err, msg, args...)

	os.Exit(1) //nolint:revive
}

func RawMessageToByte(arr []jsoniter.RawMessage) [][]byte {
	ptr := unsafe.Pointer(&arr)
	return *(*[][]byte)(ptr)
}

func ReadPeerCreds(c net.Conn) (*unix.Ucred, error) {
	var cred *unix.Ucred

	uc, ok := c.(*net.UnixConn)
	if !ok {
		return nil, ErrNotUnixConn
	}

	raw, err := uc.SyscallConn()
	if err != nil {
		return nil, fmt.Errorf("error getting raw connection: %s", err)
	}

	err1 := raw.Control(func(fd uintptr) {
		cred, err = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	})

	if err != nil {
		return nil, fmt.Errorf("getsockoptUcred error: %s", err)
	}

	if err1 != nil {
		return nil, fmt.Errorf("control error: %s", err1)
	}

	return cred, nil
}
