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

package log

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/rs/zerolog/pkgerrors"
)

type LogConfig struct {
	Level      string
	Format     string
	SampleRate float64 `mapstructure:"sample_rate" yaml:"sample_rate" json:"sample_rate"`
}

// trim full path. output in the form directory/file.go
func consoleFormatCaller(i interface{}) string {
	var c string
	if cc, ok := i.(string); ok {
		c = cc
	}
	if len(c) > 0 {
		l := strings.Split(c, "/")
		if len(l) == 1 {
			return l[0]
		}
		return l[len(l)-2] + "/" + l[len(l)-1]
	}
	return c
}

// Configure default logger
func Configure(config LogConfig) {
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	lvl, err := zerolog.ParseLevel(config.Level)
	if err != nil {
		log.Error().Err(err).Msg("error parsing log level. defaulting to info level")
		lvl = zerolog.InfoLevel
	}
	if config.Format == "console" {
		output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}
		output.FormatCaller = consoleFormatCaller
		log.Logger = zerolog.New(output).Level(lvl).With().Timestamp().CallerWithSkipFrameCount(3).Stack().Logger()
	} else {
		log.Logger = zerolog.New(os.Stdout).Level(lvl).With().Timestamp().CallerWithSkipFrameCount(3).Stack().Logger()
	}
}

// E is a helper function to shortcut condition checking and logging
// in the case of error
// Used like this:
//
// if E(err) {
//     return err
// }
//
// to replace:
//
// if err != nil {
//     log.Msgf(err.Error())
//     return err
// }
func E(err error) bool {
	if err == nil {
		return false
	}

	log.Error().CallerSkipFrame(2).Err(err).Msg("error")

	return true
}

// CE is a helper to shortcut error creation and logging
// Used like this:
//
// return CE("msg, value %v", value)
//
// to replace:
//
// err := fmt.Errorf("msg, value %v", value)
// log.Msgf("msg, value %v", value)
// return err
//
func CE(format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)

	log.Error().CallerSkipFrame(2).Err(err).Msg("error")

	return err
}
