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

package util

import "github.com/tigrisdata/tigris/lib/set"

// Version of this build
var Version string

// Service program name used in logging and monitoring
var Service string = "tigris-server"

var LanguageKeywords = set.New("abstract", "add", "alias", "and", "any", "args", "arguments", "array",
	"as", "as?", "ascending", "assert", "async", "await", "base", "bool", "boolean", "break", "by", "byte",
	"callable", "case", "catch", "chan", "char", "checked", "class", "clone", "const", "constructor", "continue",
	"debugger", "decimal", "declare", "def", "default", "defer", "del", "delegate", "delete", "descending", "die",
	"do", "double", "dynamic", "echo", "elif", "else", "elseif", "empty", "enddeclare", "endfor", "endforeach",
	"endif", "endswitch", "endwhile", "enum", "equals", "eval", "event", "except", "exception", "exit", "explicit",
	"export", "extends", "extern", "fallthrough", "false", "final", "finally", "fixed", "float", "fn", "for",
	"foreach", "from", "fun", "func", "function", "get", "global", "go", "goto", "group", "if", "implements",
	"implicit", "import", "in", "include", "include_once", "init", "instanceof", "insteadof", "int", "integer",
	"interface", "internal", "into", "is", "isset", "join", "lambda", "let", "list", "lock", "long", "managed",
	"map", "match", "module", "nameof", "namespace", "native", "new", "nint", "none", "nonlocal", "not", "notnull",
	"nuint", "null", "number", "object", "of", "on", "operator", "or", "orderby", "out", "override", "package",
	"params", "partial", "pass", "print", "private", "protected", "public", "raise", "range", "readonly", "record",
	"ref", "remove", "require", "require_once", "return", "sbyte", "sealed", "select", "set", "short", "sizeof",
	"stackalloc", "static", "strictfp", "string", "struct", "super", "switch", "symbol", "synchronized", "this",
	"throw", "throws", "trait", "transient", "true", "try", "type", "typealias", "typeof", "uint", "ulong",
	"unchecked", "unmanaged", "unsafe", "unset", "use", "ushort", "using", "val", "value", "var", "virtual", "void",
	"volatile", "when", "where", "while", "with", "xor", "yield")
