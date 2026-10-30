/*
 *
 * Copyright 2026 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package accesslog

import (
	"fmt"
	"strconv"
	"strings"
)

// ValueKind is the type of a Value.
type ValueKind int

const (
	// KindString is a string value.
	KindString ValueKind = iota
	// KindInt64 is a signed integer value.
	KindInt64
	// KindBool is a boolean value.
	KindBool
	// KindFloat64 is a floating point value.
	KindFloat64
	// KindBytes is a byte slice value.
	KindBytes
	// KindSlice is a list of values.
	KindSlice
	// KindMap is a list of key-value pairs.
	KindMap
)

// Value is the value of a log body or attribute. It mirrors the OpenTelemetry
// AnyValue type, so that this package needs no OpenTelemetry dependency.
type Value struct {
	Kind    ValueKind
	String  string
	Int64   int64
	Bool    bool
	Float64 float64
	Bytes   []byte
	Slice   []Value
	Map     []KeyValue
}

// KeyValue is a key and a value.
type KeyValue struct {
	Key   string
	Value Value
}

// format returns a deterministic text form of v, used in logs and sink keys.
func (v Value) format(b *strings.Builder) {
	switch v.Kind {
	case KindString:
		b.WriteString(strconv.Quote(v.String))
	case KindInt64:
		b.WriteString(strconv.FormatInt(v.Int64, 10))
	case KindBool:
		b.WriteString(strconv.FormatBool(v.Bool))
	case KindFloat64:
		b.WriteString(strconv.FormatFloat(v.Float64, 'g', -1, 64))
	case KindBytes:
		fmt.Fprintf(b, "bytes(%x)", v.Bytes)
	case KindSlice:
		b.WriteByte('[')
		for i, e := range v.Slice {
			if i > 0 {
				b.WriteString(", ")
			}
			e.format(b)
		}
		b.WriteByte(']')
	case KindMap:
		formatKeyValues(b, v.Map)
	}
}

func formatKeyValues(b *strings.Builder, kvs []KeyValue) {
	b.WriteByte('{')
	for i, kv := range kvs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(strconv.Quote(kv.Key))
		b.WriteByte('=')
		kv.Value.format(b)
	}
	b.WriteByte('}')
}

// valueTemplate is a Value in which strings are substitution templates.
type valueTemplate struct {
	kind ValueKind
	// formatter is set for KindString.
	formatter *Formatter
	// static is the value for KindInt64, KindBool, KindFloat64, and
	// KindBytes.
	static Value
	slice  []valueTemplate
	kvs    []keyValueTemplate
}

type keyValueTemplate struct {
	key   string
	value valueTemplate
}

// evaluate returns the value of t for the RPC described by d.
func (t *valueTemplate) evaluate(d *CallData) Value {
	switch t.kind {
	case KindString:
		return Value{Kind: KindString, String: t.formatter.Format(d)}
	case KindSlice:
		vs := make([]Value, len(t.slice))
		for i := range t.slice {
			vs[i] = t.slice[i].evaluate(d)
		}
		return Value{Kind: KindSlice, Slice: vs}
	case KindMap:
		return Value{Kind: KindMap, Map: evaluateKeyValues(t.kvs, d)}
	default:
		return t.static
	}
}

func evaluateKeyValues(kvs []keyValueTemplate, d *CallData) []KeyValue {
	out := make([]KeyValue, len(kvs))
	for i := range kvs {
		out[i] = KeyValue{Key: kvs[i].key, Value: kvs[i].value.evaluate(d)}
	}
	return out
}

// addUsage adds the header names and fields that t reads to u.
func (t *valueTemplate) addUsage(u *fieldUsage) {
	switch t.kind {
	case KindString:
		t.formatter.addUsage(u)
	case KindSlice:
		for i := range t.slice {
			t.slice[i].addUsage(u)
		}
	case KindMap:
		for i := range t.kvs {
			t.kvs[i].value.addUsage(u)
		}
	}
}

func (t *valueTemplate) format(b *strings.Builder) {
	switch t.kind {
	case KindString:
		b.WriteString("template(")
		b.WriteString(strconv.Quote(t.formatter.String()))
		b.WriteByte(')')
	case KindSlice:
		b.WriteByte('[')
		for i := range t.slice {
			if i > 0 {
				b.WriteString(", ")
			}
			t.slice[i].format(b)
		}
		b.WriteByte(']')
	case KindMap:
		formatKeyValueTemplates(b, t.kvs)
	default:
		t.static.format(b)
	}
}

func formatKeyValueTemplates(b *strings.Builder, kvs []keyValueTemplate) {
	b.WriteByte('{')
	for i := range kvs {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(strconv.Quote(kvs[i].key))
		b.WriteByte('=')
		kvs[i].value.format(b)
	}
	b.WriteByte('}')
}
