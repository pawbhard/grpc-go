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
	"time"
)

// strftimeFormat is a compiled strftime-style time format, as used by the
// Envoy %START_TIME(fmt)% operator. Go has no strftime, so the supported
// specifiers are implemented here.
//
// Supported specifiers:
//   - %Y %C %y %G %g %m %d %e %j %H %I %k %l %M %S %p %a %A %b %h %B %u %w
//     %z %Z %s %F %T %D %R %r %c %x %X %n %t %%
//   - %f: fractional seconds with 9 digits (Envoy).
//   - %<N>f: fractional seconds with N digits, 1 <= N <= 9 (Envoy).
//   - %E<N>S: seconds with N fractional digits, 0 <= N <= 9 (absl).
//   - %E*S: seconds with all significant fractional digits (absl).
//   - %E<N>f, %E*f: fractional seconds, as above, without the seconds (absl).
//   - %Ez: the UTC offset as +hh:mm (absl).
//
// Other specifiers are rejected.
type strftimeFormat struct {
	parts []func(*strings.Builder, time.Time)
}

func (f *strftimeFormat) format(t time.Time) string {
	var b strings.Builder
	for _, p := range f.parts {
		p(&b, t)
	}
	return b.String()
}

func literalPart(s string) func(*strings.Builder, time.Time) {
	return func(b *strings.Builder, _ time.Time) { b.WriteString(s) }
}

func layoutPart(layout string) func(*strings.Builder, time.Time) {
	return func(b *strings.Builder, t time.Time) { b.WriteString(t.Format(layout)) }
}

func paddedPart(width int, pad byte, value func(time.Time) int) func(*strings.Builder, time.Time) {
	return func(b *strings.Builder, t time.Time) { writePadded(b, value(t), width, pad) }
}

func writePadded(b *strings.Builder, v, width int, pad byte) {
	s := strconv.Itoa(v)
	for i := len(s); i < width; i++ {
		b.WriteByte(pad)
	}
	b.WriteString(s)
}

// fractionPart writes the fractional seconds of t with the given number of
// digits. If digits is negative, it writes all significant digits, and
// nothing if the fraction is zero.
func fractionPart(digits int, withDot bool) func(*strings.Builder, time.Time) {
	return func(b *strings.Builder, t time.Time) {
		s := fmt.Sprintf("%09d", t.Nanosecond())
		if digits < 0 {
			s = strings.TrimRight(s, "0")
		} else {
			s = s[:digits]
		}
		if s == "" {
			return
		}
		if withDot {
			b.WriteByte('.')
		}
		b.WriteString(s)
	}
}

var simpleSpecifiers = map[byte]func(*strings.Builder, time.Time){
	'Y': func(b *strings.Builder, t time.Time) { b.WriteString(strconv.Itoa(t.Year())) },
	'C': paddedPart(2, '0', func(t time.Time) int { return t.Year() / 100 }),
	'y': paddedPart(2, '0', func(t time.Time) int { return t.Year() % 100 }),
	'G': func(b *strings.Builder, t time.Time) { y, _ := t.ISOWeek(); b.WriteString(strconv.Itoa(y)) },
	'g': paddedPart(2, '0', func(t time.Time) int { y, _ := t.ISOWeek(); return y % 100 }),
	'm': paddedPart(2, '0', func(t time.Time) int { return int(t.Month()) }),
	'd': paddedPart(2, '0', func(t time.Time) int { return t.Day() }),
	'e': paddedPart(2, ' ', func(t time.Time) int { return t.Day() }),
	'j': paddedPart(3, '0', func(t time.Time) int { return t.YearDay() }),
	'H': paddedPart(2, '0', func(t time.Time) int { return t.Hour() }),
	'k': paddedPart(2, ' ', func(t time.Time) int { return t.Hour() }),
	'I': paddedPart(2, '0', hour12),
	'l': paddedPart(2, ' ', hour12),
	'M': paddedPart(2, '0', func(t time.Time) int { return t.Minute() }),
	'S': paddedPart(2, '0', func(t time.Time) int { return t.Second() }),
	'u': paddedPart(1, '0', func(t time.Time) int {
		if wd := int(t.Weekday()); wd != 0 {
			return wd
		}
		return 7
	}),
	'w': paddedPart(1, '0', func(t time.Time) int { return int(t.Weekday()) }),
	'p': layoutPart("PM"),
	'a': layoutPart("Mon"),
	'A': layoutPart("Monday"),
	'b': layoutPart("Jan"),
	'h': layoutPart("Jan"),
	'B': layoutPart("January"),
	'z': layoutPart("-0700"),
	'Z': layoutPart("MST"),
	's': func(b *strings.Builder, t time.Time) { b.WriteString(strconv.FormatInt(t.Unix(), 10)) },
	'F': layoutPart("2006-01-02"),
	'T': layoutPart("15:04:05"),
	'D': layoutPart("01/02/06"),
	'R': layoutPart("15:04"),
	'r': layoutPart("03:04:05 PM"),
	'c': layoutPart("Mon Jan _2 15:04:05 2006"),
	'x': layoutPart("01/02/06"),
	'X': layoutPart("15:04:05"),
	'n': literalPart("\n"),
	't': literalPart("\t"),
	'%': literalPart("%"),
	'f': fractionPart(9, false),
}

func hour12(t time.Time) int {
	if h := t.Hour() % 12; h != 0 {
		return h
	}
	return 12
}

// parseStrftime compiles a strftime-style format. See strftimeFormat for the
// supported specifiers.
func parseStrftime(format string) (*strftimeFormat, error) {
	f := &strftimeFormat{}
	var literal strings.Builder
	flush := func() {
		if literal.Len() > 0 {
			f.parts = append(f.parts, literalPart(literal.String()))
			literal.Reset()
		}
	}
	for i := 0; i < len(format); i++ {
		c := format[i]
		if c != '%' {
			literal.WriteByte(c)
			continue
		}
		if i+1 >= len(format) {
			return nil, fmt.Errorf("time format %q ends with '%%'", format)
		}
		i++
		c = format[i]
		var part func(*strings.Builder, time.Time)
		switch {
		case c >= '1' && c <= '9' && i+1 < len(format) && format[i+1] == 'f':
			// Envoy "%<N>f".
			part = fractionPart(int(c-'0'), false)
			i++
		case c == 'E':
			p, n, err := parseExtendedSpecifier(format[i+1:])
			if err != nil {
				return nil, fmt.Errorf("time format %q: %v", format, err)
			}
			part = p
			i += n
		default:
			p, ok := simpleSpecifiers[c]
			if !ok {
				return nil, fmt.Errorf("time format %q: unsupported specifier %%%c", format, c)
			}
			part = p
		}
		flush()
		f.parts = append(f.parts, part)
	}
	flush()
	return f, nil
}

// parseExtendedSpecifier parses the text after "%E". It returns the part and
// the number of bytes of s that it used.
func parseExtendedSpecifier(s string) (func(*strings.Builder, time.Time), int, error) {
	if strings.HasPrefix(s, "z") {
		return layoutPart("-07:00"), 1, nil
	}
	if len(s) < 2 {
		return nil, 0, fmt.Errorf("incomplete specifier %%E%s", s)
	}
	digits := -1
	switch c := s[0]; {
	case c == '*':
	case c >= '0' && c <= '9':
		digits = int(c - '0')
	default:
		return nil, 0, fmt.Errorf("unsupported specifier %%E%c", c)
	}
	switch s[1] {
	case 'S':
		seconds := simpleSpecifiers['S']
		fraction := fractionPart(digits, true)
		return func(b *strings.Builder, t time.Time) {
			seconds(b, t)
			fraction(b, t)
		}, 2, nil
	case 'f':
		if digits == 0 {
			return func(*strings.Builder, time.Time) {}, 2, nil
		}
		return fractionPart(digits, false), 2, nil
	}
	return nil, 0, fmt.Errorf("unsupported specifier %%E%s", s[:2])
}
