// Copyright (C) 2010, Kyle Lemons <kyle@kylelemons.net>.  All rights reserved.

package log4go

import (
	"bytes"
	"fmt"
	"io"
)

const (
	FORMAT_DEFAULT = "[%D %T] [%L] (%S) %M"
	FORMAT_MILLIS  = "[%D %A] [%L] (%S) %M"
	FORMAT_SHORT   = "[%t %d] [%L] %M"
	FORMAT_ABBREV  = "[%L] %M"
)

var dateFormatCache = &struct {
	// Date when the cached value was recomputed
	lastYear, lastMonth, lastDay int
	longDate, shortDate          string
}{}

var timeFormatCache = &struct {
	// Second since the epoch when the cached value was recomputed
	lastSecond          int64
	longTime, shortTime string
}{}

var millisFormatCache = &struct {
	// Millisecond since the epoch when the cached value was recomputed
	lastMillis int64
	millisTime string
}{}

// Known format codes:
// %A - Time w/ milliseconds (15:04:05.000)
// %T - Time (15:04:05 MST)
// %t - Time (15:04)
// %D - Date (2006/01/02)
// %d - Date (01/02/06)
// %L - Level (FNST, FINE, DEBG, TRAC, WARN, EROR, CRIT)
// %S - Source
// %M - Message
// Ignores unknown formats
// Recommended: "[%D %T] [%L] (%S) %M"
func FormatLogRecord(format string, rec *LogRecord) string {
	if rec == nil {
		return "<nil>"
	}
	if len(format) == 0 {
		return ""
	}

	out := bytes.NewBuffer(make([]byte, 0, 64))
	millis := rec.Created.UnixNano() / 1e6
	seconds := millis / 1000
	hour, minute, second := rec.Created.Hour(), rec.Created.Minute(), rec.Created.Second()

	// Check if we need to recompute the millisecond cache
	if millisFormatCache.lastMillis != millis {
		nano := rec.Created.Nanosecond()
		millisString := fmt.Sprintf("%02d:%02d:%02d.%03d", hour, minute, second, nano/1e6)
		millisFormatCache.lastMillis = millis
		millisFormatCache.millisTime = millisString
	}

	// Check if we need to recompute the second cache
	if timeFormatCache.lastSecond != seconds {
		zone, _ := rec.Created.Zone()
		timeFormatCache.lastSecond = seconds
		timeFormatCache.longTime = fmt.Sprintf("%02d:%02d:%02d %s", hour, minute, second, zone)
		timeFormatCache.shortTime = fmt.Sprintf("%02d:%02d", hour, minute)
	}

	// Check if we need to recompute the date cache
	month, day, year := rec.Created.Month(), rec.Created.Day(), rec.Created.Year()
	if dateFormatCache.lastDay != day || dateFormatCache.lastMonth != int(month) || dateFormatCache.lastYear != year {
		dateFormatCache.lastDay = day
		dateFormatCache.lastMonth = int(month)
		dateFormatCache.lastYear = year
		dateFormatCache.shortDate = fmt.Sprintf("%02d/%02d/%02d", month, day, year%100)
		dateFormatCache.longDate = fmt.Sprintf("%04d/%02d/%02d", year, month, day)
	}

	// Split the string into pieces by % signs
	pieces := bytes.Split([]byte(format), []byte{'%'})

	// Iterate over the pieces, replacing known formats
	for i, piece := range pieces {
		if i > 0 && len(piece) > 0 {
			switch piece[0] {
			case 'A':
				out.WriteString(millisFormatCache.millisTime)
			case 'T':
				out.WriteString(timeFormatCache.longTime)
			case 't':
				out.WriteString(timeFormatCache.shortTime)
			case 'D':
				out.WriteString(dateFormatCache.longDate)
			case 'd':
				out.WriteString(dateFormatCache.shortDate)
			case 'L':
				out.WriteString(levelStrings[rec.Level])
			case 'S':
				out.WriteString(rec.Source)
			case 'M':
				out.WriteString(rec.Message)
			}
			if len(piece) > 1 {
				out.Write(piece[1:])
			}
		} else if len(piece) > 0 {
			out.Write(piece)
		}
	}
	out.WriteByte('\n')

	return out.String()
}

// This is the standard writer that prints to standard output.
type FormatLogWriter chan *LogRecord

// This creates a new FormatLogWriter
func NewFormatLogWriter(out io.Writer, format string) FormatLogWriter {
	records := make(FormatLogWriter, LogBufferLength)
	go records.run(out, format)
	return records
}

func (w FormatLogWriter) run(out io.Writer, format string) {
	for rec := range w {
		fmt.Fprint(out, FormatLogRecord(format, rec))
	}
}

// This is the FormatLogWriter's output method.  This will block if the output
// buffer is full.
func (w FormatLogWriter) LogWrite(rec *LogRecord) {
	w <- rec
}

// Close stops the logger from sending messages to standard output.  Attempts to
// send log messages to this logger after a Close have undefined behavior.
func (w FormatLogWriter) Close() {
	close(w)
}
