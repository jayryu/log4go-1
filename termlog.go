// Copyright (C) 2010, Kyle Lemons <kyle@kylelemons.net>.  All rights reserved.

package log4go

import (
	"fmt"
	"io"
	"os"
)

var stdout io.Writer = os.Stdout

/* ConsoleLogWriter was previously a channel which would let you do things
like pass nil values instead. We chagned it to a struct so we could
dangle some extra attributes on it. So that we don't have to go rework
large swaths of the code base to uses pointers for ConsoleLogWriter we're
just going to add this interface so everything else "Just Works". */
type ConsoleLogWriter interface {
	run(out io.Writer)
	LogWrite(rec *LogRecord)
	Close()
}

// This is the standard writer that prints to standard output.
type ConsoleLogWriterImp struct {
	records   chan *LogRecord
	completed chan int
}

// This creates a new ConsoleLogWriter
func NewConsoleLogWriter() ConsoleLogWriter {
	writer := ConsoleLogWriterImp{
		records:   make(chan *LogRecord, LogBufferLength),
		completed: make(chan int),
	}
	go writer.run(stdout)
	return writer
}

func (w ConsoleLogWriterImp) run(out io.Writer) {
	var timestr string
	var timestrAt int64

	for rec := range w.records {
		if at := rec.Created.UnixNano() / 1e9; at != timestrAt {
			timestr, timestrAt = rec.Created.Format("01/02/06 15:04:05"), at
		}
		fmt.Fprint(out, "[", timestr, "] [", levelStrings[rec.Level], "] ", rec.Message, "\n")
	}
	close(w.completed)
}

// This is the ConsoleLogWriter's output method.  This will block if the output
// buffer is full.
func (w ConsoleLogWriterImp) LogWrite(rec *LogRecord) {
	w.records <- rec
}

// Close stops the logger from sending messages to standard output.  Attempts to
// send log messages to this logger after a Close have undefined behavior.
func (w ConsoleLogWriterImp) Close() {
	close(w.records)
	<-w.completed
}
