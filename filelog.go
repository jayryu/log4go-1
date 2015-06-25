// Copyright (C) 2010, Kyle Lemons <kyle@kylelemons.net>.  All rights reserved.

package log4go

import (
	"os"
	"fmt"
	"time"
	"path"
)

type rotateSuffix int

const (
	invalidSuffix rotateSuffix = iota
	intSuffix
	dateSuffix
)

type nowFunc func() time.Time

// This log writer sends output to a file
type FileLogWriter struct {
	rec chan *LogRecord
	rot chan bool
	completed chan int

	// The opened file
	filename string
	file     *os.File

	// The logging format
	format string

	// File header/trailer
	header, trailer string

	// Rotate at linecount
	maxlines          int
	maxlines_curlines int

	// Rotate at size
	maxsize         int
	maxsize_cursize int

	// Rotate daily
	daily          bool
	daily_opendate time.Time

	// The suffix to append to rolled filenames
	filenameSuffix	rotateSuffix

	// Keep old logfiles
	rotate bool

	// In production this will always be time.Now(),
	// exposed so we can set the time when testing
	now nowFunc
}

// This is the FileLogWriter's output method
func (w *FileLogWriter) LogWrite(rec *LogRecord) {
	w.rec <- rec
}

func (w *FileLogWriter) Close() {
	close(w.rec)
	<- w.completed
}

// NewFileLogWriter creates a new LogWriter which writes to the given file and
// has rotation enabled if rotate is true.
//
// If rotate is true, any time a new log file is opened, the old one is renamed
// with a .### extension to preserve it.  The various Set* methods can be used
// to configure log rotation based on lines, size, and daily.
//
// The standard log-line format is:
//   [%D %T] [%L] (%S) %M
func NewFileLogWriter(fname string, rotate bool, suffix rotateSuffix) *FileLogWriter {
	w := &FileLogWriter{
		rec:      make(chan *LogRecord, LogBufferLength),
		rot:      make(chan bool),
		completed: make(chan int),
		filename: fname,
		format:   "[%D %T] [%L] (%S) %M",
		rotate:   rotate,
		filenameSuffix:   suffix,
		now:	time.Now,
	}

	// If the directory doesn't exist, attempt to create it
	logDir := path.Dir(fname)
	err := os.MkdirAll(logDir, os.ModeDir | os.ModePerm)	
	if err != nil {
		fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
		return nil
	}

	// open the file for the first time
	if err := w.handleRotate(); err != nil {
		fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
		return nil
	}

	go func() {
		defer func() {
			if w.file != nil {
				fmt.Fprint(w.file, FormatLogRecord(w.trailer, &LogRecord{Created: w.now()}))
				w.file.Close()
			}
		}()

		for {
			select {
			case <-w.rot:
				if err := w.intRotate(); err != nil {
					fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
					return
				}
			case rec, ok := <-w.rec:
				if !ok {
					close(w.completed)
					return
				}
				now := w.now()
				// maxlines isn't supported when using dateSuffix, because we don't scan the file
				// to get the current number of lines.
				if (w.maxlines > 0 && w.maxlines_curlines >= w.maxlines && w.filenameSuffix == intSuffix) ||
					(w.maxsize > 0 && w.maxsize_cursize >= w.maxsize) ||
					(w.daily && now.Day() != w.daily_opendate.Day()) {
					if err := w.handleRotate(); err != nil {
						fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
						return
					}
				}

				// Perform the write
				n, err := fmt.Fprint(w.file, FormatLogRecord(w.format, rec))
				if err != nil {
					fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
					return
				}

				// Update the counts
				w.maxlines_curlines++
				w.maxsize_cursize += n
			}
		}
	}()

	return w
}

// Request that the logs rotate
func (w *FileLogWriter) Rotate() {
	w.rot <- true
}

func (w *FileLogWriter) handleRotate() error {
	// Distinguish between rolling the file at startup,
	// and rolling the file because we saw the date change
	startup := true

	// Close any log file that may be open
	if w.file != nil {
		w.file.Close()
		startup = false
	}
	switch (w.filenameSuffix) {
	case dateSuffix:
		return w.dateRotate(startup)
	case intSuffix:
		return w.intRotate()
	default:
		return fmt.Errorf("Invalid log filename format %v", w.filenameSuffix)
	}
}

// Rotate the file, using the standard log4j format `filename-yyyy-MM-dd`.
// If we're starting up we stat the existing file and use the last modified time to determine
// whether to roll it. When there's an existing log file open (!startup), we always roll the log file.
func (w *FileLogWriter) dateRotate(startup bool) error {
	now := w.now()
	stat, err := os.Lstat(w.filename)
	if err == nil {
		var logYear, logDay int
		var logMonth time.Month
		nowYear, nowMonth, nowDay := now.Date()

		if startup {
			// On startup, use the last modified time to figure out what the last timestamp
			// in the file will be
			logYear, logMonth, logDay = stat.ModTime().Date()

			// If the file was written earlier today, append to it
			if logDay == nowDay && logMonth == nowMonth && logYear == nowYear {
				fd, err := os.OpenFile(w.filename, os.O_WRONLY|os.O_APPEND, 0660)
				if err != nil {
					return err
				}
				w.file = fd
				w.daily_opendate = now
				w.maxlines_curlines = 0
				w.maxsize_cursize = int(stat.Size())
				return nil
			}
		}  else {
			// If we're rolling the log, use the day it was opened as the last timestamp
			logYear, logMonth, logDay = w.daily_opendate.Date()
		}

		// Try to rename the file by appending the date. If this collides with an existing file,
		// append a counter to the end to avoid data loss
		var renamed bool
		for i:=0; i < 999; i++ {
			var fileName string
			if i == 0 {
				fileName = fmt.Sprintf("%s.%4d-%02d-%02d", w.filename, logYear, logMonth, logDay)
			} else {
				fileName = fmt.Sprintf("%s.%4d-%02d-%02d.%03d", w.filename, logYear, logMonth, logDay, i)
			}
			fmt.Printf("Trying to move %q to %q \n", w.filename, fileName)
			if _, err := os.Lstat(fileName); os.IsNotExist(err) {
				if err := os.Rename(w.filename, fileName); err != nil {
					return fmt.Errorf("Rotate: Cannot rename file to %q: %v", fileName, err)
				}
				renamed = true
				break
			} else if err != nil {
				return fmt.Errorf("Rotate: could not stat file %q: %v", fileName, err)
			}
		}
		if !renamed {
			return fmt.Errorf("Rotate: failed, could not find free log number %q", w.filename)
		}
	}

	// Open a new log file
	fd, err := os.OpenFile(w.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	w.file = fd

	fmt.Fprint(w.file, FormatLogRecord(w.header, &LogRecord{Created: now}))

	// Set the daily open date to the current date
	w.daily_opendate = now
	return nil
}

// If this is called in a threaded context, it MUST be synchronized
func (w *FileLogWriter) intRotate() error {
	// If we are keeping log files, move it to the next available number
	if w.rotate {
		_, err := os.Lstat(w.filename)
		if err == nil { // file exists
			// Find the next available number
			num := 1
			fname := ""
			for ; err == nil && num <= 999; num++ {
				fname = w.filename + fmt.Sprintf(".%03d", num)
				_, err = os.Lstat(fname)
			}
			// return error if the last file checked still existed
			if err == nil {
				return fmt.Errorf("Rotate: Cannot find free log number to rename %s\n", w.filename)
			}

			// Rename the file to its newfound home
			err = os.Rename(w.filename, fname)
			if err != nil {
				return fmt.Errorf("Rotate: %s\n", err)
			}
		}
	}

	// Open the log file
	fd, err := os.OpenFile(w.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	w.file = fd

	now := w.now()
	fmt.Fprint(w.file, FormatLogRecord(w.header, &LogRecord{Created: now}))

	// Set the daily open date to the current date
	w.daily_opendate = now

	// initialize rotation values
	w.maxlines_curlines = 0
	w.maxsize_cursize = 0

	return nil
}

// Set the logging format (chainable).  Must be called before the first log
// message is written.
func (w *FileLogWriter) SetFormat(format string) *FileLogWriter {
	w.format = format
	return w
}

// Set the logfile header and footer (chainable).  Must be called before the first log
// message is written.  These are formatted similar to the FormatLogRecord (e.g.
// you can use %D and %T in your header/footer for date and time).
func (w *FileLogWriter) SetHeadFoot(head, foot string) *FileLogWriter {
	w.header, w.trailer = head, foot
	if w.maxlines_curlines == 0 {
		fmt.Fprint(w.file, FormatLogRecord(w.header, &LogRecord{Created: w.now()}))
	}
	return w
}

// Set rotate at linecount (chainable). Must be called before the first log
// message is written.
func (w *FileLogWriter) SetRotateLines(maxlines int) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotateLines: %v\n", maxlines)
	w.maxlines = maxlines
	return w
}

// Set rotate at size (chainable). Must be called before the first log message
// is written.
func (w *FileLogWriter) SetRotateSize(maxsize int) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotateSize: %v\n", maxsize)
	w.maxsize = maxsize
	return w
}

// Set rotate daily (chainable). Must be called before the first log message is
// written.
func (w *FileLogWriter) SetRotateDaily(daily bool) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotateDaily: %v\n", daily)
	w.daily = daily
	return w
}

// SetRotate changes whether or not the old logs are kept. (chainable) Must be
// called before the first log message is written.  If rotate is false, the
// files are overwritten; otherwise, they are rotated to another file before the
// new log is opened.
func (w *FileLogWriter) SetRotate(rotate bool) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotate: %v\n", rotate)
	w.rotate = rotate
	return w
}

// NewXMLLogWriter is a utility method for creating a FileLogWriter set up to
// output XML record log messages instead of line-based ones.
func NewXMLLogWriter(fname string, rotate bool, suffix rotateSuffix) *FileLogWriter {
	return NewFileLogWriter(fname, rotate, suffix).SetFormat(
		`	<record level="%L">
		<timestamp>%D %T</timestamp>
		<source>%S</source>
		<message>%M</message>
	</record>`).SetHeadFoot("<log created=\"%D %T\">", "</log>")
}
