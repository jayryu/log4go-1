// Copyright (C) 2010, Kyle Lemons <kyle@kylelemons.net>.  All rights reserved.

package log4go

import (
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"
)

// Time format
const (
	SuffixDateFormat = "2006-01-02"
)

const (
	FILELOG_ARCHIVE_REGEX = `\.[0-9]{4}-[0-9]{2}-[0-9]{2}(\.[0-9]{4})?(\.gz|\.zip)?$`
)

type CompressionMethod string

const (
	COMPRESSION_GZIP CompressionMethod = "gz"
	COMPRESSION_ZIP  CompressionMethod = "zip"
)

// Helper date comparison
func dateEqual(first time.Time, second time.Time) bool {
	firstYear, firstMonth, firstDay := first.Date()
	secondYear, secondMonth, secondDay := second.Date()
	if firstYear == secondYear && firstMonth == secondMonth && firstDay == secondDay {
		return true
	}
	return false
}

// Create directory and check basic permissions
func makeDirectory(filename string) error {
	// Create directory if doesn't exist
	logDir := filepath.Dir(filename)
	if err := os.MkdirAll(logDir, os.ModeDir|os.ModePerm); err != nil {
		return err
	}

	// Ensure we at least have permissions to stat the directory.
	// This could fail, for example, when we don't have permissions
	// to read the parent directory
	if _, err := os.Stat(logDir); os.IsPermission(err) {
		return err
	}

	return nil
}

// This log writer sends output to a file
type FileLogWriter struct {
	rec             chan *LogRecord
	rot             chan bool
	completed       chan int
	backgroundTasks chan string
	wg              *sync.WaitGroup

	// The opened file
	filename string
	file     *os.File

	// The error channel
	errorWriter io.Writer

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
	daily_opendate int

	// Keep old logfiles
	rotate bool

	// Use date-based rotation
	rotateDateSuffix bool

	// Rotate on startup
	rotateOnStartup             bool
	currentFileExistedAtStartup bool

	// Archive (age-off) options
	filesToKeep    int
	logfileMatcher *regexp.Regexp

	// Compression
	compress          bool
	compressionMethod CompressionMethod

	// Failure counters
	rotationFailures uint64
	writeFailures    uint64

	// Whether we've fully started, that is, received our first log message
	started bool
}

// This is the FileLogWriter's output method
func (w *FileLogWriter) LogWrite(rec *LogRecord) {
	w.rec <- rec
}

func (w *FileLogWriter) Close() {
	close(w.rec)
	<-w.completed
	close(w.backgroundTasks)
	w.wg.Wait()
}

// Track write failures and prints to stderr when possible. If err is nil, we'll try to clear the failures
func (w *FileLogWriter) handleWriteFailure(err error) {
	// Try to note any previous failures
	if w.writeFailures != 0 {
		_, fprintfErr := fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Dropped %d previous log message(s)\n", w.filename, w.writeFailures)
		if fprintfErr != nil {
			// If we can't print now, exit early and try later
			if err != nil {
				w.writeFailures += 1
			}
			return
		} else {
			w.writeFailures = 0
		}
	}
	// If we have a current failure, attempt to print it
	if err != nil {
		_, fprintfErr := fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Write failed: %v\n", w.filename, err)
		if fprintfErr != nil {
			w.writeFailures += 1
		}
	}
}

// Track rotation failures and prints to stderr when possible. If err is nil, we'll try to clear the failures
func (w *FileLogWriter) handleRotationFailure(err error) {
	// Try to note any previous failures
	if w.rotationFailures != 0 {
		_, fprintfErr := fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): %d previous rotation failures occurred\n", w.filename, w.rotationFailures)
		if fprintfErr != nil {
			// If we can't print now, exit early and try later
			if err != nil {
				w.rotationFailures += 1
			}
			return
		} else {
			w.rotationFailures = 0
		}
	}
	// If we have a current failure, attempt to print it
	if err != nil {
		_, fprintfErr := fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Rotation failed: %v\n", w.filename, err)
		if fprintfErr != nil {
			w.rotationFailures += 1
		}
	}
}

// This is called on first log write
func (w *FileLogWriter) handleStartupRotation() error {
	// Skip rotation if the current file didn't exist at startup
	if w.currentFileExistedAtStartup == false {
		return nil
	}

	// open the file for the first time, rotating only if necessary
	fileInfo, fileInfoErr := os.Lstat(w.filename)
	if fileInfoErr == nil {
		if !dateEqual(fileInfo.ModTime(), time.Now()) || w.rotateOnStartup {
			if err := w.handleRotate(fileInfo.ModTime()); err != nil {
				fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): %s\n", w.filename, err)
				return err
			}
		}
	}
	return nil
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
func NewFileLogWriter(fname string, rotate bool, compress bool) *FileLogWriter {
	w := &FileLogWriter{
		rec:                         make(chan *LogRecord, LogBufferLength),
		rot:                         make(chan bool),
		backgroundTasks:             make(chan string, 1),
		completed:                   make(chan int),
		filename:                    fname,
		format:                      "[%D %T] [%L] (%S) %M",
		rotate:                      rotate,
		rotateDateSuffix:            false,
		rotateOnStartup:             true,
		currentFileExistedAtStartup: true,
		compress:                    compress,
		compressionMethod:           FILELOG_DEFAULT_COMPRESSION_METHOD,
		errorWriter:                 os.Stderr,
		started:                     false,
		filesToKeep:                 30,
		wg:                          &sync.WaitGroup{},
	}

	// Compile the regex to match against files to archive
	logfilePrefix := filepath.Base(w.filename)
	logfileMatcher, err := regexp.Compile("^" + regexp.QuoteMeta(logfilePrefix) + FILELOG_ARCHIVE_REGEX)
	if err != nil {
		return nil
	}
	w.logfileMatcher = logfileMatcher

	// If the current file doesn't exist, we should short-circuit handleStartupRotation,
	// or we will rotate twice
	if _, err := os.Lstat(w.filename); os.IsNotExist(err) {
		w.currentFileExistedAtStartup = false
	}

	// Open the file initially; startup rotation is handled on first log message
	if err := w.openLogFile(); err != nil {
		fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): %s\n", w.filename, err)
		return nil
	}

	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		defer func() {
			if w.file != nil {
				fmt.Fprint(w.file, FormatLogRecord(w.trailer, &LogRecord{Created: time.Now()}))
				w.file.Close()
			}
		}()

		for {
			if w.started == false {
				err := w.handleStartupRotation()
				w.handleRotationFailure(err)
				w.started = true
			}
			select {
			case <-w.rot:
				err := w.handleRotate(time.Now())
				w.handleRotationFailure(err)
			case rec, ok := <-w.rec:
				if !ok {
					close(w.completed)
					return
				}
				now := time.Now()
				if (w.maxlines > 0 && w.maxlines_curlines >= w.maxlines) ||
					(w.maxsize > 0 && w.maxsize_cursize >= w.maxsize) {
					err := w.handleRotate(now)
					w.handleRotationFailure(err)
				} else if w.daily && now.Day() != w.daily_opendate {
					// Since we crossed the time boundary, back the date up by one day
					err := w.handleRotate(now.Add(-1 * 24 * time.Hour))
					w.handleRotationFailure(err)
				}

				// Perform the write
				n, err := fmt.Fprint(w.file, FormatLogRecord(w.format, rec))
				w.handleWriteFailure(err)

				// Update the counts
				w.maxlines_curlines++
				w.maxsize_cursize += n
			}
		}
	}()

	// Background tasks goroutine
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()

		for filename := range w.backgroundTasks {
			if w.filesToKeep > 0 {
				dir := filepath.Dir(filename)
				err := w.archiveFiles(dir)
				if err != nil {
					fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't archive files: %s\n", filename, err)
				}
			}

			if w.compress {
				compressedFilename := filename + "." + string(w.compressionMethod)
				compressedInprogressFilename := compressedFilename + ".inprogress"

				success := w.compressFile(filename, compressedInprogressFilename, w.compressionMethod)
				if success {
					w.moveCompressedFile(filename, compressedFilename, compressedInprogressFilename)
				} else {
					w.deleteInprogressFile(compressedInprogressFilename)
				}
			}
		}
	}()

	return w
}

func (w *FileLogWriter) archiveFiles(dir string) error {

	// Get a handle to the directory
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}

	dirInfo, err := dirFile.Stat()
	if err != nil {
		return err
	}
	if !dirInfo.IsDir() {
		return fmt.Errorf("%q must be a directory", dir)
	}

	var filesInDir []string
	var matchedFiles []string

	for {
		filesInDir, err = dirFile.Readdirnames(1000)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		for _, fullFilename := range filesInDir {
			baseFilename := filepath.Base(fullFilename)
			if !w.logfileMatcher.MatchString(baseFilename) {
				// Not interested in this file
				continue
			}

			matchedFiles = append(matchedFiles, fullFilename)
		}
	}

	// matchedFiles contains all the logfiles that matched the regexp.
	// When sorted, we can find the oldest files because the suffixes are
	// fixed width - .log.YYYY-MM-DD
	sort.Strings(matchedFiles)

	// Remove unwanted files
	if len(matchedFiles) > w.filesToKeep {
		for _, filename := range matchedFiles[0 : len(matchedFiles)-w.filesToKeep] {
			os.Remove(filename)
		}
	}

	return nil
}

// Compress a file after it has been rotated.
// plainFile - name of the rotated, uncompressed file
// compressedInprogressFilename - name of a temporary file to hold compressed data
// method - type of compression to use
func (w *FileLogWriter) compressFile(plainFilename, compressedInprogressFilename string, method CompressionMethod) bool {
	plainFile, err := os.Open(plainFilename)
	if err != nil {
		fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't open logfile %q to begin compression: %s\n", w.filename, plainFilename, err)
		return false
	}
	defer plainFile.Close()

	compressedFile, err := os.Create(compressedInprogressFilename)
	if err != nil {
		fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't open new compressed file %q: %s\n", w.filename, compressedInprogressFilename, err)
		return false
	}

	// Defer closing the underlying file
	defer func() {
		err = compressedFile.Close()
		if err != nil {
			fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't close file %q: %s\n", w.filename, compressedInprogressFilename, err)
		}
	}()

	var compressedFileWriter io.Writer
	switch method {
	case "gz":
		gzipWriter := gzip.NewWriter(compressedFile)

		defer func() {
			err = gzipWriter.Close()
			if err != nil {
				fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't close gzip writer on %q: %s\n", w.filename, compressedInprogressFilename, err)
			}
		}()
		compressedFileWriter = gzipWriter
	case "zip":
		// zipWriter is the outer container, compressedFileWriter is an entry inside the zip
		zipWriter := zip.NewWriter(compressedFile)

		// Defer closing the zip writer
		defer func() {
			err = zipWriter.Close()
			if err != nil {
				fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't close zip writer on %q: %s\n", w.filename, compressedInprogressFilename, err)
			}
		}()

		// compressedFileWriter, the zip file entry, doesn't have a Close() method
		basename := filepath.Base(plainFilename)
		compressedFileWriter, err = zipWriter.Create(basename)
		if err != nil {
			fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't open zip file entry: %s\n", w.filename, err)
			return false
		}
	default:
		fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Unknown compression method: %q\n", w.filename, method)
		return false
	}

	// Read plain file, write to compressed file
	_, err = io.Copy(compressedFileWriter, plainFile)
	if err != nil {
		fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't write compressed file %q: %s\n", w.filename, compressedInprogressFilename, err)
		return false
	}

	return true
}

func (w *FileLogWriter) moveCompressedFile(plainFilename, compressedFilename, compressedInprogressFilename string) {
	// Rename compressed file
	err := os.Rename(compressedInprogressFilename, compressedFilename)
	if err != nil {
		fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't rename file %q to %q: %s\n", w.filename, compressedInprogressFilename, compressedFilename, err)
		return
	}

	// Delete plain file
	err = os.Remove(plainFilename)
	if err != nil {
		fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't remove file %q: %s\n", w.filename, plainFilename, err)

		// If we can't remove the old plain file, delete the compressed file so we don't affect the
		// retention of archived files
		err = os.Remove(compressedFilename)
		if err != nil {
			fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't remove file %q: %s\n", w.filename, compressedFilename, err)
		}
		return
	}
}

func (w *FileLogWriter) deleteInprogressFile(compressedInprogressFilename string) {
	err := os.Remove(compressedInprogressFilename)
	if err != nil {
		fmt.Fprintf(w.errorWriter, "FileLogWriter(%q): Couldn't remove temporary file %q: %s\n", w.filename, compressedInprogressFilename, err)
	}
}

// Request that the logs rotate
func (w *FileLogWriter) Rotate() {
	w.rot <- true
}

// Generate the next filename for rotation using integer suffix
func (w *FileLogWriter) nextIntegerFilename(filename string) (string, error) {
	for i := 1; i <= 999; i++ {
		fullName := filename + fmt.Sprintf(".%03d", i)
		if _, err := os.Lstat(fullName); os.IsNotExist(err) {
			return fullName, nil
		}
	}

	return "", fmt.Errorf("Rotate: Cannot find free log number to rename %s\n", filename)
}

// Generate the next filename for rotation using date suffix
func (w *FileLogWriter) nextDateFilename(filename string, suffix string) (string, error) {
	// Attempt filename.suffix
	fullName := fmt.Sprintf("%s.%s", filename, suffix)
	if _, err := os.Stat(fullName); os.IsNotExist(err) {
		// File does not exist, return it as the next filename
		return fullName, nil
	}

	// If necessary, add integer suffix
	var lastErr error
	var lastFullname string
	for i := 1; i < 10000; i++ {
		fullNameWithSuffix := fmt.Sprintf("%s.%s.%04d", filename, suffix, i)
		if _, err := os.Stat(fullNameWithSuffix); os.IsNotExist(err) {
			return fullNameWithSuffix, nil
		} else {
			lastErr = err
		}
	}

	return "", fmt.Errorf("Cannot rotate %s to %s: %v\n", filename, lastFullname, lastErr)
}

// If this is called in a threaded context, it MUST be synchronized
func (w *FileLogWriter) handleRotate(rotateTime time.Time) error {
	rotatedName := ""

	// If we are keeping log files, move it to the correct date
	if w.rotate {
		_, err := os.Lstat(w.filename)
		if err == nil { // file exists
			var nextFilenameErr error
			if w.rotateDateSuffix {
				dateSuffix := rotateTime.Format(SuffixDateFormat)
				rotatedName, nextFilenameErr = w.nextDateFilename(w.filename, dateSuffix)
			} else {
				rotatedName, nextFilenameErr = w.nextIntegerFilename(w.filename)
			}
			if nextFilenameErr != nil {
				return nextFilenameErr
			}

			w.closeLogFile()

			// Rename the file to its newfound home
			err = os.Rename(w.filename, rotatedName)
			if err != nil {
				return fmt.Errorf("Rotate: %s\n", err)
			}

			// If we're configured to archive files, signal the background goroutine
			if w.filesToKeep > 0 {
				w.backgroundTasks <- rotatedName
			}
		}
	}

	return w.openLogFile()
}

func (w *FileLogWriter) closeLogFile() {
	// Close any log file that may be open
	if w.file != nil {
		fmt.Fprint(w.file, FormatLogRecord(w.trailer, &LogRecord{Created: time.Now()}))
		w.file.Close()
		w.file = nil
	}
}

func (w *FileLogWriter) openLogFile() error {
	if err := makeDirectory(w.filename); err != nil {
		return err
	}

	// Open the log file
	fd, err := os.OpenFile(w.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}

	w.closeLogFile()
	w.file = fd

	now := time.Now()
	fmt.Fprint(w.file, FormatLogRecord(w.header, &LogRecord{Created: now}))

	// Set the daily open date to the current date
	w.daily_opendate = now.Day()

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
		fmt.Fprint(w.file, FormatLogRecord(w.header, &LogRecord{Created: time.Now()}))
	}
	return w
}

// Set rotate at linecount (chainable). Must be called before the first log
// message is written.
func (w *FileLogWriter) SetRotateLines(maxlines int) *FileLogWriter {
	//fmt.Fprintf(w.errorWriter, "FileLogWriter.SetRotateLines: %v\n", maxlines)
	w.maxlines = maxlines
	return w
}

// Set rotate at size (chainable). Must be called before the first log message
// is written.
func (w *FileLogWriter) SetRotateSize(maxsize int) *FileLogWriter {
	//fmt.Fprintf(w.errorWriter, "FileLogWriter.SetRotateSize: %v\n", maxsize)
	w.maxsize = maxsize
	return w
}

// Set rotate daily (chainable). Must be called before the first log message is
// written.
func (w *FileLogWriter) SetRotateDaily(daily bool) *FileLogWriter {
	//fmt.Fprintf(w.errorWriter, "FileLogWriter.SetRotateDaily: %v\n", daily)
	w.daily = daily
	return w
}

// SetRotate changes whether or not the old logs are kept. (chainable) Must be
// called before the first log message is written.  If rotate is false, the
// files are overwritten; otherwise, they are rotated to another file before the
// new log is opened.
func (w *FileLogWriter) SetRotate(rotate bool) *FileLogWriter {
	//fmt.Fprintf(w.errorWriter, "FileLogWriter.SetRotate: %v\n", rotate)
	w.rotate = rotate
	return w
}

// SetRotateDateSuffix uses date rotation (.YYYY-MM-DD) instead of
// integer-based rotation (.001, .002, etc) (chainable)
func (w *FileLogWriter) SetRotateDateSuffix(dateSuffix bool) *FileLogWriter {
	w.rotateDateSuffix = dateSuffix
	return w
}

// SetRotateOnStartup determines wheter to rotate the logfile on startup.
// When true, rotate the logfile at every startup. When false, rotate the
// logfile only when the date of the existing logfile is different than the
// current date
func (w *FileLogWriter) SetRotateOnStartup(rotateOnStartup bool) *FileLogWriter {
	w.rotateOnStartup = rotateOnStartup
	return w
}

// SetMaxArchiveFiles determines the maximum number of kept log files before
// age-off. To keep all log files, set to 0.
func (w *FileLogWriter) SetMaxArchiveFiles(filesToKeep int) *FileLogWriter {
	w.filesToKeep = filesToKeep
	return w
}

// SetCompressionMethod determines the type of compression to use. Valid options
// are "gz" and "zip"
func (w *FileLogWriter) SetCompressionMethod(compressionMethod CompressionMethod) *FileLogWriter {
	w.compressionMethod = compressionMethod
	return w
}

// NewXMLLogWriter is a utility method for creating a FileLogWriter set up to
// output XML record log messages instead of line-based ones.
func NewXMLLogWriter(fname string, rotate bool) *FileLogWriter {
	return NewFileLogWriter(fname, rotate, false).SetFormat(
		`	<record level="%L">
		<timestamp>%D %T</timestamp>
		<source>%S</source>
		<message>%M</message>
	</record>`).SetHeadFoot("<log created=\"%D %T\">", "</log>")
}
