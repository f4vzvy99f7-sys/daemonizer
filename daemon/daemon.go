// Package daemon provides a library for building long-lived daemon processes
// with type-safe IPC for Go CLI tools. A single binary acts as both the daemon
// and the client: the daemon is automatically started and detached when the
// client first connects, and subsequent invocations communicate with it over a
// Unix socket.
//
// Handlers run exclusively in the daemon process. All communication between
// client and daemon happens through function arguments and return values — there
// is no shared memory.
//
// Usage:
//
//	type MyClient struct {
//	    Add   func(a, b int) (int, error)
//	    Greet func(name string) (string, error)
//	}
//
//	var client = daemon.Client[MyClient]("my-service", func(ctx context.Context, impl *MyClient, _ daemon.Args) (daemon.CleanupFunc, error) {
//	    impl.Add = func(a, b int) (int, error) { return a + b, nil }
//	    impl.Greet = func(name string) (string, error) {
//	        return fmt.Sprintf("Hello, %s!", name), nil
//	    }
//	    return func() { /* cleanup */ }, nil
//	})
//
//	func main() {
//	    if daemon.IsRunning(client) {
//	        fmt.Println(client.Add(1, 2)) // 3
//	    } else {
//	        daemon.Start(client, nil)
//	    }
//	}
//
// Runtime files (socket, PID) are stored under $XDG_RUNTIME_DIR/<name>.
// Log files are stored under $XDG_STATE_HOME/<name>/<name>.log.
package daemon

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ─── Wire protocol types ────────────────────────────────────────────────────

// frameType tags each length-prefixed frame on the wire.
type frameType byte

const (
	frameRequest    frameType = 0x01
	frameResponse   frameType = 0x02
	frameWriterData frameType = 0x05
)

// request is the gob-encoded payload sent from client to daemon.
type request struct {
	Method string
	Args   []byte // gob-encoded argument values
}

// response is the gob-encoded payload sent from daemon to client.
type response struct {
	Result []byte // gob-encoded return values (excluding the trailing error)
	Error  string // non-empty if the handler returned a non-nil error
}

// ─── Environment variable used to identify the daemon process ───────────────

const daemonEnvKey = "__DAEMON_SERVICE"
const daemonReadyFDEnvKey = "__DAEMON_READY_FD"
const daemonPipeStdoutKey = "DAEMONIZER_PIPE_STDOUT"
const daemonPipeStderrKey = "DAEMONIZER_PIPE_STDERR"
const daemonArgsKey = "DAEMONIZER_ARGS"

// ErrNotRunning is returned by Start and Stop when the daemon process is not
// running.
var ErrNotRunning = errors.New("daemon is not running")

// registry maps client pointers (*T as any) to their associated *Daemon.
// Populated by Client; used by Start, Stop, and IsRunning.
var registry sync.Map

var daemonLogger *log.Logger
var daemonLogFile *os.File
var daemonLogPath string

// ─── Public types ───────────────────────────────────────────────────────────

// CleanupFunc is a function returned by the Client setup function that runs on
// daemon shutdown.
type CleanupFunc func()

// Args is a map of named arguments passed to the daemon setup function from
// StartupOptions. Values are JSON-serialized to an env var and reconstructed
// inside the daemon process.
type Args map[string]string

// Writer is an io.Writer that can be passed as an argument to daemon handler
// functions. On the client side, Wrap creates a Writer from any io.Writer.
// On the daemon side, the handler receives a Writer whose Write method sends
// data back to the caller's writer over the IPC connection.
type Writer struct {
	w  io.Writer // client-side underlying writer; nil on daemon side
	id uint32    // per-request identifier assigned by the IPC layer
}

func (w Writer) Write(p []byte) (int, error) { return w.w.Write(p) }

// GobEncode encodes only the writer ID. The io.Writer itself is not
// serialized — it is reconstructed on the daemon side.
func (w Writer) GobEncode() ([]byte, error) {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], w.id)
	return buf[:], nil
}

func (w *Writer) GobDecode(data []byte) error {
	w.id = binary.BigEndian.Uint32(data)
	w.w = nil
	return nil
}

// Wrap wraps an io.Writer into a Writer for passing to daemon handler
// functions. Typically used on the client side:
//
//	client.PrintRecords(daemon.Wrap(os.Stdout))
func Wrap(w io.Writer) Writer { return Writer{w: w} }

// StartupOptions configures the daemon process when calling Start.
type StartupOptions struct {
	Args                Args
	PipeStdoutToLogfile bool // redirect os.Stdout to daemon log file
	PipeStderrToLogfile bool // redirect os.Stderr to daemon log file
}

func Start(client any, opts *StartupOptions) error {
	d, ok := registry.Load(client)
	if !ok {
		return fmt.Errorf("client not found in registry")
	}
	env := make(map[string]string)
	if opts != nil {
		if len(opts.Args) > 0 {
			jsonData, err := json.Marshal(opts.Args)
			if err != nil {
				return fmt.Errorf("failed to marshal args: %w", err)
			}
			env[daemonArgsKey] = string(jsonData)
		}
		if opts.PipeStdoutToLogfile {
			env[daemonPipeStdoutKey] = "1"
		}
		if opts.PipeStderrToLogfile {
			env[daemonPipeStderrKey] = "1"
		}
	}
	return d.(*Daemon).start(env)
}

func Stop(client any) error {
	d, ok := registry.Load(client)
	if !ok {
		return fmt.Errorf("client not found in registry")
	}
	return d.(*Daemon).stop()
}

func IsRunning(client any) bool {
	d, ok := registry.Load(client)
	if !ok {
		return false
	}
	return d.(*Daemon).isRunning()
}

// ─── Handler storage ────────────────────────────────────────────────────────

// handler stores a registered handler function alongside its reflected type.
type handler struct {
	fn     reflect.Value
	fnType reflect.Type
}

// Daemon holds configuration for a named service.
type Daemon struct {
	name     string
	handlers map[string]handler
	cleanup  CleanupFunc
	mu       sync.Mutex
	conn     net.Conn // client-side connection; nil until first RPC, nilled by stop()
}

// ─── Paths ──────────────────────────────────────────────────────────────────

func (d *Daemon) runtimeDir() string {
	dir := os.Getenv("XDG_RUNTIME_DIR")
	if dir == "" {
		dir = filepath.Join(os.TempDir(), fmt.Sprintf("daemon-%d", os.Getuid()))
	}
	return dir
}

func (d *Daemon) stateDir() string {
	dir := os.Getenv("XDG_STATE_HOME")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			home = os.TempDir()
		}
		dir = filepath.Join(home, ".local", "state")
	}
	return filepath.Join(dir, d.name)
}

func (d *Daemon) socketPath() string {
	return filepath.Join(d.runtimeDir(), d.name+".sock")
}

func (d *Daemon) pidPath() string {
	return filepath.Join(d.runtimeDir(), d.name+".pid")
}

func (d *Daemon) logPath() string {
	return filepath.Join(d.stateDir(), d.name+".log")
}

// getConn returns the cached client connection, dialing if necessary.
// Caller must hold d.mu.
func (d *Daemon) getConn() (net.Conn, error) {
	if d.conn != nil {
		return d.conn, nil
	}
	conn, err := net.Dial("unix", d.socketPath())
	if err != nil {
		return nil, ErrNotRunning
	}
	d.conn = conn
	return conn, nil
}

// ─── Client creation ────────────────────────────────────────────────────────

// Client creates a daemon client for the given service name.
//
// If the current process IS the daemon (detected via an environment variable),
// Client runs setup, starts serving requests, and never returns (os.Exit).
//
// If the current process is a client, Client creates IPC stubs for each func
// field on T and returns a *T ready for use.
//
// Client panics on configuration errors — it is designed to be called at
// package level and fail fast if things are misconfigured.
func Client[T any](name string, setup func(ctx context.Context, impl *T, args Args) (CleanupFunc, error)) *T {
	d := &Daemon{name: name}

	// ── Daemon branch ──
	if os.Getenv(daemonEnvKey) == d.name {
		// Initialize logger before any user code runs.
		if err := os.MkdirAll(d.stateDir(), 0700); err != nil {
			fmt.Fprintf(os.Stderr, "failed to create state dir: %v\n", err)
			os.Exit(1)
		}
		logFile, err := os.OpenFile(d.logPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open log file: %v\n", err)
			os.Exit(1)
		}
		daemonLogPath = d.logPath()
		daemonLogFile = logFile
		daemonLogger = log.New(logFile, "", log.LstdFlags)
		defer func() { daemonLogFile.Close() }()

		if os.Getenv(daemonPipeStdoutKey) == "1" {
			os.Stdout = logFile
		}
		if os.Getenv(daemonPipeStderrKey) == "1" {
			os.Stderr = logFile
		}

		// signalReady writes msg to the parent's startup pipe (if present) and
		// closes it. Called once: either "ok" after the socket is bound, or an
		// error string before os.Exit on setup/validation failure.
		signalReady := func(msg string) {
			fdStr := os.Getenv(daemonReadyFDEnvKey)
			if fdStr == "" {
				return
			}
			fd, err := strconv.Atoi(fdStr)
			if err != nil {
				return
			}
			f := os.NewFile(uintptr(fd), "ready")
			if f == nil {
				return
			}
			f.WriteString(msg)
			f.Close()
		}

		ctx, cancel := context.WithCancel(context.Background())

		impl := new(T)
		var args Args
		if argsJSON := os.Getenv(daemonArgsKey); argsJSON != "" {
			if err := json.Unmarshal([]byte(argsJSON), &args); err != nil {
				cancel()
				signalReady(fmt.Sprintf("failed to unmarshal args: %v", err))
				fmt.Fprintf(os.Stderr, "daemon setup error: failed to unmarshal args: %v\n", err)
				os.Exit(1)
			}
		}
		cleanup, err := setup(ctx, impl, args)
		if err != nil {
			cancel()
			signalReady(fmt.Sprintf("setup error: %v", err))
			fmt.Fprintf(os.Stderr, "daemon setup error: %v\n", err)
			os.Exit(1)
		}

		// Extract handlers from impl's func fields.
		v := reflect.ValueOf(impl).Elem()
		t := v.Type()
		reg := make(map[string]handler)
		errType := reflect.TypeFor[error]()

		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if !field.IsExported() || field.Type.Kind() != reflect.Func {
				continue
			}
			fn := v.Field(i)
			if fn.IsNil() {
				cancel()
				signalReady(fmt.Sprintf("field %s is nil — all func fields must be assigned in setup", field.Name))
				fmt.Fprintf(os.Stderr, "daemon setup error: field %s is nil\n", field.Name)
				os.Exit(1)
			}
			ft := field.Type
			if ft.NumOut() == 0 || !ft.Out(ft.NumOut()-1).Implements(errType) {
				cancel()
				signalReady(fmt.Sprintf("field %s must have error as last return value", field.Name))
				fmt.Fprintf(os.Stderr, "daemon setup error: field %s must have error as last return value\n", field.Name)
				os.Exit(1)
			}
			reg[field.Name] = handler{fn: fn, fnType: ft}
		}

		d.handlers = reg
		d.cleanup = cleanup

		// Trap SIGTERM and cancel the daemon context on shutdown.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGTERM)
		go func() {
			<-sigCh
			cancel()
		}()

		if err := d.serve(logFile, func() { signalReady("ok") }); err != nil {
			cancel()
			fmt.Fprintf(os.Stderr, "daemon serve error: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// ── Client branch ──

	client := new(T)
	v := reflect.ValueOf(client).Elem()
	t := v.Type()

	if t.Kind() != reflect.Struct {
		fmt.Fprintf(os.Stderr, "daemon.Client: type parameter must be a struct, got %v\n", t.Kind())
		os.Exit(1)
	}

	errType := reflect.TypeFor[error]()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() || field.Type.Kind() != reflect.Func {
			continue
		}
		ft := field.Type
		if ft.NumOut() == 0 || ft.Out(ft.NumOut()-1) != errType {
			fmt.Fprintf(os.Stderr, "daemon.Client: field %s must have error as last return value\n", field.Name)
			os.Exit(1)
		}
		v.Field(i).Set(makeStub(d, field.Name, ft))
	}

	registry.Store(client, d)
	return client
}

// ─── Client IPC stub ────────────────────────────────────────────────────────

// makeStub creates a reflect.Value function that, when called, serializes
// the arguments, sends an IPC request, and deserializes the response.
// It uses d.getConn() to lazily establish the connection on first call.
func makeStub(d *Daemon, method string, funcType reflect.Type) reflect.Value {
	errType := reflect.TypeFor[error]()
	writerType := reflect.TypeFor[Writer]()

	return reflect.MakeFunc(funcType, func(args []reflect.Value) []reflect.Value {
		numOut := funcType.NumOut()
		results := make([]reflect.Value, numOut)

		for i := range numOut {
			results[i] = reflect.Zero(funcType.Out(i))
		}

		var argBuf bytes.Buffer
		enc := gob.NewEncoder(&argBuf)

		// Track writers for routing frameWriterData frames back.
		var writers []struct {
			id uint32
			w  io.Writer
		}

		for i, arg := range args {
			// Check if this parameter is a Writer.
			if funcType.In(i) == writerType {
				w := arg.Interface().(Writer)
				w.id = uint32(i + 1) // 1-based per-request ID
				writers = append(writers, struct {
					id uint32
					w  io.Writer
				}{w.id, w.w})
				// GobEncode serializes only the id.
				if err := enc.Encode(w); err != nil {
					results[numOut-1] = reflect.ValueOf(fmt.Errorf("failed to encode writer arg %d: %w", i, err)).Convert(errType)
					return results
				}
			} else {
				if err := enc.Encode(arg.Interface()); err != nil {
					results[numOut-1] = reflect.ValueOf(fmt.Errorf("failed to encode arg %d: %w", i, err)).Convert(errType)
					return results
				}
			}
		}

		req := request{
			Method: method,
			Args:   argBuf.Bytes(),
		}

		var reqBuf bytes.Buffer
		if err := gob.NewEncoder(&reqBuf).Encode(req); err != nil {
			results[numOut-1] = reflect.ValueOf(fmt.Errorf("failed to encode request: %w", err)).Convert(errType)
			return results
		}

		d.mu.Lock()
		defer d.mu.Unlock()

		conn, err := d.getConn()
		if err != nil {
			results[numOut-1] = reflect.ValueOf(err).Convert(errType)
			return results
		}

		if err := writeFrame(conn, frameRequest, reqBuf.Bytes()); err != nil {
			results[numOut-1] = reflect.ValueOf(fmt.Errorf("failed to send request: %w", err)).Convert(errType)
			return results
		}

		for {
			ft, data, err := readFrame(conn)
			if err != nil {
				results[numOut-1] = reflect.ValueOf(fmt.Errorf("failed to read response: %w", err)).Convert(errType)
				return results
			}

			switch ft {
			case frameWriterData:
				if len(data) < 1 {
					continue
				}
				id := data[0]
				payload := data[1:]
				for _, w := range writers {
					if byte(w.id) == id {
						w.w.Write(payload)
						break
					}
				}

			case frameResponse:
				var resp response
				if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&resp); err != nil {
					results[numOut-1] = reflect.ValueOf(fmt.Errorf("failed to decode response: %w", err)).Convert(errType)
					return results
				}

				if resp.Error != "" {
					results[numOut-1] = reflect.ValueOf(errors.New(resp.Error)).Convert(errType)
					return results
				}

				dec := gob.NewDecoder(bytes.NewReader(resp.Result))
				for i := 0; i < numOut-1; i++ {
					ptr := reflect.New(funcType.Out(i))
					if err := dec.Decode(ptr.Interface()); err != nil {
						results[numOut-1] = reflect.ValueOf(fmt.Errorf("failed to decode return value %d: %w", i, err)).Convert(errType)
						return results
					}
					results[i] = ptr.Elem()
				}
				return results
			}
		}
	})
}

// ─── Daemon server ──────────────────────────────────────────────────────────

// serve starts the daemon: acquires the PID file lock, listens on the Unix
// socket, and handles incoming connections. logFile is already opened by
// Client before setup runs.
func (d *Daemon) serve(logFile *os.File, onReady func()) error {
	if err := os.MkdirAll(d.runtimeDir(), 0700); err != nil {
		return fmt.Errorf("failed to create runtime dir: %w", err)
	}

	if daemonLogger == nil {
		daemonLogger = log.New(logFile, "", log.LstdFlags)
	}

	// Acquire exclusive lock on PID file.
	pidFile, err := os.OpenFile(d.pidPath(), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("failed to open PID file: %w", err)
	}
	defer pidFile.Close()

	if err := syscall.Flock(int(pidFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("another daemon instance is running (could not acquire lock): %w", err)
	}

	pidFile.Truncate(0)
	pidFile.Seek(0, 0)
	fmt.Fprintf(pidFile, "%d\n", os.Getpid())
	pidFile.Sync()

	os.Remove(d.socketPath())

	ln, err := net.Listen("unix", d.socketPath())
	if err != nil {
		return fmt.Errorf("failed to listen on socket: %w", err)
	}
	defer os.Remove(d.socketPath())

	onReady()

	// Trap SIGTERM and close the listener to break the accept loop.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		<-sigCh
		ln.Close()
	}()

	var wg sync.WaitGroup
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				break
			}
			continue
		}
		wg.Go(func() {
			d.handleConnection(conn)
		})
	}

	wg.Wait()

	// Run the cleanup function returned by setup.
	if d.cleanup != nil {
		d.cleanup()
	}

	return nil
}

// handleConnection reads requests from a single client connection and
// dispatches them to the appropriate handler.
func (d *Daemon) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		ft, data, err := readFrame(conn)
		if err != nil {
			return
		}
		if ft != frameRequest {
			continue
		}

		var req request
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&req); err != nil {
			d.sendErrorResponse(conn, fmt.Sprintf("failed to decode request: %v", err))
			continue
		}

		d.dispatch(conn, &req)
	}
}

// dispatch calls the registered handler for a request and sends back the
// response, including any writer data.
func (d *Daemon) dispatch(conn net.Conn, req *request) {
	h, ok := d.handlers[req.Method]
	if !ok {
		d.sendErrorResponse(conn, fmt.Sprintf("unknown method: %q", req.Method))
		return
	}

	dec := gob.NewDecoder(bytes.NewReader(req.Args))
	args := make([]reflect.Value, h.fnType.NumIn())
	writerType := reflect.TypeFor[Writer]()

	for i := 0; i < h.fnType.NumIn(); i++ {
		paramType := h.fnType.In(i)

		if paramType == writerType {
			var w Writer
			if err := dec.Decode(&w); err != nil {
				d.sendErrorResponse(conn, fmt.Sprintf("failed to decode writer arg %d: %v", i, err))
				return
			}
			w.w = &connWriter{conn: conn, id: w.id}
			args[i] = reflect.ValueOf(w)
		} else {
			ptr := reflect.New(paramType)
			if err := dec.Decode(ptr.Interface()); err != nil {
				d.sendErrorResponse(conn, fmt.Sprintf("failed to decode arg %d: %v", i, err))
				return
			}
			args[i] = ptr.Elem()
		}
	}

	results := h.fn.Call(args)

	errVal := results[len(results)-1]
	var resp response
	if !errVal.IsNil() {
		resp.Error = errVal.Interface().(error).Error()
	} else {
		var resultBuf bytes.Buffer
		enc := gob.NewEncoder(&resultBuf)
		for i := 0; i < len(results)-1; i++ {
			if err := enc.Encode(results[i].Interface()); err != nil {
				d.sendErrorResponse(conn, fmt.Sprintf("failed to encode return value %d: %v", i, err))
				return
			}
		}
		resp.Result = resultBuf.Bytes()
	}

	var respBuf bytes.Buffer
	if err := gob.NewEncoder(&respBuf).Encode(resp); err != nil {
		return
	}
	writeFrame(conn, frameResponse, respBuf.Bytes())
}

// connWriter sends data back to the client as frameWriterData frames.
type connWriter struct {
	conn net.Conn
	id   uint32
}

func (cw *connWriter) Write(p []byte) (int, error) {
	data := make([]byte, 1+len(p))
	data[0] = byte(cw.id)
	copy(data[1:], p)
	if err := writeFrame(cw.conn, frameWriterData, data); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (d *Daemon) sendErrorResponse(conn net.Conn, errMsg string) {
	resp := response{Error: errMsg}
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(resp)
	writeFrame(conn, frameResponse, buf.Bytes())
}

// ─── Logging ────────────────────────────────────────────────────────────────

// Logger returns the daemon's logger for logging from handlers or goroutines
// spawned during daemon operation. Returns nil if called from outside the
// daemon process (e.g. from the client side).
func Logger() *log.Logger {
	return daemonLogger
}

// LogPath returns the path of the daemon's current log file.
// Returns an empty string when not called from within the daemon process.
func LogPath() string {
	return daemonLogPath
}

// ArchiveLog renames the current log file to a timestamped backup and opens a
// fresh log file in its place. Returns the path of the archived file.
// Only has effect when called from within the daemon process; returns an error
// otherwise.
func ArchiveLog() (string, error) {
	if daemonLogFile == nil || daemonLogPath == "" {
		return "", errors.New("not running as daemon")
	}

	dir := filepath.Dir(daemonLogPath)
	base := filepath.Base(daemonLogPath)
	ext := filepath.Ext(base)
	stem := strings.TrimSuffix(base, ext)

	timestamp := time.Now().Format("20060102-150405")
	archivePath := filepath.Join(dir, fmt.Sprintf("%s.%s.backup%s", stem, timestamp, ext))

	if err := os.Rename(daemonLogPath, archivePath); err != nil {
		return "", fmt.Errorf("failed to archive log: %w", err)
	}

	newFile, err := os.OpenFile(daemonLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return "", fmt.Errorf("failed to open new log file: %w", err)
	}

	daemonLogger.SetOutput(newFile)
	old := daemonLogFile
	daemonLogFile = newFile
	old.Close()

	return archivePath, nil
}

// ─── Process management ─────────────────────────────────────────────────────

func (d *Daemon) start(env map[string]string) error {
	if err := os.MkdirAll(d.runtimeDir(), 0700); err != nil {
		return err
	}

	if d.isRunning() {
		return fmt.Errorf("daemon is already running")
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("could not determine executable path: %w", err)
	}

	r, w, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("failed to create startup pipe: %w", err)
	}
	defer r.Close()

	cmd := exec.Command(exe)
	cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s", daemonEnvKey, d.name))
	cmd.Env = append(cmd.Env, fmt.Sprintf("%s=3", daemonReadyFDEnvKey))
	for k, v := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}
	cmd.ExtraFiles = []*os.File{w}
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		w.Close()
		return fmt.Errorf("failed to start daemon process: %w", err)
	}

	cmd.Process.Release()
	w.Close()

	type readResult struct {
		msg string
		err error
	}
	ch := make(chan readResult, 1)
	go func() {
		buf, err := io.ReadAll(r)
		ch <- readResult{strings.TrimSpace(string(buf)), err}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			return fmt.Errorf("failed to read startup result: %w", res.err)
		}
		if res.msg == "ok" {
			return nil
		}
		if res.msg == "" {
			return fmt.Errorf("daemon exited unexpectedly during startup")
		}
		return fmt.Errorf("daemon startup failed: %s", res.msg)
	case <-time.After(30 * time.Second):
		return fmt.Errorf("daemon startup timed out")
	}
}

func (d *Daemon) isRunning() bool {
	conn, err := net.DialTimeout("unix", d.socketPath(), 500*time.Millisecond)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (d *Daemon) stop() error {
	if !d.isRunning() {
		return nil
	}

	data, err := os.ReadFile(d.pidPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("failed to read PID file: %w", err)
	}

	var pid int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); err != nil {
		return fmt.Errorf("failed to parse PID file: %w", err)
	}

	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("failed to find process %d: %w", pid, err)
	}

	if err := proc.Signal(syscall.SIGTERM); err != nil {
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return fmt.Errorf("failed to signal process %d: %w", pid, err)
	}

	if d.conn != nil {
		d.conn.Close()
		d.conn = nil
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := proc.Signal(syscall.Signal(0)); err != nil {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}

	return fmt.Errorf("daemon did not stop within timeout")
}

// ─── Wire protocol helpers ──────────────────────────────────────────────────

// Frame format: [1 byte type][4 bytes big-endian length][payload]

func writeFrame(w io.Writer, ft frameType, data []byte) error {
	header := make([]byte, 5)
	header[0] = byte(ft)
	binary.BigEndian.PutUint32(header[1:], uint32(len(data)))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

func readFrame(r io.Reader) (frameType, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	ft := frameType(header[0])
	length := binary.BigEndian.Uint32(header[1:])
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, nil, err
	}
	return ft, data, nil
}
