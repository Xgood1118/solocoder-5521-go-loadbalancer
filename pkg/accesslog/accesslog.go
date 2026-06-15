package accesslog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/loadbalancer/lb/pkg/logger"
	"github.com/loadbalancer/lb/pkg/types"
)

type Entry struct {
	Timestamp    time.Time `json:"timestamp"`
	ClientIP     string    `json:"client_ip"`
	Method       string    `json:"method"`
	Path         string    `json:"path"`
	Protocol     string    `json:"protocol"`
	StatusCode   int       `json:"status_code"`
	BytesSent    int64     `json:"bytes_sent"`
	LatencyMs    float64   `json:"latency_ms"`
	BackendID    string    `json:"backend_id,omitempty"`
	BackendAddr  string    `json:"backend_addr,omitempty"`
	Referer      string    `json:"referer,omitempty"`
	UserAgent    string    `json:"user_agent,omitempty"`
}

type Writer struct {
	mu         sync.Mutex
	cfg        types.AccessLogConfig
	file       *os.File
	writer     *bufio.Writer
	currentHour string
	queue      chan Entry
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

func NewWriter(cfg types.AccessLogConfig) *Writer {
	w := &Writer{
		cfg:    cfg,
		queue:  make(chan Entry, 10000),
		stopCh: make(chan struct{}),
	}
	if cfg.Enabled {
		w.wg.Add(1)
		go w.run()
	}
	return w
}

func (w *Writer) UpdateConfig(cfg types.AccessLogConfig) {
	w.mu.Lock()
	w.cfg = cfg
	w.mu.Unlock()
}

func (w *Writer) run() {
	defer w.wg.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			for {
				select {
				case e := <-w.queue:
					w.writeEntry(e)
				default:
					w.flushAndClose()
					return
				}
			}
		case e := <-w.queue:
			w.writeEntry(e)
		case <-ticker.C:
			w.mu.Lock()
			if w.writer != nil {
				w.writer.Flush()
			}
			w.mu.Unlock()
			w.rotateAndArchive()
		}
	}
}

func (w *Writer) Log(e Entry) {
	if !w.cfg.Enabled {
		return
	}
	select {
	case w.queue <- e:
	default:
	}
}

func (w *Writer) hourStr(t time.Time) string {
	return t.Format("2006-01-02_15")
}

func (w *Writer) ensureFile(now time.Time) error {
	hour := w.hourStr(now)
	if w.file != nil && w.currentHour == hour {
		return nil
	}
	if w.writer != nil {
		w.writer.Flush()
	}
	if w.file != nil {
		w.file.Close()
	}
	baseDir := filepath.Dir(w.cfg.FilePath)
	if baseDir != "." && baseDir != "" {
		if err := os.MkdirAll(baseDir, 0755); err != nil {
			return err
		}
	}
	ext := filepath.Ext(w.cfg.FilePath)
	nameWithoutExt := strings.TrimSuffix(filepath.Base(w.cfg.FilePath), ext)
	newPath := filepath.Join(baseDir, fmt.Sprintf("%s_%s%s", nameWithoutExt, hour, ext))
	f, err := os.OpenFile(newPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	w.file = f
	w.writer = bufio.NewWriterSize(f, 64*1024)
	w.currentHour = hour
	return nil
}

func (w *Writer) writeEntry(e Entry) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.ensureFile(e.Timestamp); err != nil {
		logger.Log.Error().Err(err).Msg("access log ensure file failed")
		return
	}
	data, err := json.Marshal(e)
	if err != nil {
		return
	}
	data = append(data, '\n')
	if _, err := w.writer.Write(data); err != nil {
		logger.Log.Error().Err(err).Msg("access log write failed")
	}
}

func (w *Writer) flushAndClose() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writer != nil {
		w.writer.Flush()
	}
	if w.file != nil {
		w.file.Close()
		w.file = nil
	}
}

func (w *Writer) rotateAndArchive() {
	w.mu.Lock()
	cfg := w.cfg
	filePath := cfg.FilePath
	maxHours := cfg.MaxHours
	w.mu.Unlock()
	if filePath == "" || maxHours <= 0 {
		return
	}
	baseDir := filepath.Dir(filePath)
	if baseDir == "." || baseDir == "" {
		return
	}
	ext := filepath.Ext(filePath)
	nameWithoutExt := strings.TrimSuffix(filepath.Base(filePath), ext)
	prefix := fmt.Sprintf("%s_", nameWithoutExt)
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return
	}
	var files []os.DirEntry
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), prefix) && strings.HasSuffix(e.Name(), ext) {
			files = append(files, e)
		}
	}
	if len(files) <= maxHours {
		return
	}
	sort.Slice(files, func(i, j int) bool {
		fi, _ := files[i].Info()
		fj, _ := files[j].Info()
		return fi.ModTime().Before(fj.ModTime())
	})
	excess := len(files) - maxHours
	for i := 0; i < excess; i++ {
		fpath := filepath.Join(baseDir, files[i].Name())
		os.Remove(fpath)
	}
}

func (w *Writer) Stop() {
	close(w.stopCh)
	w.wg.Wait()
}

func (w *Writer) BuildEntry(r *http.Request, statusCode int, bytesSent int64, latencyMs float64, backendID, backendAddr string) Entry {
	clientIP := r.RemoteAddr
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		clientIP = xff
	} else if xri := r.Header.Get("X-Real-IP"); xri != "" {
		clientIP = xri
	} else {
		if idx := strings.LastIndex(clientIP, ":"); idx > 0 {
			clientIP = clientIP[:idx]
		}
	}
	return Entry{
		Timestamp:   time.Now(),
		ClientIP:    clientIP,
		Method:      r.Method,
		Path:        r.URL.RequestURI(),
		Protocol:    r.Proto,
		StatusCode:  statusCode,
		BytesSent:   bytesSent,
		LatencyMs:   latencyMs,
		BackendID:   backendID,
		BackendAddr: backendAddr,
		Referer:     r.Referer(),
		UserAgent:   r.UserAgent(),
	}
}

func ParseInt(s string, def int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return def
}
