package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

// API relies on Cloud Run's mandatory IAM invoker check in remote deployments.
// Local mode binds loopback only. It accepts no caller-supplied object URLs or SQL.
type API struct {
	Store               analysis.Store
	Warehouse           Warehouse
	Prefix, IndexPrefix string
	Now                 func() time.Time
	NotificationStatus  func(context.Context, string, string) map[string]string
}

func (a *API) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	write := func(status int, v any) { w.WriteHeader(status); _ = json.NewEncoder(w).Encode(v) }
	if req.Method != "GET" {
		w.Header().Set("Allow", "GET")
		write(405, map[string]string{"error": "method_not_allowed"})
		return
	}
	// Cloud Run reserves some paths ending in z; retain the old local alias only.
	if req.URL.Path == "/health" || req.URL.Path == "/healthz" {
		write(200, map[string]string{"status": "ok"})
		return
	}
	ctx, cancel := context.WithTimeout(req.Context(), 15*time.Second)
	defer cancel()
	if req.URL.Path == "/analyses" {
		now := time.Now()
		if a.Now != nil {
			now = a.Now()
		}
		q := req.URL.Query()
		for key, values := range q {
			if len(values) != 1 || !strings.Contains("|from|to|source_run_id|quality|cursor|limit|", "|"+key+"|") {
				write(400, map[string]string{"error": "invalid_filter"})
				return
			}
		}
		f := Filter{From: q.Get("from"), To: q.Get("to"), Source: q.Get("source_run_id"), Quality: q.Get("quality"), Cursor: q.Get("cursor"), Limit: 100}
		if f.From == "" {
			f.From = now.AddDate(0, 0, -30).Format("2006-01-02")
		}
		if f.To == "" {
			f.To = now.AddDate(0, 0, 1).Format("2006-01-02")
		}
		if q.Get("limit") != "" {
			n, err := strconv.Atoi(q.Get("limit"))
			if err != nil {
				write(400, map[string]string{"error": "invalid_filter"})
				return
			}
			f.Limit = n
		}
		if f.Validate() != nil {
			write(400, map[string]string{"error": "invalid_filter"})
			return
		}
		rows, err := a.Warehouse.History(ctx, f)
		if err != nil {
			write(503, map[string]string{"error": "history_unavailable"})
			return
		}
		next := ""
		if len(rows) == f.Limit {
			var v struct {
				AnalysisID string `json:"analysis_id"`
			}
			if json.Unmarshal(rows[len(rows)-1], &v) != nil || !IDValid(v.AnalysisID) {
				write(503, map[string]string{"error": "history_unavailable"})
				return
			}
			next = v.AnalysisID
		}
		write(200, map[string]any{"analyses": rows, "next_cursor": next, "from": f.From, "to": f.To})
		return
	}
	parts := strings.Split(strings.TrimPrefix(req.URL.Path, "/"), "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] != "analyses" || !IDValid(parts[1]) || (len(parts) == 3 && parts[2] != "report") || len(req.URL.Query()) != 0 {
		write(404, map[string]string{"error": "not_found"})
		return
	}
	id := parts[1]
	r, hash, err := Load(ctx, a.Store, a.Prefix, id)
	if errors.Is(err, analysis.ErrMissing) {
		// A failed artifact is read separately; pending absence is not reported as success.
		raw, e := a.Store.Read(ctx, a.Prefix+"/"+id+"/_FAILED.json", 1<<20)
		if e == nil {
			var f struct {
				AnalysisID string `json:"analysis_id"`
				Stage      string `json:"stage"`
			}
			if analysis.Decode(raw, &f) == nil && f.AnalysisID == id {
				write(200, map[string]string{"analysis_id": id, "processing_status": "failed", "indexing_status": "not_eligible"})
				return
			}
		}
		write(404, map[string]string{"error": "analysis_not_found"})
		return
	}
	if err != nil {
		write(503, map[string]string{"error": "artifact_unavailable"})
		return
	}
	if len(parts) == 3 {
		write(200, r)
		return
	}
	indexed := "pending"
	raw, err := a.Store.Read(ctx, receiptPath(a.IndexPrefix, id, hash), 1<<20)
	if err == nil {
		var receipt Receipt
		if analysis.Decode(raw, &receipt) != nil || receipt.SchemaVersion != "index-v1" || receipt.AnalysisID != id || receipt.ReportSHA256 != hash || receipt.Status != "indexed" {
			write(503, map[string]string{"error": "index_receipt_invalid"})
			return
		}
		indexed = "indexed"
	} else if !errors.Is(err, analysis.ErrMissing) {
		write(503, map[string]string{"error": "index_status_unavailable"})
		return
	}
	run, _ := Project(r, hash)
	result := map[string]any{"analysis": run, "processing_status": "completed", "indexing_status": indexed}
	if a.NotificationStatus != nil {
		for key, value := range a.NotificationStatus(ctx, id, hash) {
			result[key] = value
		}
	}
	write(200, result)
}
