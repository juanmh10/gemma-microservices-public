package analytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/juanmh10/gemma-microservices/worker-b/internal/telemetry"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
	bq "google.golang.org/api/bigquery/v2"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

// Field matches the checked-in Terraform table schemas and staged SQL order.
type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Mode string `json:"mode"`
}

var RunFields = []Field{{"analysis_id", "STRING", "REQUIRED"}, {"source_run_id", "STRING", "REQUIRED"}, {"created_at", "TIMESTAMP", "REQUIRED"}, {"schema_version", "STRING", "REQUIRED"}, {"report_sha256", "STRING", "REQUIRED"}, {"report_uri", "STRING", "REQUIRED"}, {"quality_status", "STRING", "REQUIRED"}, {"records_expected", "INTEGER", "REQUIRED"}, {"records_valid", "INTEGER", "REQUIRED"}, {"records_invalid", "INTEGER", "REQUIRED"}, {"correct_outcomes", "INTEGER", "NULLABLE"}, {"schema_valid_rate", "FLOAT", "REQUIRED"}, {"outcome_accuracy", "FLOAT", "NULLABLE"}, {"cause_f1", "FLOAT", "NULLABLE"}, {"model", "STRING", "REQUIRED"}, {"model_mode", "STRING", "REQUIRED"}, {"model_location", "STRING", "REQUIRED"}, {"model_version", "STRING", "REQUIRED"}, {"input_tokens", "INTEGER", "REQUIRED"}, {"output_tokens", "INTEGER", "REQUIRED"}, {"provenance", "JSON", "REQUIRED"}}
var ReportFields = []Field{{"analysis_id", "STRING", "REQUIRED"}, {"created_at", "TIMESTAMP", "REQUIRED"}, {"schema_version", "STRING", "REQUIRED"}, {"report_sha256", "STRING", "REQUIRED"}, {"report_uri", "STRING", "REQUIRED"}, {"summary", "STRING", "REQUIRED"}, {"findings", "JSON", "REQUIRED"}, {"recommendations", "STRING", "REPEATED"}, {"limitations", "STRING", "REPEATED"}, {"per_cause", "JSON", "REQUIRED"}}

func table(name string) string { return "`" + analysis.Project + "." + Dataset + "." + name + "`" }
func stage(fields []Field, param string) string {
	items := []string{}
	for _, f := range fields {
		expr := fmt.Sprintf("JSON_VALUE(@%s, '$.%s')", param, f.Name)
		if f.Mode == "REPEATED" {
			expr = fmt.Sprintf("ARRAY(SELECT JSON_VALUE(v) FROM UNNEST(JSON_QUERY_ARRAY(@%s, '$.%s')) v)", param, f.Name)
		} else if f.Type == "JSON" {
			expr = fmt.Sprintf("PARSE_JSON(JSON_QUERY(@%s, '$.%s'))", param, f.Name)
		} else {
			typ := f.Type
			if typ == "INTEGER" {
				typ = "INT64"
			}
			if typ == "FLOAT" {
				typ = "FLOAT64"
			}
			expr = "CAST(" + expr + " AS " + typ + ")"
		}
		items = append(items, expr+" AS "+f.Name)
	}
	return "SELECT " + strings.Join(items, ",\n")
}
func IndexSQL() string {
	return "BEGIN TRANSACTION;\nCREATE TEMP TABLE stage_runs AS " + stage(RunFields, "run") + ";\nCREATE TEMP TABLE stage_reports AS " + stage(ReportFields, "report") + ";\n" +
		"ASSERT NOT EXISTS(SELECT 1 FROM " + table("analysis_runs") + " WHERE analysis_id=@id AND report_sha256<>@hash) AS 'immutable analysis conflict';\n" +
		"ASSERT NOT EXISTS(SELECT 1 FROM " + table("analysis_reports") + " WHERE analysis_id=@id AND report_sha256<>@hash) AS 'immutable report conflict';\n" +
		"MERGE " + table("analysis_runs") + " T USING stage_runs S ON T.analysis_id=S.analysis_id WHEN NOT MATCHED THEN INSERT ROW;\n" +
		"MERGE " + table("analysis_reports") + " T USING stage_reports S ON T.analysis_id=S.analysis_id WHEN NOT MATCHED THEN INSERT ROW;\nCOMMIT TRANSACTION;"
}
func parameter(name, value string) *bq.QueryParameter {
	v := &bq.QueryParameterValue{Value: value}
	if value == "" {
		v.ForceSendFields = []string{"Value"}
	}
	return &bq.QueryParameter{Name: name, ParameterType: &bq.QueryParameterType{Type: "STRING"}, ParameterValue: v}
}
func configuration(sql string, params []*bq.QueryParameter) *bq.JobConfiguration {
	return &bq.JobConfiguration{JobTimeoutMs: 120000, Query: &bq.JobConfigurationQuery{Query: sql, UseLegacySql: new(bool), ParameterMode: "NAMED", QueryParameters: params, MaximumBytesBilled: MaxBytesBilled, ForceSendFields: []string{"UseLegacySql"}}}
}

type Warehouse interface {
	Index(context.Context, string, analysis.Report, string) (string, error)
	History(context.Context, Filter) ([]json.RawMessage, error)
}
type BigQuery struct{ Service *bq.Service }

func NewBigQuery(ctx context.Context) (*BigQuery, error) {
	s, err := bq.NewService(ctx, option.WithQuotaProject(analysis.Project))
	return &BigQuery{Service: s}, err
}

// submit never retries insertion. A conflict or ambiguous response is reconciled
// by the exact job reference and configuration, never by generating a new ID.
func (w *BigQuery) submit(ctx context.Context, id string, cfg *bq.JobConfiguration) (result *bq.Job, retErr error) {
	span := telemetry.Begin(ctx, "index", "query")
	defer func() { span.End(retErr != nil) }()
	j := &bq.Job{JobReference: &bq.JobReference{ProjectId: analysis.Project, Location: Region, JobId: id}, Configuration: cfg}
	got, err := w.Service.Jobs.Insert(analysis.Project, j).Context(ctx).Do()
	if err != nil {
		got, err = w.Service.Jobs.Get(analysis.Project, id).Location(Region).Context(ctx).Do()
		if err != nil {
			return nil, errors.New("BigQuery submission unresolved")
		}
		// BigQuery enriches defaults; compare the essential immutable operation.
		q := got.Configuration
		if q == nil || q.Query == nil || q.Query.Query != cfg.Query.Query || q.Query.MaximumBytesBilled != cfg.Query.MaximumBytesBilled || !reflect.DeepEqual(q.Query.QueryParameters, cfg.Query.QueryParameters) {
			return nil, errors.New("BigQuery job configuration conflict")
		}
	}
	for got.Status == nil || got.Status.State != "DONE" {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
		got, err = w.Service.Jobs.Get(analysis.Project, id).Location(Region).Context(ctx).Do()
		if err != nil {
			return nil, errors.New("BigQuery status unavailable")
		}
	}
	if got.Status.ErrorResult != nil {
		return nil, errors.New("BigQuery query failed: " + got.Status.ErrorResult.Reason)
	}
	if got.Statistics != nil && got.Statistics.StartTime > 0 && got.Statistics.EndTime >= got.Statistics.StartTime {
		span.Remote(time.Time{}, time.UnixMilli(got.Statistics.StartTime), time.UnixMilli(got.Statistics.EndTime))
	}
	return got, nil
}
func (w *BigQuery) Index(ctx context.Context, attempt string, r analysis.Report, hash string) (string, error) {
	if !IDValid(attempt) {
		return "", ErrInvalid
	}
	run, report := Project(r, hash)
	a, _ := json.Marshal(run)
	b, _ := json.Marshal(report)
	id := indexJobID(r.AnalysisID, hash, attempt)
	_, err := w.submit(ctx, id, configuration(IndexSQL(), []*bq.QueryParameter{parameter("run", string(a)), parameter("report", string(b)), parameter("id", r.AnalysisID), parameter("hash", hash)}))
	return id, err
}

type Filter struct {
	From, To, Source, Quality, Cursor string
	Limit                             int
}

func (f Filter) Validate() error {
	from, e1 := time.Parse("2006-01-02", f.From)
	to, e2 := time.Parse("2006-01-02", f.To)
	if e1 != nil || e2 != nil || !from.Before(to) || to.Sub(from) > 366*24*time.Hour || f.Limit < 1 || f.Limit > 100 || (f.Source != "" && !IDValid(f.Source)) || (f.Cursor != "" && !IDValid(f.Cursor)) || (f.Quality != "" && f.Quality != "pass" && f.Quality != "fail" && f.Quality != "not_evaluated") {
		return ErrInvalid
	}
	return nil
}
func (w *BigQuery) History(ctx context.Context, f Filter) ([]json.RawMessage, error) {
	if f.Validate() != nil {
		return nil, ErrInvalid
	}
	sql := "SELECT TO_JSON_STRING(t) FROM " + table("analysis_runs") + " t WHERE created_at>=TIMESTAMP(@from) AND created_at<TIMESTAMP(@to) AND (@source='' OR source_run_id=@source) AND (@quality='' OR quality_status=@quality) AND analysis_id>@cursor ORDER BY analysis_id LIMIT @limit"
	params := []*bq.QueryParameter{parameter("from", f.From), parameter("to", f.To), parameter("source", f.Source), parameter("quality", f.Quality), parameter("cursor", f.Cursor), parameter("limit", strconv.Itoa(f.Limit))}
	params[5].ParameterType.Type = "INT64"
	id := "history_" + analysis.Digest([]byte(strconv.FormatInt(time.Now().UnixNano(), 10)+fmt.Sprint(f)))
	cfg := configuration(sql, params)
	cfg.JobTimeoutMs = 15000
	_, err := w.submit(ctx, id, cfg)
	if err != nil {
		return nil, err
	}
	got, err := w.Service.Jobs.GetQueryResults(analysis.Project, id).Location(Region).MaxResults(int64(f.Limit)).Context(ctx).Do()
	if err != nil {
		return nil, errors.New("BigQuery results unavailable")
	}
	rows := []json.RawMessage{}
	for _, row := range got.Rows {
		if len(row.F) != 1 {
			return nil, ErrInvalid
		}
		s, ok := row.F[0].V.(string)
		if !ok || !json.Valid([]byte(s)) {
			return nil, ErrInvalid
		}
		rows = append(rows, json.RawMessage(s))
	}
	if got.PageToken != "" {
		return nil, errors.New("unexpected result page")
	}
	return rows, nil
}

// NotFound distinguishes absence during recovery; it does not authorize resubmit.
func NotFound(err error) bool { var e *googleapi.Error; return errors.As(err, &e) && e.Code == 404 }
