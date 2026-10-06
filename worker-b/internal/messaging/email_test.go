package messaging

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/juanmh10/gemma-microservices/worker-b/internal/analysis"
)

const testDomain = "example.invalid"
const testSender = "sender" + "@" + testDomain
const testRecipient = "recipient" + "@" + testDomain

type emailTransport func(*http.Request) (*http.Response, error)

func (f emailTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestResendSingleRequestRedactionAndClassification(t *testing.T) {
	for _, tc := range []struct {
		code              int
		body              string
		permanent, review bool
	}{
		{200, `{"id":"provider-001"}`, false, false},
		{403, `{"message":"private recipient and credential"}`, true, false},
		{429, `{"message":"private recipient"}`, false, false},
		{503, `private credential`, false, false},
		{409, `{"name":"concurrent_idempotent_requests"}`, false, false},
		{409, `{"name":"invalid_idempotent_request"}`, false, true},
		{200, `{"id":"invalid/private"}`, false, false},
		{302, `redirect`, true, false},
	} {
		t.Run(tc.body, func(t *testing.T) {
			calls := 0
			s, err := NewResend("test-credential")
			if err != nil {
				t.Fatal(err)
			}
			s.Client.Transport = emailTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.String() != "https://api.resend.com/emails" || r.Header.Get("Authorization") != "Bearer test-credential" || r.Header.Get("Idempotency-Key") != "same-key" {
					t.Fatal("request contract")
				}
				return &http.Response{StatusCode: tc.code, Body: io.NopCloser(strings.NewReader(tc.body)), Header: make(http.Header)}, nil
			})
			id, err := s.Send(context.Background(), "same-key", EmailRequest{From: testSender, To: []string{testRecipient}, Subject: "Test", Text: "Aggregate metrics"})
			if calls != 1 {
				t.Fatal("hidden provider retry")
			}
			if tc.code == 200 && strings.Contains(tc.body, "provider-001") {
				if err != nil || id != "provider-001" {
					t.Fatal(err)
				}
				return
			}
			if err == nil || errors.Is(err, ErrEmailPermanent) != tc.permanent || errors.Is(err, ErrEmailReview) != tc.review {
				t.Fatal("classification", err)
			}
			if strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), "credential") {
				t.Fatal("private error leaked")
			}
		})
	}
}

type testEmailSender struct {
	calls     int
	keys      []string
	hashes    []string
	fail      bool
	permanent bool
}

func (s *testEmailSender) Send(_ context.Context, key string, p EmailRequest) (string, error) {
	s.calls++
	s.keys = append(s.keys, key)
	s.hashes = append(s.hashes, analysis.Digest(analysis.JSON(p)))
	if s.permanent {
		return "", ErrEmailPermanent
	}
	if s.fail {
		return "", errors.New("response lost")
	}
	return "provider-001", nil
}

type lostEmailReceipt struct {
	analysis.Store
	fail bool
}

func (s *lostEmailReceipt) Create(ctx context.Context, name string, data []byte) error {
	if strings.HasSuffix(name, "/_ACCEPTED.json") && s.fail {
		s.fail = false
		return errors.New("receipt unavailable")
	}
	return s.Store.Create(ctx, name, data)
}

func TestEmailReceiptRecoveryReplayAndPrivateConfiguration(t *testing.T) {
	x, e, _ := fixture(t)
	sender := &testEmailSender{}
	d, err := NewEmailDelivery(sender, testSender, testRecipient, "example.invalid")
	if err != nil {
		t.Fatal(err)
	}
	x.Email = d
	lost := &lostEmailReceipt{Store: x.Store, fail: true}
	x.Store = lost
	if err = x.Deliver(context.Background(), e); err == nil {
		t.Fatal("receipt failure hidden")
	}
	if got := x.Status(context.Background(), e.AnalysisID, e.ReportSHA256)["email_status"]; got != "pending" {
		t.Fatal(got)
	}
	if err = x.Deliver(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 2 || sender.keys[0] != sender.keys[1] || sender.hashes[0] != sender.hashes[1] {
		t.Fatal("ambiguous recovery changed request")
	}
	if err = x.Deliver(context.Background(), e); err != nil || sender.calls != 2 {
		t.Fatal("accepted email replayed", err)
	}
	if got := x.Status(context.Background(), e.AnalysisID, e.ReportSHA256)["email_status"]; got != "accepted" {
		t.Fatal(got)
	}
	raw, err := x.Store.Read(context.Background(), EmailBase(x.NotificationPrefix, e)+"/_ACCEPTED.json", 8<<10)
	if err != nil || strings.Contains(string(raw), "example.invalid") || strings.Contains(string(raw), "test-credential") {
		t.Fatal("private config in receipt")
	}
	d.To = "another" + "@" + testDomain
	if !errors.Is(x.Deliver(context.Background(), e), ErrEmailReview) || sender.calls != 2 {
		t.Fatal("changed recipient sent without reconciliation")
	}
}

func TestEmailFailureWindowAndPermanentRejection(t *testing.T) {
	x, e, _ := fixture(t)
	sender := &testEmailSender{fail: true}
	d, _ := NewEmailDelivery(sender, testSender, testRecipient, "example.invalid")
	x.Email = d
	now := time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)
	d.Now = func() time.Time { return now }
	if x.Deliver(context.Background(), e) == nil {
		t.Fatal("outage acknowledged")
	}
	now = now.Add(23 * time.Hour)
	if !errors.Is(x.Deliver(context.Background(), e), ErrEmailReview) || sender.calls != 1 {
		t.Fatal("expired idempotency sent")
	}
	x, e, _ = fixture(t)
	sender = &testEmailSender{permanent: true}
	d, _ = NewEmailDelivery(sender, testSender, testRecipient, "example.invalid")
	x.Email = d
	for range 2 {
		if !errors.Is(x.Deliver(context.Background(), e), ErrEmailPermanent) {
			t.Fatal("permanent rejection missing")
		}
	}
	if sender.calls != 1 || x.Status(context.Background(), e.AnalysisID, e.ReportSHA256)["email_status"] != "rejected" {
		t.Fatal("permanent request retried")
	}
}

func TestInvalidEmailConfigurationAndUntrustedEventCannotSend(t *testing.T) {
	sender := &testEmailSender{}
	for _, from := range []string{"example.invalid", "sender" + "@" + "other.invalid", testSender + "\r\nBcc: other" + "@" + testDomain} {
		if _, err := NewEmailDelivery(sender, from, testRecipient, "example.invalid"); err == nil {
			t.Fatal("invalid private sender accepted")
		}
	}
	x, e, _ := fixture(t)
	d, _ := NewEmailDelivery(sender, testSender, testRecipient, "example.invalid")
	x.Email = d
	e.ReportURI = "https://untrusted.invalid/report"
	if !errors.Is(x.Deliver(context.Background(), e), ErrInvalid) || sender.calls != 0 {
		t.Fatal("forged event sent")
	}
}

func TestEmailV2UsesNeutralTextAndExcludesEvidenceAndNarrative(t *testing.T) {
	x, e, r := fixture(t)
	sentinel := "private-source-excerpt-sentinel"
	r.Metrics.Evidence = []analysis.Evidence{{RecordID: "conv_000001", CauseCode: "price", TurnID: 1, Text: sentinel}}
	r.Narrative.Summary = sentinel
	sender := &testEmailSender{}
	d, _ := NewEmailDelivery(sender, testSender, testRecipient, testDomain)
	payload := d.payload(e, r, EmailVersion)
	if strings.Contains(payload.Subject+payload.Text, "POC") || strings.Contains(payload.Text, sentinel) || strings.Contains(payload.Text, "synthetic") {
		t.Fatal("new email contains legacy wording or source/model text")
	}
	if err := d.Deliver(context.Background(), x.Store, x.NotificationPrefix, e, r); err != nil {
		t.Fatal(err)
	}
	raw, err := x.Store.Read(context.Background(), EmailBase(x.NotificationPrefix, e)+"/_ACCEPTED.json", 8<<10)
	if err != nil || !SchemaValid("email-receipt", raw) || !strings.Contains(string(raw), EmailVersion) || len(sender.keys) != 1 || sender.keys[0] != "pipeline-email/"+e.EventID+"/"+EmailVersion {
		t.Fatal("new template receipt/key not versioned")
	}
}

func TestLegacyAcceptedAndPendingEmailRetainPayloadAndKey(t *testing.T) {
	for _, status := range []string{"accepted", "pending"} {
		t.Run(status, func(t *testing.T) {
			x, e, r := fixture(t)
			sender := &testEmailSender{}
			d, _ := NewEmailDelivery(sender, testSender, testRecipient, testDomain)
			now := time.Now().UTC()
			d.Now = func() time.Time { return now }
			payload := d.payload(e, r, LegacyEmailVersion)
			if payload.Subject != "Pipeline POC: "+e.AnalysisID || !strings.Contains(payload.Text, "This POC uses a retained synthetic shard.") {
				t.Fatal("legacy payload changed")
			}
			receipt := EmailReceipt{SchemaVersion: LegacyEmailVersion, EventID: e.EventID, AnalysisID: e.AnalysisID, PayloadSHA256: analysis.Digest(analysis.JSON(payload)), StartedAt: now.Format(time.RFC3339Nano), Status: status}
			file := "_INTENT.json"
			if status == "accepted" {
				file = "_ACCEPTED.json"
				receipt.ProviderID = "legacy-provider"
			}
			if err := x.Store.Create(context.Background(), EmailBase(x.NotificationPrefix, e)+"/"+file, analysis.JSON(receipt)); err != nil {
				t.Fatal(err)
			}
			if err := d.Deliver(context.Background(), x.Store, x.NotificationPrefix, e, r); err != nil {
				t.Fatal(err)
			}
			if err := d.Deliver(context.Background(), x.Store, x.NotificationPrefix, e, r); err != nil {
				t.Fatal(err)
			}
			if status == "accepted" && sender.calls != 0 {
				t.Fatal("legacy accepted email resent")
			}
			if status == "pending" && (sender.calls != 1 || sender.keys[0] != "poc-email/"+e.EventID+"/"+LegacyEmailVersion || sender.hashes[0] != receipt.PayloadSHA256) {
				t.Fatal("legacy recovery changed request or identity")
			}
			x.Email = d
			if x.Status(context.Background(), e.AnalysisID, e.ReportSHA256)["email_status"] != "accepted" {
				t.Fatal("legacy status no longer readable")
			}
		})
	}
}

type legacyClaimRace struct {
	analysis.Store
	receipt  EmailReceipt
	injected bool
}

func (s *legacyClaimRace) Create(ctx context.Context, name string, raw []byte) error {
	if strings.HasSuffix(name, "/_INTENT.json") && !s.injected {
		s.injected = true
		if err := s.Store.Create(ctx, name, analysis.JSON(s.receipt)); err != nil {
			return err
		}
		return analysis.ErrExists
	}
	return s.Store.Create(ctx, name, raw)
}
func TestTemplateClaimRaceCannotCreateIndependentSend(t *testing.T) {
	x, e, r := fixture(t)
	sender := &testEmailSender{}
	d, _ := NewEmailDelivery(sender, testSender, testRecipient, testDomain)
	now := time.Now().UTC()
	d.Now = func() time.Time { return now }
	payload := d.payload(e, r, LegacyEmailVersion)
	s := &legacyClaimRace{Store: x.Store, receipt: EmailReceipt{SchemaVersion: LegacyEmailVersion, EventID: e.EventID, AnalysisID: e.AnalysisID, PayloadSHA256: analysis.Digest(analysis.JSON(payload)), StartedAt: now.Format(time.RFC3339Nano), Status: "pending"}}
	if err := d.Deliver(context.Background(), s, x.NotificationPrefix, e, r); !errors.Is(err, ErrEmailReview) || sender.calls != 0 {
		t.Fatal("template race sent a second independent request")
	}
	if err := d.Deliver(context.Background(), s, x.NotificationPrefix, e, r); err != nil || sender.calls != 1 || sender.hashes[0] != s.receipt.PayloadSHA256 {
		t.Fatal("legacy claim recovery changed payload")
	}
}

func TestLegacyEmailPayloadRemainsByteCompatible(t *testing.T) {
	_, e, r := fixture(t)
	d, _ := NewEmailDelivery(&testEmailSender{}, testSender, testRecipient, testDomain)
	prefix := "Analysis test-analysis completed: quality not_evaluated; 297/300 valid records.\nInvalid records: 3.\nQuality reasons: ground_truth_unavailable\nReference: test-analysis. "
	for _, tc := range []struct{ schema, suffix string }{{analysis.SchemaVersion, "This POC uses a retained synthetic shard.\n"}, {analysis.BatchSchemaVersion, "This POC uses a fresh synthetic batch.\n"}} {
		r.SchemaVersion = tc.schema
		got := d.payload(e, r, LegacyEmailVersion)
		want := EmailRequest{From: testSender, To: []string{testRecipient}, Subject: "Pipeline POC: test-analysis", Text: prefix + tc.suffix}
		if string(analysis.JSON(got)) != string(analysis.JSON(want)) {
			t.Fatal("legacy provider payload bytes changed")
		}
	}
}
