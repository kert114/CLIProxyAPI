package cursorusage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestFetchUsagePreservesOptionalFieldsAndMoneyUnits(t *testing.T) {
	for _, tc := range []struct {
		name, usage, limit string
		percent, limitUSD  *float64
		kind               string
	}{
		{"explicit zero", `{"planUsage":{"totalPercentUsed":0,"includedSpend":20,"limit":40}}`, `{}`, floatPtr(0), nil, ""},
		{"percentage fallback", `{"planUsage":{"includedSpend":20,"limit":40}}`, `{}`, floatPtr(50), nil, ""},
		{"unknown percentage", `{"planUsage":{}}`, `{}`, nil, nil, ""},
		{"personal dollar limit", `{"planUsage":{},"spendLimitUsage":{"individualUsed":1234,"individualLimit":2500}}`, `{"hardLimit":500}`, nil, floatPtr(500), "fixed"},
		{"team pooled limit", `{"planUsage":{},"spendLimitUsage":{"limitType":"team","pooledLimit":"100000","pooledUsed":99000}}`, `{"hardLimit":500}`, nil, nil, "unlimited"},
		{"team disabled individual", `{"planUsage":{},"spendLimitUsage":{"limitType":"team","individualLimit":0}}`, `{"hardLimit":500}`, nil, nil, "disabled"},
		{"organization disables spending", `{"planUsage":{},"spendLimitUsage":{"limitType":"team","individualLimit":2500}}`, `{"hardLimit":500,"onDemandSpendDisabledByOrganization":true}`, nil, nil, "disabled"},
		{"personal unlimited", `{"planUsage":{},"spendLimitUsage":{}}`, `{"hardLimit":2147483647}`, nil, nil, "unlimited"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := fixtureClient(t, tc.usage, tc.limit, `{}`).Fetch(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if !sameFloat(got.Included.PercentUsed, tc.percent) || got.Included.AutoPercentUsed != nil || got.Included.APIPercentUsed != nil {
				t.Fatalf("unexpected included usage: %+v", got.Included)
			}
			if tc.kind == "" {
				if got.OnDemand != nil {
					t.Fatal("missing spend object became fabricated spending")
				}
			} else if got.OnDemand == nil || got.OnDemand.LimitKind != tc.kind || !sameFloat(got.OnDemand.LimitUSD, tc.limitUSD) {
				t.Fatalf("unexpected individual spending limit: %+v", got.OnDemand)
			}
			if tc.name == "team pooled limit" && got.OnDemand.UsedUSD != 0 {
				t.Fatal("pooled team spend was substituted for personal spend")
			}
		})
	}
}

func floatPtr(value float64) *float64 { return &value }

func sameFloat(a, b *float64) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}

func TestFetchUsageHandlesUnavailableEnrichment(t *testing.T) {
	c := fixtureClient(t, `{"planUsage":{"totalPercentUsed":10},"spendLimitUsage":{"individualLimit":2500}}`, `not JSON secret-value`, `not JSON secret-value`)
	got, err := c.Fetch(context.Background())
	if err != nil || got.OnDemand == nil || !sameFloat(got.OnDemand.LimitUSD, floatPtr(25)) || got.Plan != "Cursor" || len(got.Warnings) != 2 {
		t.Fatalf("optional failures discarded usage: %+v, %v", got, err)
	}
}

func TestFetchUsageRejectsAbsentPlanUsage(t *testing.T) {
	_, err := fixtureClient(t, `{}`, `{}`, `{}`).Fetch(context.Background())
	if !errors.Is(err, ErrNoUsage) {
		t.Fatalf("error = %v, want unavailable plan usage", err)
	}
}

func TestFetchUsageDoesNotForwardTokensOnRedirectOrExposeErrors(t *testing.T) {
	for _, status := range []int{http.StatusFound, http.StatusUnauthorized, http.StatusInternalServerError, http.StatusOK} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := 0
			c := &Client{
				Credentials: func(context.Context) (Credentials, error) { return Credentials{AccessToken: "test-access-token"}, nil },
				HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					calls++
					if r.URL.Host != "api2.cursor.sh" {
						t.Fatal("token forwarded to redirected destination")
					}
					return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("invalid JSON with secret-value")), Header: http.Header{"Location": {"https://api2.cursor.sh/secret-value"}}}, nil
				})},
			}
			_, err := c.Fetch(context.Background())
			want := ErrUnavailable
			if status == http.StatusUnauthorized {
				want = ErrLoginRequired
			}
			if !errors.Is(err, want) || calls != 1 || strings.Contains(err.Error(), "secret-value") {
				t.Fatalf("unsafe failure: calls = %d, error = %v", calls, err)
			}
		})
	}
}

func TestFetchUsageWithoutLoginDoesNotContactCursor(t *testing.T) {
	c := &Client{
		Credentials: func(context.Context) (Credentials, error) { return Credentials{}, ErrLoginRequired },
		HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("unauthenticated request contacted Cursor")
			return nil, nil
		})},
	}
	if _, err := c.Fetch(context.Background()); !errors.Is(err, ErrLoginRequired) {
		t.Fatalf("error = %v, want login required", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func fixtureClient(t *testing.T, usage, limit, plan string) *Client {
	t.Helper()
	return &Client{
		Credentials: func(context.Context) (Credentials, error) {
			return Credentials{AccessToken: "test-access-token", TeamID: 123}, nil
		},
		HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.URL.Scheme != "https" || r.URL.Host != "api2.cursor.sh" {
				t.Fatalf("unexpected destination %s", r.URL)
			}
			if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer test-access-token" || r.Header.Get("X-Cursor-Team-ID") != "123" {
				t.Fatal("missing authenticated Teams RPC request")
			}
			if r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Connect-Protocol-Version") != "1" {
				t.Fatal("missing Connect JSON headers")
			}
			body, err := io.ReadAll(r.Body)
			if err != nil || string(body) != "{}" {
				t.Fatalf("unexpected request body %q", body)
			}
			var response string
			switch r.URL.Path {
			case "/aiserver.v1.DashboardService/GetCurrentPeriodUsage":
				response = usage
			case "/aiserver.v1.DashboardService/GetHardLimit":
				response = limit
			case "/aiserver.v1.DashboardService/GetPlanInfo":
				response = plan
			default:
				t.Fatalf("unexpected RPC %s", r.URL.Path)
			}
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(response)), Header: make(http.Header)}, nil
		})},
	}
}

func TestFetchTeamsUsage(t *testing.T) {
	c := fixtureClient(t,
		`{"billingCycleEnd":"1791417600000","planUsage":{"totalPercentUsed":25,"autoPercentUsed":10,"apiPercentUsed":15},"spendLimitUsage":{"individualUsed":1234,"individualLimit":2500,"limitType":"team"},"accessToken":"never-return-this","email":"private@example.test"}`,
		`{"hardLimit":500}`,
		`{"planInfo":{"planName":"Teams"}}`)
	snapshot, err := c.Fetch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Plan != "Teams" || snapshot.Included.PercentUsed == nil || *snapshot.Included.PercentUsed != 25 || snapshot.OnDemand == nil || snapshot.OnDemand.UsedUSD != 12.34 || snapshot.OnDemand.LimitUSD == nil || *snapshot.OnDemand.LimitUSD != 25 || snapshot.OnDemand.LimitKind != "fixed" {
		t.Fatalf("unexpected snapshot: %+v", snapshot)
	}
	if snapshot.ResetsAt == nil || !snapshot.ResetsAt.Equal(time.Date(2026, 10, 8, 0, 0, 0, 0, time.UTC)) || snapshot.CheckedAt.IsZero() {
		t.Fatalf("unexpected snapshot dates: %+v", snapshot)
	}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "never-return-this") || strings.Contains(string(encoded), "private@example.test") || strings.Contains(string(encoded), "test-access-token") {
		t.Fatal("snapshot leaked provider credentials or identity")
	}
}
