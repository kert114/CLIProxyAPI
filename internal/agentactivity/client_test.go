package agentactivity

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSnapshotURL(t *testing.T) {
	for _, tc := range []struct {
		origin string
		want   string
	}{
		{origin: "", want: "http://127.0.0.1:8765/api/snapshot"},
		{origin: "http://localhost:9876/", want: "http://127.0.0.1:9876/api/snapshot"},
		{origin: "http://[::1]:8765", want: "http://[::1]:8765/api/snapshot"},
		{origin: "http://192.0.2.1:8765"},
		{origin: "http://dashboard.example.com:8765"},
		{origin: "https://127.0.0.1:8765"},
		{origin: "http://user@127.0.0.1:8765"},
		{origin: "http://127.0.0.1:8765/other"},
		{origin: "http://127.0.0.1:8765/?x=1"},
		{origin: "http://127.0.0.1:65536"},
	} {
		t.Run(tc.origin, func(t *testing.T) {
			got, err := snapshotURL(tc.origin)
			if got != tc.want || (err == nil) != (tc.want != "") {
				t.Fatalf("got %q, %v, want %q", got, err, tc.want)
			}
		})
	}
}

func TestFetchRejectsInvalidDashboardResponses(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply http.HandlerFunc
	}{
		{"redirect", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/api/snapshot" {
				t.Error("followed a dashboard redirect")
				_, _ = io.WriteString(w, `{"hosts":[]}`)
				return
			}
			http.Redirect(w, r, "/unexpected", http.StatusFound)
		}},
		{"invalid JSON", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"hosts":[`)
		}},
		{"missing hosts", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = io.WriteString(w, `{"status":"ok"}`)
		}},
		{"oversize", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprintf(w, `{"hosts":[],"padding":%q}`, strings.Repeat("a", maxSnapshotBytes))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(tc.reply)
			defer server.Close()
			if _, err := Fetch(context.Background(), server.URL); err == nil {
				t.Fatal("expected fetch failure")
			}
		})
	}
}
