package cursorusage

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

var (
	ErrLoginRequired = errors.New("sign in to Cursor CLI with agent login")
	ErrUnavailable   = errors.New("Cursor usage could not be fetched")
	ErrNoUsage       = errors.New("Cursor did not report included usage for this plan")
)

type Credentials struct {
	AccessToken string `json:"-"`
	TeamID      int    `json:"-"`
}

type Snapshot struct {
	Plan      string         `json:"plan"`
	CheckedAt time.Time      `json:"checked_at"`
	ResetsAt  *time.Time     `json:"resets_at,omitempty"`
	Included  IncludedUsage  `json:"included"`
	OnDemand  *OnDemandUsage `json:"on_demand,omitempty"`
	Warnings  []string       `json:"warnings,omitempty"`
}

type IncludedUsage struct {
	PercentUsed     *float64 `json:"percent_used,omitempty"`
	AutoPercentUsed *float64 `json:"auto_percent_used,omitempty"`
	APIPercentUsed  *float64 `json:"api_percent_used,omitempty"`
}

type OnDemandUsage struct {
	UsedUSD   float64  `json:"used_usd"`
	LimitUSD  *float64 `json:"limit_usd,omitempty"`
	LimitKind string   `json:"limit_kind"`
}

type Client struct {
	HTTPClient  *http.Client
	Credentials func(context.Context) (Credentials, error)
}

func NewClient() *Client {
	return &Client{HTTPClient: &http.Client{}, Credentials: LoadCLICredentials}
}

type periodUsage struct {
	BillingCycleEnd json.Number `json:"billingCycleEnd"`
	PlanUsage       *struct {
		IncludedSpend    float64  `json:"includedSpend"`
		Limit            float64  `json:"limit"`
		TotalPercentUsed *float64 `json:"totalPercentUsed"`
		AutoPercentUsed  *float64 `json:"autoPercentUsed"`
		APIPercentUsed   *float64 `json:"apiPercentUsed"`
	} `json:"planUsage"`
	SpendLimitUsage *struct {
		IndividualUsed  float64     `json:"individualUsed"`
		IndividualLimit *float64    `json:"individualLimit"`
		PooledLimit     json.Number `json:"pooledLimit"`
		LimitType       string      `json:"limitType"`
	} `json:"spendLimitUsage"`
}

type hardLimit struct {
	HardLimit                           float64 `json:"hardLimit"`
	NoUsageBasedAllowed                 bool    `json:"noUsageBasedAllowed"`
	OnDemandSpendDisabledByOrganization bool    `json:"onDemandSpendDisabledByOrganization"`
}

func (c *Client) Fetch(ctx context.Context) (Snapshot, error) {
	credentials, err := c.Credentials(ctx)
	if err != nil {
		if errors.Is(err, ErrLoginRequired) {
			return Snapshot{}, ErrLoginRequired
		}
		return Snapshot{}, ErrUnavailable
	}
	if strings.TrimSpace(credentials.AccessToken) == "" {
		return Snapshot{}, ErrLoginRequired
	}
	var usage periodUsage
	if err := c.call(ctx, credentials, "GetCurrentPeriodUsage", &usage); err != nil {
		return Snapshot{}, err
	}
	if usage.PlanUsage == nil {
		return Snapshot{}, ErrNoUsage
	}
	var limit hardLimit
	errLimit := c.call(ctx, credentials, "GetHardLimit", &limit)
	var plan struct {
		PlanInfo *struct {
			PlanName        string      `json:"planName"`
			BillingCycleEnd json.Number `json:"billingCycleEnd"`
		} `json:"planInfo"`
	}
	errPlan := c.call(ctx, credentials, "GetPlanInfo", &plan)
	result := Snapshot{Plan: "Cursor", CheckedAt: time.Now().UTC(), Included: IncludedUsage{
		PercentUsed: usage.PlanUsage.TotalPercentUsed, AutoPercentUsed: usage.PlanUsage.AutoPercentUsed, APIPercentUsed: usage.PlanUsage.APIPercentUsed,
	}}
	if result.Included.PercentUsed == nil && usage.PlanUsage.Limit > 0 {
		percent := usage.PlanUsage.IncludedSpend / usage.PlanUsage.Limit * 100
		result.Included.PercentUsed = &percent
	}
	reset, _ := usage.BillingCycleEnd.Int64()
	if errPlan == nil && plan.PlanInfo != nil {
		if plan.PlanInfo.PlanName != "" {
			result.Plan = plan.PlanInfo.PlanName
		}
		if reset <= 0 {
			reset, _ = plan.PlanInfo.BillingCycleEnd.Int64()
		}
	}
	if reset > 0 {
		at := time.UnixMilli(reset).UTC()
		result.ResetsAt = &at
	}
	if errPlan != nil {
		result.Warnings = append(result.Warnings, "Plan details could not be fetched")
	}
	if errLimit != nil {
		result.Warnings = append(result.Warnings, "Spending limit could not be fetched")
	}
	if spend := usage.SpendLimitUsage; spend != nil {
		result.OnDemand = &OnDemandUsage{UsedUSD: spend.IndividualUsed / 100, LimitKind: "unavailable"}
		setLimit := func(amount float64) {
			result.OnDemand.LimitKind = "disabled"
			if amount > 0 {
				result.OnDemand.LimitKind = "fixed"
				result.OnDemand.LimitUSD = &amount
			}
		}
		switch {
		case errLimit == nil && limit.OnDemandSpendDisabledByOrganization:
			result.OnDemand.LimitKind = "disabled"
		case spend.LimitType == "team" && spend.IndividualLimit != nil:
			setLimit(*spend.IndividualLimit / 100)
		case spend.LimitType == "team" && errLimit == nil:
			result.OnDemand.LimitKind = "disabled"
			if !limit.NoUsageBasedAllowed && limit.HardLimit > 0 {
				result.OnDemand.LimitKind = "unlimited"
			}
		case spend.LimitType == "team":
			pooledLimit, _ := spend.PooledLimit.Int64()
			if pooledLimit > 0 {
				result.OnDemand.LimitKind = "unlimited"
			}
		case errLimit == nil && limit.NoUsageBasedAllowed:
			result.OnDemand.LimitKind = "disabled"
		case errLimit == nil && limit.HardLimit >= 2147483647:
			result.OnDemand.LimitKind = "unlimited"
		case errLimit == nil:
			setLimit(limit.HardLimit)
		case spend.IndividualLimit != nil:
			setLimit(*spend.IndividualLimit / 100)
		}
	}
	return result, nil
}

func (c *Client) call(ctx context.Context, credentials Credentials, method string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api2.cursor.sh/aiserver.v1.DashboardService/"+method, strings.NewReader("{}"))
	if err != nil {
		return ErrUnavailable
	}
	req.Header.Set("Authorization", "Bearer "+credentials.AccessToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connect-Protocol-Version", "1")
	req.Header.Set("X-Cursor-Client-Type", "cli")
	req.Header.Set("X-Cursor-Client-Version", "extension-unknown")
	req.Header.Set("X-Ghost-Mode", "true")
	if credentials.TeamID > 0 {
		req.Header.Set("X-Cursor-Team-ID", strconv.Itoa(credentials.TeamID))
	}
	client := *c.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		return ErrUnavailable
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Debug("failed to close Cursor usage response")
		}
	}()
	if resp.StatusCode == http.StatusUnauthorized {
		return ErrLoginRequired
	}
	if resp.StatusCode != http.StatusOK {
		return ErrUnavailable
	}
	const maxResponseSize = 1024 * 1024
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize+1))
	if err != nil || len(body) > maxResponseSize || json.Unmarshal(body, out) != nil {
		return ErrUnavailable
	}
	return nil
}
