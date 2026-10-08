# Cursor account usage

This fork can read the allowance and spending for the account and team selected
in Cursor CLI. It also covers usage through apps such as T3 Code that use that
account. It does not attribute spending to individual apps or sessions.

Start CLIProxyAPI with a management key configured, open `/management.html`, and
choose **Cursor usage**. You can also open `/cursor-usage.html` directly. Enter
the proxy management key and choose **Load usage**. **Refresh** fetches a new
snapshot; there is no automatic polling.

The page shows included usage as a percentage, the reported Auto and other-model
breakdown, personal on-demand spending in USD, the individual spending limit,
plan name, and billing reset time. Missing fields display as **Not reported**.
The included allowance is not shown as a dollar balance because its absolute
units have not been established. Pooled team spending is not your personal
spending.

## Sign-in and access

Use an existing Cursor CLI sign-in. If it is missing or expired, run `agent login`
yourself, then refresh. The monitor does not start the agent, refresh credentials,
write Cursor configuration, or change spending limits.

On macOS the default token comes from Cursor CLI's Keychain entry. With
`AGENT_CLI_CREDENTIAL_STORE=file`, it reads `~/.cursor/auth.json`. Linux uses
`$XDG_CONFIG_HOME/cursor/auth.json` or `~/.config/cursor/auth.json`; Windows uses
`%APPDATA%/Cursor/auth.json`. The `memory` backend cannot be read by another
process. Run the proxy as the same OS user, with the same Cursor environment as
the CLI, to select the same account and team. Containers need access to that
user's credential store; a host Keychain is not available inside a container.

The team selection comes from `cli-config.json` in `$CURSOR_CONFIG_DIR`, then
`$XDG_CONFIG_HOME/cursor`, then `~/.cursor`. An unreadable or invalid existing
config fails the request rather than selecting another team silently.

The endpoint is `GET /v8/management/observability/usage/cursor`. It requires the
normal management key **and a direct loopback connection**, even when remote
management is enabled. Forwarded IP headers do not satisfy this requirement.
Keep the proxy bound to localhost for desktop use. The page keeps the management
key only in memory and clears it on disconnect or reload. Responses are not
cached and contain no Cursor token, email, or team identifier.

## Upstream dependency

This uses the private DashboardService RPCs used by the installed Cursor CLI:
`GetCurrentPeriodUsage`, `GetHardLimit`, and `GetPlanInfo` at `api2.cursor.sh`.
Redirects are rejected. Cursor may change this interface; this is not the public
Enterprise Admin API. Plan or spending-limit lookup failures produce warnings
while preserving available usage. Failure of the primary usage lookup hides the
previous figures and offers a manual retry.
