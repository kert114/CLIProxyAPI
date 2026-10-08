# Cursor account usage

This fork can read the allowance and spending for the account and team selected
in Cursor CLI. It also covers usage through apps such as T3 Code that use that
account. It does not attribute spending to individual apps or sessions.

Start CLIProxyAPI with a management key configured, open `/management.html`, sign
in normally, and choose **Quota Management**. The **Cursor account usage** card
loads independently of auth files, including when there are no proxy credentials.
Use its **Refresh Cursor usage** button or either dashboard refresh control to
fetch a new snapshot. There is no automatic polling or separate key prompt.

The native card requires the frontend from
[the management panel fork](https://github.com/kert114/Cli-Proxy-API-Management-Center/tree/feat/native-cursor-quota).
Build it with `bun install --frozen-lockfile` and `bun run build`, then copy
`dist/index.html` to this proxy's `static/management.html` (or the directory set
by `MANAGEMENT_STATIC_PATH`). Set `management.disable-auto-update-panel`
to `true` in the local proxy config so the official panel updater does not replace
the custom build. The generated HTML is a local artifact; frontend source lives
in the linked repository.

The older `/cursor-usage.html` page remains available directly as a fallback.
It asks for the proxy management key and keeps it only in memory.

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
Keep the proxy bound to localhost for desktop use. The native card uses the
dashboard's existing management session and remember-password setting. Usage
snapshots stay in memory and are cleared on logout or a connection change;
pending requests are canceled. Responses are not cached and contain no Cursor
token, email, or team identifier. This is an account monitor, not a proxy auth
file or a Cursor inference provider.

## Upstream dependency

This uses the private DashboardService RPCs used by the installed Cursor CLI:
`GetCurrentPeriodUsage`, `GetHardLimit`, and `GetPlanInfo` at `api2.cursor.sh`.
Redirects are rejected. Cursor may change this interface; this is not the public
Enterprise Admin API. Plan or spending-limit lookup failures produce warnings
while preserving available usage. Failure of the primary usage lookup hides the
previous figures and offers a manual retry.
