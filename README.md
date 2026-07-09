# lazysusan

Run shell commands on your own machine from anywhere, using nothing but
`curl`.

No inbound ports, no SSH, no daemon to babysit. Built for agentic LLM
tool calls first, humans second.

- **`susan`** runs on the machine you want to reach. It holds one outbound
  WebSocket to the relay and keeps nothing on disk.
- **`tray`** is the relay. It takes `POST /` and forwards the command to
  the right `susan`.

```
 your machine                 relay                      anywhere
┌────────────┐  1 connect   ┌────────┐  2 POST / with  ┌──────────────┐
│   susan    │ ─────WSS───► │  tray  │ ◄────permit──── │ curl / agent │
│            │ ◄─3 command─ │        │ ───4 output───► │              │
└────────────┘              └────────┘                 └──────────────┘

1. susan connects out once and stays connected
2. a caller POSTs a command with the permit
3. tray forwards it; susan runs it in $SHELL
4. output comes back as the HTTP response
```

The relay is the only thing that listens; neither of your machines needs
an open inbound port.

## Install

On the machine you want to reach:

```sh
curl -fsSL https://lazysusan.sh/get | sh                       # latest
curl -fsSL https://lazysusan.sh/get | SUSAN_VERSION=v1.2.3 sh  # pinned
```

This downloads the right binary, checks its sha256, and puts it in
`~/.local/bin/susan`. Add `~/.local/bin` to your `PATH` if it isn't already.

Or with Go:

```sh
go install github.com/belugashark/lazysusan/cmd/susan@latest   # latest
go install github.com/belugashark/lazysusan/cmd/susan@v1.2.3   # pinned
```

The calling side needs nothing but `curl`, or any HTTP client.

## Run

Start `susan`. It connects to the hosted relay at `lazysusan.sh` by default
and prints a **permit**, the one credential a caller needs:

```sh
$ susan    # or ~/.local/bin/susan if it's not on your PATH
✓ connected

  relay:   https://lazysusan.sh
  permit:  <machine-id>:<call-secret>

  run:     curl https://lazysusan.sh -H 'X-Susan-Permit: <permit>' -d 'uptime'
```

Paste that `run:` line anywhere and the command runs on your machine.

- The body is the command. It runs under `$SHELL -c`, as the user who
  started `susan`, with that user's privileges.
- The response body is the command's output, stdout and stderr combined.
- The status mirrors the exit code: `200` ok, `502` non-zero, `504`
  timeout. Add `--fail-with-body` and the `curl` call itself fails when
  the command failed, so a script can treat it like running the command
  locally.

Good to know:

- Every `susan` run mints a fresh permit. Kill the process and the permit
  dies with it.
- On a network blip `susan` reconnects and keeps the same permit for a
  short grace window, so callers can simply retry.
- One command at a time per machine. Each running `susan` is its own
  machine with its own permit.

Client flags:

- `--relay <url>` or `SUSAN_RELAY`: which relay to use, defaults to the hosted `lazysusan.sh`
- `--relay-key <key>` or `SUSAN_RELAY_KEY`: key for a gated relay
- `--timeout <dur>` or `SUSAN_TIMEOUT`: command timeout ceiling, default `60s`
- `--json`: print the relay, permit, and a ready-made curl as one JSON line; useful for agents

## API

One endpoint does the work:

```
POST /
X-Susan-Permit:  <machine-id>:<call-secret>   required
X-Susan-Timeout: <seconds>                    optional, integer >= 1
body: the command (text, up to 64 KiB)
```

### Request headers

| Header | Meaning | Default |
|---|---|---|
| `X-Susan-Permit` | the `<machine-id>:<call-secret>` printed by `susan` | required |
| `X-Susan-Timeout` | seconds, clamped to the relay's ceiling | the relay's ceiling (`60`) |

### Response headers

| Header | Meaning |
|---|---|
| `X-Susan-Exit` | exit code |
| `X-Susan-Duration-Ms` | how long the command ran |
| `X-Susan-Timed-Out` | `true` if the timeout hit |
| `X-Susan-Truncated` | `true` if output was cut at the response cap (10 MiB default) |
| `X-Susan-Reason` | why it failed: `command_failed`, `command_timeout`, `deadline_exceeded`, `wsclient_disconnected`, `protocol_error`, or `reconnecting` |
| `X-Susan-Retry-After-Ms` | wait this long before retrying (409, 429, retryable 503) |
| `X-Susan-Request-Id` | on every response |

### Status codes

| Status | Meaning |
|---|---|
| 200 | ran, exit 0 |
| 400 | bad `X-Susan-Timeout`, or body over the cap |
| 401 / 404 | bad permit / unknown machine |
| 409 | machine busy with another command, retry |
| 429 | rate limited, retry |
| 502 | non-zero exit, or the client dropped mid-command |
| 503 | relay draining, or client reconnecting (retry with the same permit) |
| 504 | timeout |

### Other endpoints

| Endpoint | Serves |
|---|---|
| `GET /` | plain-text landing page |
| `GET /get` | the installer script |
| `GET /healthz` | `200` when healthy, `503` while draining |
| `GET /stats` | JSON counters; needs the relay key as a bearer token if one is set |

## Self-hosting

The hosted relay is open: any `susan` may register on it. Run your own
`tray` to be your own operator:

```sh
docker build -f Dockerfile.tray -t tray .
docker run --rm -p 8080:8080 -e SUSAN_PUBLIC_URL=https://relay.example.com tray
```

Point clients at it with a connect string:

```sh
susan susan://relay.example.com               # open relay
susan susan://<relay-key>@relay.example.com   # gated relay
```

| Env | Default | What it does |
|---|---|---|
| `SUSAN_PUBLIC_URL` | required | public URL; `https://` unless `SUSAN_INSECURE=1` |
| `PORT` | `8080` | listen port |
| `SUSAN_RELAY_KEY` | unset | set to gate registration and `/stats`; unset = open relay |
| `SUSAN_MAX_TIMEOUT_SECONDS` | `60` | per-command timeout ceiling (hard cap 290) |
| `SUSAN_MAX_RESPONSE_BYTES` | 10 MiB | output cap; `0` = unbounded |
| `SUSAN_MAX_CMD_BYTES` | 64 KiB | command body cap |
| `SUSAN_RATE_LIMIT_PER_MIN` | `60` | per machine; `0` disables |
| `SUSAN_TRUST_XFF` | off | trust `X-Forwarded-For` from your proxy |
| `SUSAN_LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |

`tray --help` lists the rest (per-IP and global abuse budgets).

Gotchas:

- **Run one replica.** The machine map lives in memory. A redeploy drops
  every client; they reconnect on their own but come back with fresh
  permits.
- `tray` speaks plain HTTP. Put a TLS proxy in front (Caddy, nginx) that
  allows WebSocket upgrades on `/ws` and keeps responses open for at least
  `SUSAN_MAX_TIMEOUT_SECONDS + 5` seconds.
- With `SUSAN_TRUST_XFF=1` the proxy must **replace** `X-Forwarded-For`,
  not append to it, or callers can spoof their IP.

## Contributing

```sh
go build ./...
go test ./...   # unit + e2e, ~15 s
```

The e2e suite builds real `susan` and `tray` binaries and covers auth
failures, rate limits, timeouts, reconnects, floods, truncation, and
drain. To hack on it locally without TLS:

```sh
SUSAN_PUBLIC_URL=http://localhost:8080 SUSAN_INSECURE=1 go run ./cmd/tray
go run ./cmd/susan susan+insecure://localhost:8080
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the workflow.

## Out of scope

- Streaming output. You get everything when the command finishes.
- Concurrent commands per machine. One at a time.
- Persistent identity. A new process is a clean slate.
- File uploads, stdin, TTY commands.
- Browser callers (no CORS).
- Telemetry of any kind.

## Trust model

**The machine owner trusts the relay operator completely.** Once a `susan`
registers, the relay can run any command on that box. Self-host and you
trust yourself; use a hosted relay and you trust its operator. That is the
nature of reverse tunnels, not a bug.

**The relay does not trust clients.** Frame-size caps, flood limits, idle
deadlines, and pre-register slot caps apply before anything a client sends
touches relay state.

Found a vulnerability? See [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
