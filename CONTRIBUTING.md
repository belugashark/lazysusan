# Contributing

Thanks for considering a contribution to lazysusan.

## Before you open a PR

1. Read the README for the trust model and the "out of scope" list. The design
   is intentionally narrow — suggestions to add streaming, persistent
   identity, etc. will be closed without action.
2. Run `go test ./...` and confirm everything passes locally.
3. New behavior needs a test in either `internal/<pkg>/<pkg>_test.go` or
   `e2e/*_test.go`.

## House style

- Match the existing tone in code comments: explain *why* if non-obvious,
  don't restate what the code does.
- No `time.Sleep` in unit tests under `internal/` — use `clock.Fake`. E2E tests
  that coordinate real subprocesses may sleep where there's no other signal.
- Keep public API surface small. If you find yourself exporting many types
  from `internal/`, reconsider the design.
- Run `gofmt -s` before committing.

## Reporting bugs

Open an issue describing:

- What you ran (relay version, client version, env vars).
- What you expected.
- What happened. Include the relay's structured log line for the affected
  `X-Susan-Request-Id` if you have it.
