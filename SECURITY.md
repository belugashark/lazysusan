# Security policy

## Reporting a vulnerability

lazysusan is, by design, a remote-code-execution surface. Take security
reports seriously.

If you believe you've found a vulnerability, **please do not file a public
issue.** Instead, use the repo's **Report a vulnerability** button (GitHub
private vulnerability reporting) and the maintainer will pick it up.

We aim to:

- Acknowledge the report within a few days.
- Provide an initial assessment within a week or so.
- Coordinate a fix and disclosure date together with the reporter.

## Scope

In-scope:

- Authentication bypass (executing commands without a matching token).
- Privilege escalation past the registered client's shell scope.
- Denial of service that survives a process restart.
- Protocol-level memory exhaustion, frame-injection, or header-injection.
- Cryptographic weaknesses in id or token generation.

Out of scope:

- A malicious relay operator running commands on a connected client. This is
  documented in the README trust model and is structural to reverse-tunneling.
- A malicious `susan` user running anything their shell already permits — the
  command body is passed to `$SHELL -c` by design.
- Denial of service by simply disconnecting the client. The relay can't help
  with that; restart `susan`.

## What we keep on the relay

The hosted relay sees, **in memory only**: the relay key (transient),
the assigned id, the source IP, `client_version`, command durations, exit
codes, and response byte counts. **Command text and output bodies are never
logged.** Response bodies transit relay memory but are released as soon as the
request completes.
