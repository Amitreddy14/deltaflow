# ADR-0013: Production hardening and deployment shape

- Status: Accepted
- Date: 2026-11-07

## Context

The development stack optimises for convenience: published ports, no authentication, default
passwords. A public deployment needs the opposite, on a budget of one free VM and no operations
team.

## Decision

1. **One public door.** A Caddy reverse proxy is the only service with published ports. It gets and renews a Let's Encrypt certificate by itself, which removes certificate handling from the operator's job. Every datastore, Kafka and Connect stay on the private Docker network.
2. **Authenticate what can be authenticated cheaply.** Elasticsearch security and MongoDB auth are switched on with generated credentials. The API gets `DELTAFLOW_AUTH_REQUIRED`: when set, every route except `/health` needs the key, `/docs` and the schema are disabled, and the process **refuses to start** without a key. Fail closed means a forgotten variable produces an outage, not an open API. Bearer tokens are accepted alongside `X-API-Key` so Prometheus can scrape `/metrics` with its native credentials file.
3. **No default secrets.** The production compose file uses `${VAR:?}` for every secret, and `deltaflow gen-env` creates them (192-bit hex, URL-safe, file mode 0600, never overwritten). Hex avoids the escaping bugs that arbitrary characters cause in MongoDB URLs.
4. **One place builds clients.** `deltaflow.clients` constructs Elasticsearch and MongoDB clients so authentication is applied uniformly (basic auth for Elasticsearch, not credentials in a URL).
5. **Images are built once, by CI.** A tag `v*` publishes a multi-arch image (amd64 and arm64, because the free Oracle VM is ARM) to GHCR. The server pulls; it never builds.
6. **Bound the disk.** Postgres `max_slot_wal_keep_size=5GB` stops a stopped connector from filling the disk with WAL, and Kafka retention is capped at 3 days or 2 GiB per partition. Both trade recoverability for survival; the recovery procedure is in the deployment guide.
7. **Harden what can be verified.** App containers (API, sinks, Caddy) run read-only with all capabilities dropped and `no-new-privileges`. Datastore containers only get `no-new-privileges`, memory limits and log rotation: read-only roots and dropped capabilities on stateful images fail in ways that cannot be checked without running them.

## Consequences

- Kafka and Connect remain plaintext and unauthenticated; the network boundary is their only protection. This is documented in `SECURITY.md` rather than hidden. SASL/TLS is the next step if the broker ever leaves the private network.
- A single shared API key, no per-user identity.
- The deployment is single-node: no high availability, and a VM loss means restoring Postgres from backup and rebuilding the derived stores.
- The files are validated in CI but have not been exercised on a real cloud VM; the guide says so, and the first deployment is expected to correct it.
