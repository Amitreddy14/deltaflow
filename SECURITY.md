# Security

Deltaflow is a portfolio-grade platform that handles a copy of a database, so it should not be
casually exposed. This document states what the production setup
([`docker-compose.prod.yml`](docker-compose.prod.yml), [deployment guide](docs/deployment.md))
protects, and what it does not.

## What is protected

| Surface | Protection |
|---|---|
| Public network | Only Caddy publishes ports (80, 443). Postgres, Kafka, Kafka Connect, Elasticsearch and MongoDB publish nothing. |
| Transport | HTTPS with an automatically issued and renewed certificate; HTTP redirects to HTTPS; HSTS and basic hardening headers; request bodies capped at 1 MB. |
| API | With `DELTAFLOW_AUTH_REQUIRED=true` every route except `/health` needs the key (`X-API-Key` or `Authorization: Bearer`), compared in constant time. Interactive docs and the OpenAPI schema are disabled. |
| Fail closed | The API refuses to start in production mode without a key, and `compose up` refuses to start if any secret is unset. There are no default passwords in the production file. |
| Datastores | Elasticsearch security enabled (password for `elastic`); MongoDB authentication enabled (root user). Credentials are random 192-bit values from `deltaflow gen-env`. |
| Secrets at rest | `.env.prod` is mode 0600 and git-ignored; `gen-env` never overwrites it. Elasticsearch credentials are sent as basic auth, never embedded in URLs that could end up in logs. |
| Containers | The API and sink workers run as a non-root user with a read-only filesystem, all Linux capabilities dropped, and `no-new-privileges`. Memory limits and log rotation apply to every service. |
| Supply chain | CI runs lint, strict typing and tests on every change; the image is built from source in GitHub Actions. |

## What is not protected, honestly

- **Kafka and Kafka Connect use plaintext and no authentication.** Their only defence is that
  nothing outside the Docker network can reach them. Anyone who gets a shell on the VM, or into
  another container, can read every change event and reconfigure the connector. Adding SASL and
  TLS is the next hardening step.
- **Traffic between containers is not encrypted** (it never leaves the host).
- **One shared API key**, no users, roles or audit trail. Anyone with the key can pause, restart
  or re-register the connector. Rotate it by editing `.env.prod` and restarting the API.
- **The data stores run as root-capable images** with default container settings (not read-only,
  capabilities not dropped). That hardening was left out because it is easy to get subtly wrong
  and cannot be verified without running each datastore.
- **No rate limiting or WAF.** The key is long enough that guessing is not practical, but the
  endpoint can still be flooded.
- **No disk encryption or backup encryption** beyond what the cloud provider offers.
- **Not penetration tested.** Treat the setup as sensible defaults, not as an assurance.
- Change events contain whatever is in your tables. Do not point this at data you would not
  want in Elasticsearch and MongoDB, and add column filtering in the connector config for
  sensitive columns.

## Reporting a vulnerability

Please open a private security advisory on the GitHub repository
(**Security**, **Report a vulnerability**) rather than a public issue.
