# Conductor's Flyway migrations (test fixture)

Verbatim copies of Conductor OSS's PostgreSQL migrations
(`postgres-persistence/src/main/resources/db/`), Apache License 2.0
(Copyright Netflix, Inc. and the Conductor authors; see `LICENSE-APACHE-2.0`
here and the repository `NOTICE`). Used only by
`ConductorFlywayMigrationTest`, which runs them through this driver with
Conductor's own Flyway configuration -- the path Conductor takes at boot.

| Path | What it is for |
|---|---|
| `migration_postgres/` | Conductor's schema migrations (V1..V13.1), the default `classpath:db/migration_postgres` location. |
| `migration_postgres_data/` | Conductor's data migration (V13.2), applied by default (`applyDataMigrations=true`). |
| `LICENSE-APACHE-2.0` | The Apache License 2.0 these copied migrations are distributed under. |

Do not edit the SQL to make a test pass: the point is that Conductor's real
migrations apply. If Conductor's migration set changes, re-copy it and update
`CONDUCTOR_MIGRATIONS` in the test.
