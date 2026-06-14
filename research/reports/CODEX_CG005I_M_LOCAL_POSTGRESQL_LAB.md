# CG005I-M local PostgreSQL lab

- Phase: `CG005I_M_LOCAL_POSTGRESQL_BEHAVIORAL_LAB`
- Verdict: `BLOCK`
- Baseline: `c54e2933251122375336590a931bdbc1e514718a`
- PostgreSQL: `17.10 (Debian 17.10-1.pgdg13+1)`
- Database: `stock_zero_cg005_lab`
- Supabase contacted: `False`

## Gates

- `cg005i` passed: `True`
- `cg005j` passed: `False`
- `cg005k` passed: `False`
- `cg005l` passed: `False`
- `cg005m` passed: `False`
- `platform_008` passed: `False`

## Blockers

- `snapshot_a_loader_apply_failed`
- Detail: `ProgrammingError:the query has 10 placeholders but 11 parameters were passed`
- Blocked at: `snapshot_a_apply`
- Loader error: `ProgrammingError:the query has 10 placeholders but 11 parameters were passed`

## Safety

- No DSN, password, row payload, customer, store, address or person values are recorded.
- Writes are limited to the dedicated loopback PostgreSQL lab databases.
- Snapshot B was generated under the OS temp directory and is not recorded in the repo.
