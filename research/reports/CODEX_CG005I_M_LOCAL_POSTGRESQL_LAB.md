# CG005I-M local PostgreSQL lab

- Phase: `CG005I_M_LOCAL_POSTGRESQL_BEHAVIORAL_LAB`
- Verdict: `LOCAL_LAB_VALIDATED`
- Baseline: `c54e2933251122375336590a931bdbc1e514718a`
- PostgreSQL: `17.10 (Debian 17.10-1.pgdg13+1)`
- Database: `stock_zero_cg005_lab`
- Supabase contacted: `False`

## Gates

- `cg005i` passed: `True`
- `cg005j` passed: `True`
- `cg005k` passed: `True`
- `cg005l` passed: `True`
- `cg005m` passed: `True`
- `platform_008` passed: `True`

## Safety

- No DSN, password, row payload, customer, store, address or person values are recorded.
- Writes are limited to the dedicated loopback PostgreSQL lab databases.
- Snapshot B was generated under the OS temp directory and is not recorded in the repo.

## Attempt history

- Attempt 1: `BLOCKED` - Assignment placeholder arity blocked local apply before full lab completion.
- Attempt 2: `PARTIAL` - CG005I-M local lab validated, but PLATFORM_008 was blocked by input_file_name technical-code validation.
- Attempt 3: `LOCAL_LAB_VALIDATED` - Final run validated CG005I-M and PLATFORM_008 after using the technical input code.

## Platform 008

- Observation candidate validated: `True`
- Input file code: `DB_GLOBAL_INVENTARIO_XLSX`
- Observation ledger unchanged: `True`
- Ledger write executed: `False`
- Productive DDL/apply, Supabase writes, cleanup and retention remain unauthorized.
