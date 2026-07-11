# Control Gestion operational calendar rule

Status: active contract, promoted from existing evidence.

This is not a new business rule. It promotes prior 014A/014C/014D evidence into a reusable Control Gestion calendar contract.

Authoritative contract:

- `contracts/control_gestion/operational_calendar_contract_v1.json`
- `scripts/monthly_input_layout_contract_015.py`

Rules:

- Operational week starts on Monday and ends on Sunday.
- A week belongs to the month containing at least 4 days of that week.
- `S1` is the first Monday-Sunday week assigned to the operational month.
- `S2..Sn` advance by 7 days from `S1`.
- An operational month has 4 or 5 weeks.
- `S1` may start in the previous calendar month.
- The final week may end in the next calendar month.
- Calendar folder does not govern operational week ownership.
- RUTA_RUTERO month governs the operational weeks expected for that month.

Equivalent S1 rule:

- If day 1 falls Monday through Thursday, `S1` is the week containing day 1.
- If day 1 falls Friday through Sunday, `S1` begins the following Monday.

Examples:

- `2026-06`: `S1=2026-06-01`, `S4=2026-06-22`, coverage `2026-06-01..2026-06-28`.
- `2026-07`: `S1=2026-06-29`, `S5=2026-07-27`, coverage `2026-06-29..2026-08-02`.
- `2027-01`: `S1=2027-01-04`.
- `2027-04`: `S1=2027-03-29`.

Historical authority:

- `scripts/validate_june_data_foundation_gate_014A_no_apply.py`
- `research/014C_KPIONE_RAW_EXPORT_VALIDATOR_NO_APPLY`
- `research/014D_KPIONE_RAW_EXPORT_REMEDIATION_NO_APPLY`
- `contracts/control_gestion/kpione2_photo_export_contract_v1.json`

No-apply boundary: this contract does not authorize DB access, Supabase, SQL apply, DDL, productive loader changes, app runtime changes, Excel edits, or data movement.
