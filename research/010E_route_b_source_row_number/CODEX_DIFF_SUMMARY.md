# 010E Route B Diff Summary

## Modified

- `scripts/load_kpione2_photo_from_excel.py`
  - assigns 1-based Excel `source_row_number` values after sheet read
  - validates complete, unique and contiguous photo-row provenance
  - emits source-row metrics, samples and a full-mapping SHA-256 manifest
  - preserves `ID + SP Item ID` as event identity
- `tests/test_kpione2_photo_grain.py`
  - validates origin mapping and index-independent stability
  - extends CLI and real-workbook assertions

## Created

- `research/010E_route_b_source_row_number/CODEX_DRY_RUN_OUTPUT.json`
- `research/010E_route_b_source_row_number/CODEX_SOURCE_ROW_NUMBER_NOTE.md`
- `research/010E_route_b_source_row_number/CODEX_DIFF_SUMMARY.md`

## Explicitly Not Modified

- `scripts/load_control_gestion_raw_v17.py`
- `contracts/control_gestion/kpione2_photo_export_contract_v1.json`
- productive compliance views
- SQL DDL and rollback files
- governance locks and status files

## Safety

- DB apply: `false`
- SQL apply: `false`
- writes executed: `false`
- productive loader touched: `false`
