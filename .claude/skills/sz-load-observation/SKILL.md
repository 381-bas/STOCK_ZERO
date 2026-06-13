---
name: sz-load-observation
description: Prepara o valida un candidato de observación de carga sin ejecutar loaders, DB ni escribir el ledger.
disable-model-invocation: true
allowed-tools:
  - Bash(python scripts/sz_load_observation.py *)
---

# STOCK_ZERO load observation candidate

Convierte un resultado técnico seguro de una fase de carga en un candidato
normalizado segun `research/AI_LOAD_OBSERVATION_CONTRACT.json`, o valida un
candidato ya creado. El resultado se entrega solo por pantalla.

## Ejecución

!`python scripts/sz_load_observation.py $ARGUMENTS`

## Respuesta

Devuelve exclusivamente el JSON generado por el script.

- No leas otros archivos.
- No corrijas el resultado con razonamiento libre.
- No escribas el ledger ni ningún archivo.
- No ejecutes DB, Docker, SQL, loaders, refresh, commit ni push.
- No agregues la observación al ledger: eso requiere una fase independiente,
  validación de Codex y autorización de Bastián.
- Si el script devuelve un objeto `error`, devuélvelo intacto sin adivinar campos.

Cada observación nace como `UNREVIEWED` salvo label explícito válido, y
`implementation_authorized` es siempre `false`.
