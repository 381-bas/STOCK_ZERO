---
name: sz-load-observation
description: Prepara o valida un candidato de observacion de carga sin ejecutar loaders, DB ni escribir el ledger.
disable-model-invocation: true
allowed-tools:
  - Bash(python scripts/sz_load_observation.py *)
---

# STOCK_ZERO load observation candidate

Convierte un resultado tecnico seguro de una fase de carga en un candidato
normalizado segun `research/AI_LOAD_OBSERVATION_CONTRACT.json`, o valida un
candidato ya creado. El resultado se entrega solo por pantalla.

El script falla cerrado ante rutas fuera de alcance, rutas sensibles,
contradicciones entre CLI y JSON, tipos invalidos, conteos incoherentes,
identidad sin hash SHA-256 valido, shapes incompletos, privacidad riesgosa,
labels incompatibles con la operacion, referencias de evidencia invalidas y
errores de argumentos.

## Ejecucion

!`python scripts/sz_load_observation.py $ARGUMENTS`

## Respuesta

Devuelve exclusivamente el JSON generado por el script.

- No leas otros archivos.
- No corrijas el resultado con razonamiento libre.
- No escribas el ledger ni ningun archivo.
- No ejecutes DB, Docker, SQL, loaders, refresh, commit ni push.
- No agregues la observacion al ledger: eso requiere una fase independiente,
  validacion de Codex y autorizacion de Bastian.
- Si el script devuelve un objeto `error`, devuelvelo intacto sin adivinar campos.
- Los errores del CLI tambien son JSON; no reemplaces ni completes el payload.

Cada observacion nace como `UNREVIEWED` salvo label explicito valido, y
`implementation_authorized` es siempre `false`. El `observation_id` identifica
un evento y se deriva de source, semana, operacion, hash de archivo y
`recorded_at` normalizado.
