---
name: sz-context
description: Genera un contexto determinista y compacto de STOCK_ZERO para un dominio usando la memoria versionada del repositorio.
disable-model-invocation: true
allowed-tools:
  - Bash(python scripts/sz_context_bundle.py *)
---

# STOCK_ZERO context bundle

Dominio solicitado: $0

## Contexto generado

!`python scripts/sz_context_bundle.py --domain "$0" --pretty`

## Respuesta

Devuelve exclusivamente el JSON generado arriba.

No leas otros archivos.
No busques código adicional.
No ejecutes DB, Docker, SQL, loaders, tests, commit o push.
No modifiques archivos.
No amplíes la investigación.
Si el JSON contiene error, devuelve ese error sin intentar corregirlo.
