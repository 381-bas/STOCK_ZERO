Antes de emitir juicio, levanto evidencia real: qué validó el gate 014A, qué consume hoy el loader productivo, y qué contienen de verdad el maestro manual y los 6 exports raw (solapamiento de IDs incluido — sin eso, cualquier respuesta sobre duplicados es narrativa).



Heredoc no funciona en PowerShell — cambio a Bash para la inspección Python, y localizo el closeout exacto vía git.



Auditoría con evidencia dura, no narrativa. Inspeccioné los 6 exports raw (filas, IDs, ventanas, solapamiento), la hoja `DB (KPIONE2.0)` del maestro (esquema, cobertura, y la fórmula real de `VISITA`), el gate 014A committeado y su closeout. Tres descubrimientos cambian el problema.



---



\# Auditoría adversarial 014B — Reemplazo de CUMPLIMIENTO\_FRECUENCIA.xlsx por ingesta raw KPIONE



\## Los tres hechos que cambian el problema



\*\*\[FACT 1] El maestro manual ES los exports raw + 3 columnas derivadas.\*\* La hoja `DB (KPIONE2.0)` tiene el mismo esquema que `photo-excel-admin\_\*` (ID, SP Item ID, Holding… Link Foto) más `VISITA`, `REGISTRO\_FUERA\_CRUCE`, `SEMANA`. Población idéntica verificada: para 2026-06-01 (único día de solapamiento), 1,220 de 1,221 IDs del maestro están en el export raw (99.7%). No son dos fuentes: son la misma fuente en dos estados de procesamiento.



\*\*\[FACT 2] La semántica de VISITA no viene de KPIONE — es una fórmula Excel del maestro.\*\* `VISITA = IFERROR(1/COUNTIFS(Codigo Local, Fecha, Marca), 1/COUNTIFS(Local, Fecha, Marca))`: presencia fraccional 1/N por (local, día, marca), con \*\*fallback a nombre de Local cuando Codigo Local falla\*\*. Es day-presence implementado en COUNTIFS — la validación empírica de todo el diseño Route B. Y expone una semántica oculta: el pipeline real tolera `Codigo Local` vacío vía fallback por nombre; el validador 013 lo \*\*bloquea\*\*. Esa divergencia hay que resolverla explícitamente, no descubrirla en producción.



\*\*\[FACT 3] Los 6 exports no son 6 batches comparables y junio NO está cubierto.\*\*



| Export (epoch ms) | Descarga UTC | Filas | IDs | Ventana Fecha |

|---|---|---:|---:|---|

| …5989376 | 06-20 17:19 | 46,807 | 7,101 | 06-01..06-07 |

| …6368641 | 06-20 17:26 | \*\*50,001\*\* | 7,832 | 06-08..06-13 |

| …2877303 | 06-21 03:34 | 8,372 | 1,295 | \*\*06-08 solamente\*\* |

| …6423312 | 06-20 17:27 | 107 | 14 | \*\*06-14 solamente\*\* |

| …3512473 | 06-20 16:38 | 46,078 | 6,997 | 06-15..06-19 |

| …0454408 | 06-26 02:20 | 37,908 | 5,892 | 06-20..06-24 |



Solapamiento de IDs entre archivos: \*\*cero\*\*. IDs con más de una Fecha: \*\*cero\*\*. El archivo de 50,001 filas huele a \*\*cap de export (~50k) truncado\*\*, y los dos archivos de un solo día son los parches manuales que lo compensan (el parche de 06-08 tiene cero IDs en común con el archivo que "cubre" 06-08..13: contiene exactamente lo que el truncado perdió). Cobertura real: maestro hasta 06-01, raw 06-01..06-24, \*\*06-25..06-30 no existe en ningún lado\*\*.



\## Respuestas



\*\*1. Suposiciones que podrían estar mal:\*\* (a) "6 archivos = 6 batches" — falso: 1 truncado + 2 parches; (b) "no hay duplicados entre exports" — cierto hoy solo porque un humano gestionó ventanas disjuntas a mano; mañana un re-download de la misma semana romperá el supuesto; (c) "VISITA viene de la fuente" — falso, es fórmula local del maestro; (d) "el export es siempre nivel-foto" — el maestro está casi 1-fila-por-ID (45,736 filas / 45,236 IDs): o KPIONE tiene/tuvo un modo export nivel-evento, o alguien deduplicaba a mano — hay que preguntarle a Bastián cuál de las dos, porque define si la eventificación es nuestra responsabilidad permanente; (e) "ID globalmente único y estable" — soportado por 3.5 semanas de evidencia (0 multi-fecha), no probado a escala de meses; (f) el propio maestro tiene ~500 IDs duplicados internos (45,736 vs 45,236) — el modo de fallo del paste manual que este modelo viene a matar.



\*\*2. ¿Nueva entrada o evidencia auxiliar?\*\* \*\*Nueva entrada primaria.\*\* Con 99.7% de población compartida y esquema idéntico, tratarlos como "evidencia auxiliar" sería negar lo que los datos muestran: son el insumo upstream del maestro. Lo auxiliar no es el archivo — es la derivación (`VISITA`, cruce, semana) que hoy vive en fórmulas Excel y debe migrar a código testeable (013 ya construyó la mitad).



\*\*3. Duplicados entre exports raw:\*\* tres capas, todas locales: (i) \*\*manifiesto por archivo\*\* (sha256, filas, IDs distintos, fecha\_min/max) — detecta el re-download idéntico por hash; (ii) \*\*matriz de intersección de IDs\*\* entre archivos — hoy debe ser 0; cualquier valor >0 exige clasificación: mismo ID + mismo `photo\_row\_hash` → dedupe silencioso; mismo ID + contenido distinto → conflicto que bloquea hasta decisión explícita de precedencia por batch (nunca "gana el último" implícito — regla dura del proyecto); (iii) \*\*gate de truncamiento\*\*: filas ≥ 50,000 → flag `TRUNCATION\_SUSPECT` bloqueante, porque el archivo de 50,001 prueba que el export corta.



\*\*4. ¿El ID del nombre de archivo sirve como source\_file\_id o batch\_id?\*\* Como `source\_file\_id`: \*\*sí\*\* — es epoch-ms de generación (1781973512473 → 2026-06-20 16:38 UTC, coincide con el mtime), único por artefacto de descarga y ordena descargas. Como `batch\_id`: \*\*no\*\* — tres descargas del mismo minuto contienen tres semanas distintas y un re-export futuro de la misma ventana tendrá otro epoch. El batch debe ser declarado: (ventana declarada, set de source\_file\_ids, sha256s). Es exactamente la regla "never treat latest as temporal contract" aplicada a archivos. Y el nombre es reescribible por humanos: la identidad de contenido es el sha256, el nombre es solo etiqueta.



\*\*5. Campos mínimos del evento canónico:\*\* `event\_id` (ID), `sp\_item\_id`, `cod\_rt` (Codigo Local), `cliente\_norm` (Marca), `local\_nombre` (obligatorio: es la clave de fallback de la fórmula real), `fecha`, `week\_start` (lunes), `n\_fotos\_calculado`, `photo\_rows`, `reponedor`, `tipos\_de\_tarea`, `event\_stable\_hash`, y trazabilidad: `source\_file\_id`, `source\_file\_sha256`, `source\_row\_span`. Derivadas aguas abajo (no del raw): presencia diaria por (cod\_rt|local\_nombre, fecha, cliente\_norm) y el equivalente de `REGISTRO\_FUERA\_CRUCE` (requiere join con ruta — pertenece a la capa de cruce, no al evento).



\*\*6. Qué queda local/repo y qué llegaría a Supabase (futuro RED):\*\* en repo: contratos, validadores, manifiestos ligeros JSON (hash, conteos, ventanas, verdicts) y fixtures golden pequeños. En evidence/local: dumps pesados, perfiles completos, comparaciones fila a fila. A Supabase, como máximo y solo en RED: filas normalizadas nivel-foto según el DDL ya revisado (`sql/15 cg\_raw.kpione2\_photo\_raw`) + registro de batch (file\_id, sha256, ventana, conteos) — jamás manifiestos históricos ni evidencia de auditoría (restricción explícita de fase, y correcta).



\*\*7. Compatibilidad con la app mientras se rompe la dependencia:\*\* la app no sabe que existe el maestro — lee vistas Supabase vía `get\_cg\_v2\_\*`; el loader v17 es el único consumidor del maestro. Estrategia: \*\*congelar\*\* el maestro como artefacto histórico (≤ 06-01), desarrollar el camino raw en paralelo 100% local (derivación + paridad), y no tocar loader/db.py/UX hasta que la paridad esté probada. La app sigue funcionando porque nada de lo que consume cambia. El puente de paridad ya existe: el validador 013 y el contrato photo v1 son la mitad de la maquinaria.



\*\*8. Romper sin miedo:\*\* el mantenimiento hacia adelante del maestro (no más pastes post-06-01 — cada paste nuevo agranda la deuda); el supuesto "1 descarga = 1 semana completa" (reemplazar por descargas ventaneadas + manifiesto); las hojas `Tabla1`, `OUT`, `gg`, `DESARROLLO\_CONTROL\_GESTION` como fuera de contrato (el loader solo consume 3 hojas — validado C001); y la fórmula COUNTIFS como dueña de la semántica VISITA — se replica en código con test golden y deja de vivir en Excel.



\*\*9. No tocar todavía:\*\* `load\_control\_gestion\_raw\_v17.py`, el contrato activo v1 (extensión = propuesta v2, no edición in-place), `app/db.py`, UX activa, Supabase/SQL, y \*\*el archivo maestro mismo\*\* (es evidencia histórica read-only; ni "limpiarlo" ni corregirle los ~500 duplicados — se documentan, no se editan).



\*\*10. Fases dentro de PR 14 (sin PR por idea):\*\* ya está bien encaminado con letras internas. \*\*014B\*\*: modelo de ingesta raw no-apply — manifiesto por archivo + gates de duplicado/truncamiento/ventana + derivación canónica reutilizando funciones de `load\_kpione2\_photo\_from\_excel` + réplica de la fórmula VISITA con fallback. \*\*014C\*\*: corrida de evidencia sobre los 6 archivos + paridad 06-01 vs maestro + mapa de cobertura junio + decisión. \*\*014D\*\* (solo si Bastián autoriza tocar UX): preview visible alimentado del artefacto local. Si no, PR 14 cierra con B+C y el digest ya implementado.



\*\*11. Evidencia 014C para que la decisión no sea narrativa:\*\* (i) manifiesto JSON versionado por archivo (sha256, filas, IDs, ventana, flag truncamiento, fila de la matriz de solapamiento); (ii) \*\*paridad 06-01\*\*: tasa de match de IDs (esperado ≥99%), y suma de VISITA replicada por (clave, fecha, marca) = presencias del maestro, con tabla de deltas y cada delta explicado; (iii) test unitario golden de la fórmula VISITA incluyendo el caso fallback Codigo Local vacío; (iv) calendario junio por fuente con huecos explícitos (06-25..30 faltante); (v) comandos exactos con intérprete del venv y exit codes. Y una corrección de proceso no negociable: el closeout 014A versionado tiene \*\*placeholders PowerShell sin expandir\*\* (`$phase`, `$mdHash`, `$evDir`) y su evidencia real vive en una carpeta git-ignorada — es decir, el artefacto auditable no contiene los hashes que dice contener. 014C debe versionar el manifiesto ligero en `research/014\_\*/`, no dejarlo en `evidence/` invisible.



\*\*12. Veredicto final: `PROMOTE\_RAW\_EXPORTS\_AS\_NEW\_INPUT`\*\* — como dirección de modelo de ingesta, local y no-apply, con tres condiciones bloqueantes antes de cualquier promoción productiva: (a) gate de truncamiento/ventana/duplicado operativo (el archivo de 50,001 filas lo exige); (b) paridad 06-01 y réplica de VISITA con fallback resuelto contra el contrato (hoy 013 bloquea lo que el maestro tolera — decisión de Bastián: ¿el fallback por nombre es política o deuda?); (c) cierre del hueco 06-25..30 con descargas nuevas bajo el modelo ventaneado. No elijo `USE\_RAW\_EXPORTS\_FOR\_UX\_PREVIEW\_ONLY` porque subusa la evidencia (99.7% de población compartida no es un preview — es la fuente); no elijo `KEEP\_LEGACY\_MASTER\_TEMPORARILY` como veredicto porque ya es cierto por construcción (el loader sigue intacto y congelado hasta RED — no necesita veredicto, necesita fecha de defunción); y `BLOCK` sería teatro: las dos incógnitas reales (cap de export, modo evento-vs-foto) se responden con una pregunta a Bastián y un gate de 20 líneas, no bloqueando la fase.



\*\*La pregunta que solo Bastián puede responder\*\* (y conviene responder antes de 014C): ¿el maestro está 1-fila-por-evento porque KPIONE ofrecía un export nivel-evento, o porque tú deduplicabas a mano? Si existía un export nivel-evento, hay que saber por qué se abandonó — podría ser el formato correcto a pedir de vuelta. Y su gemela operativa: ¿el corte en 50,001 filas es un límite conocido de la plataforma KPIONE? Si sí, el gate de truncamiento pasa de heurística a regla contractual.

