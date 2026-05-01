import html
import os
import math
import traceback

import pandas as pd
import streamlit as st


def _cg_get_page_state(page_key: str, page_size: int = 25) -> tuple[int, int]:
    st.session_state.setdefault(page_key, 1)
    try:
        current_page = int(st.session_state.get(page_key, 1))
    except Exception:
        current_page = 1
    current_page = max(1, current_page)
    st.session_state[page_key] = current_page
    return current_page, (current_page - 1) * page_size


def _cg_render_pager(page_key: str, total_rows: int, page_size: int = 25) -> None:
    total_pages = max(1, int(math.ceil(total_rows / page_size))) if total_rows > 0 else 1
    if int(st.session_state.get(page_key, 1)) > total_pages:
        st.session_state[page_key] = total_pages
    if int(st.session_state.get(page_key, 1)) < 1:
        st.session_state[page_key] = 1

    current_page = int(st.session_state.get(page_key, 1))

    def _page_prev():
        st.session_state[page_key] = max(1, int(st.session_state.get(page_key, 1)) - 1)

    def _page_next():
        st.session_state[page_key] = min(total_pages, int(st.session_state.get(page_key, 1)) + 1)

    p1, p2, p3 = st.columns([0.9, 1.8, 2.5], gap="small")
    with p1:
        st.button(
            "◀",
            key=f"{page_key}_prev_btn",
            disabled=(current_page <= 1),
            on_click=_page_prev,
            use_container_width=True,
        )
    with p2:
            st.selectbox(
            "Página",
            options=list(range(1, total_pages + 1)),
            index=max(0, current_page - 1),
            key=f"{page_key}_selectbox",
            on_change=lambda: st.session_state.update({page_key: st.session_state[f"{page_key}_selectbox"]}),
            label_visibility="collapsed",
        )
    with p3:
        st.button(
            "▶",
            key=f"{page_key}_next_btn",
            disabled=(current_page >= total_pages),
            on_click=_page_next,
            use_container_width=True,
        )
    if total_rows:
        start = (current_page - 1) * page_size + 1
        end = min(current_page * page_size, total_rows)
        st.caption(f"{start}-{end} de {total_rows} registros")
    else:
        st.caption("Sin filas para el filtro aplicado.")


def _cg_show_df(title: str, df: pd.DataFrame | None) -> None:
    st.markdown(f"#### {title}")
    if df is None or df.empty:
        st.info("Sin filas para esta superficie del contrato B3.")
        return
    st.dataframe(df, width="stretch", hide_index=True)


def _cg_metric_cards(metrics: dict[str, int | float]) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Cumple", int(metrics.get("cumple") or 0))
    c2.metric("Incumple", int(metrics.get("incumple") or 0))
    c3.metric("Cumple c/doble", int(metrics.get("cumple_con_doble_marcaje") or 0))
    c4.metric("Locales", int(metrics.get("locales_scope") or 0))
    c5, c6 = st.columns(2)
    c5.caption(f'Visitas plan: {int(metrics.get("visitas_plan") or 0)}')
    c6.caption(f'Visitas realizadas: {int(metrics.get("visitas_realizadas") or 0)}')


def _cg_ensure_option_state(state_key: str, options: list[str], default: str) -> str:
    if st.session_state.get(state_key) not in options:
        st.session_state[state_key] = default
    return str(st.session_state.get(state_key) or default)


def _cg_reset_page_on_filter_change(filter_key: str, page_key: str, values: tuple[object, ...]) -> None:
    if st.session_state.get(filter_key) != values:
        st.session_state[filter_key] = values
        st.session_state[page_key] = 1


def _cg_v2_metric_cards(
    scope_metrics: dict[str, int | float],
) -> None:
    visita_plan = int(scope_metrics.get("visita_plan") or 0)
    visita_realizada_raw = int(scope_metrics.get("visita_realizada_raw") or 0)
    visita_realizada_cap = int(scope_metrics.get("visita_realizada_cap") or 0)
    visitas_pendientes = int(scope_metrics.get("visitas_pendientes") or 0)
    sobre_cumplimiento = int(scope_metrics.get("sobre_cumplimiento") or 0)
    cumple_rows = int(scope_metrics.get("cumple_rows") or 0)
    incumple_rows = int(scope_metrics.get("incumple_rows") or 0)
    gestion_compartida_rows = int(scope_metrics.get("gestion_compartida_rows") or 0)
    pct_cumplimiento = round((visita_realizada_cap / visita_plan) * 100, 1) if visita_plan > 0 else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("% cumplimiento", f"{pct_cumplimiento}%")
    c2.metric("Visitas exigidas", visita_plan)
    c3.metric("Visitas validas", visita_realizada_cap)
    c4.metric("Visitas pendientes", visitas_pendientes)

    c5, c6, c7, c8 = st.columns(4)
    c5.metric("Rutas cumplen", cumple_rows)
    c6.metric("Rutas incumplen", incumple_rows)
    c7.metric("Sobrecumplimiento", sobre_cumplimiento)
    c8.metric("Gestion compartida", gestion_compartida_rows)

    st.caption(f"Visitas reportadas: {visita_realizada_raw}")


def _cg_v2_audit_cards(audit_metrics: dict[str, int | float]) -> None:
    st.markdown("#### Auditorias y contexto V2")
    a1, a2, a3, a4 = st.columns(4)
    a1.metric("Ruta duplicada", int(audit_metrics.get("ruta_duplicada_rows") or 0))
    a2.metric("Fuera cruce real", int(audit_metrics.get("fuera_cruce_real_rows") or 0))
    a3.metric("Doble/triple", int(audit_metrics.get("doble_triple_rows") or 0))
    a4.metric("Sin batch ruta semana", int(audit_metrics.get("sin_batch_ruta_semana_rows") or 0))

    st.caption("Sin batch ruta semana se muestra como contexto historico, no como KPI operativo principal.")
    with st.expander("Ayuda auditoria v2", expanded=False):
        st.caption("Fuera cruce real: registros que no calzan con ruta vigente.")
        st.caption(
            "Sin batch ruta semana: evidencia historica sin snapshot semanal de RUTA_RUTERO; "
            "no necesariamente error operativo actual."
        )
        st.caption("Doble/triple: misma visita detectada en mas de una fuente.")
        st.caption("Ruta duplicada: combinacion ruta/cliente duplicada en la base operativa versionada.")


def _cg_v2_day_html(value: object) -> str:
    raw = str(value or "").strip().upper()
    check = '<span class="cg-ok-check">&#10003;</span>'
    if raw == "REQ_OK":
        return f'1 {check}'
    if raw == "REQ":
        return "1"
    if raw == "OK":
        return check
    return ""


def _cg_v2_render_validation_table(df: pd.DataFrame) -> None:
    columns = [
        "COD_RT",
        "LOCAL",
        "CLIENTE",
        "VISITAS EXIGIDAS",
        "LUN",
        "MAR",
        "MIE",
        "JUE",
        "VIE",
        "SAB",
        "DOM",
        "PENDIENTE",
        "ALERTA",
        "MODALIDAD",
        "GESTION COMPARTIDA",
    ]
    day_cols = {"LUN", "MAR", "MIE", "JUE", "VIE", "SAB", "DOM"}
    rows_html: list[str] = []
    for _, row in df.iterrows():
        cell_html: list[str] = []
        for col in columns:
            raw_value = row.get(col, "")
            if col in day_cols:
                rendered = _cg_v2_day_html(raw_value)
                cell_html.append(f'<td class="cg-day-cell">{rendered}</td>')
            else:
                safe_value = html.escape("" if pd.isna(raw_value) else str(raw_value))
                cell_html.append(f"<td>{safe_value}</td>")
        rows_html.append("<tr>" + "".join(cell_html) + "</tr>")

    header_html = "".join(f"<th>{html.escape(col)}</th>" for col in columns)
    table_html = f"""
    <style>
      .cg-validation-wrap {{ overflow-x: auto; }}
      table.cg-validation-table {{
        width: 100%;
        border-collapse: collapse;
        margin: 0.5rem 0 1rem 0;
        font-size: 0.92rem;
      }}
      .cg-validation-table th,
      .cg-validation-table td {{
        border-bottom: 1px solid rgba(49, 51, 63, 0.18);
        padding: 0.45rem 0.55rem;
        text-align: left;
        vertical-align: middle;
      }}
      .cg-validation-table th {{
        position: sticky;
        top: 0;
        background: #f7f8fb;
        z-index: 1;
      }}
      .cg-validation-table td.cg-day-cell {{
        min-width: 3.25rem;
        text-align: center;
        white-space: nowrap;
      }}
      .cg-ok-check {{
        color: #14804a;
        font-weight: 700;
      }}
    </style>
    <div class="cg-validation-wrap">
      <table class="cg-validation-table">
        <thead><tr>{header_html}</tr></thead>
        <tbody>{''.join(rows_html)}</tbody>
      </table>
    </div>
    """
    st.markdown(table_html, unsafe_allow_html=True)


def render_control_gestion(
    *,
    db,
    DEBUG,
    top2,
    top3,
    show_top_selectors=True,
    _dbg,
    _timed,
    _df_total_rows,
    _rename_and_pick
):
    cg_role_opts = ["JEFE OPERACIONES", "GESTOR"]
    cg_module_opts = ["Inicio", "Cumplimiento"]
    cg_all_token = "Todos"

    if show_top_selectors:
        with top2:
            st.selectbox(
                "ROL",
                cg_role_opts,
                key="sel_cg_role",
            )

        with top3:
            st.selectbox(
            "MÓDULO",
            cg_module_opts,
            key="sel_cg_module",
            )
    else:
        st.session_state["sel_cg_role"] = st.session_state.get("sel_cg_role") or "JEFE OPERACIONES"
        st.session_state["sel_cg_module"] = "Cumplimiento"

    role_sel = (st.session_state.get("sel_cg_role") or "JEFE OPERACIONES").strip().upper()
    module_sel = (st.session_state.get("sel_cg_module") or "Inicio").strip()
    use_cg_v2_requested = module_sel == "Cumplimiento" and os.getenv("USE_CG_V2", "1") == "1"
    cg_mode = "legacy"
    cg_caption = "B3 contrato publico"
    cg_fallback_notice: str | None = None

    smoke = None
    smoke_status = "DEFERRED"
    if use_cg_v2_requested:
        cg_mode = "v2"
        cg_caption = "CONTROL_GESTION v2"
        if show_top_selectors:
            st.caption(f"{cg_caption} | modo={cg_mode.upper()} | rol={role_sel} | modulo={module_sel}")
        else:
            st.caption(f"{cg_caption} | modo={cg_mode.upper()}")
    else:
        try:
            smoke = db.get_cg_contract_smoke()
            smoke_status = str(smoke.get("smoke_status") or "unknown").upper()
        except Exception as e:
            _dbg("FAIL cg_contract_smoke", err=repr(e))
            st.error("No pude validar el contrato B3 en db.py.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

        if show_top_selectors:
            st.caption(f"{cg_caption} | smoke={smoke_status} | modo={cg_mode.upper()} | rol={role_sel} | modulo={module_sel}")
        else:
            st.caption(f"{cg_caption} | smoke={smoke_status} | modo={cg_mode.upper()}")

        with st.expander("Estado contrato B3", expanded=(smoke_status != "OK")):
            smoke_rows = pd.DataFrame(smoke.get("results") or [])
            if smoke_rows.empty:
                st.warning("Smoke sin resultados. Revisa visibilidad de views public.v_cg_*.")
            else:
                st.dataframe(smoke_rows, width="stretch", hide_index=True)

    if cg_fallback_notice:
        st.warning(cg_fallback_notice)

    try:
        if module_sel == "Inicio":
            if role_sel == "JEFE OPERACIONES":
                with _timed("PAGE cg_inicio_jefe", tag="PAGE"):
                    df_inicio = db.get_cg_inicio_jefe(limit=50, offset=0)
            else:
                with _timed("PAGE cg_inicio_gestor", tag="PAGE"):
                    df_inicio = db.get_cg_inicio_gestor(limit=50, offset=0)

            with _timed("PAGE cg_alertas_control_gestion", tag="PAGE"):
                df_alertas = db.get_cg_alertas_page(limit=100, offset=0)

            inicio_row = df_inicio.iloc[0].to_dict() if df_inicio is not None and not df_inicio.empty else {}
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Locales", int(inicio_row.get("locales_scope") or 0))
            c2.metric("Clientes", int(inicio_row.get("clientes_scope") or 0))
            c3.metric("Reponedores", int(inicio_row.get("reponedores_scope") or 0))
            c4.metric("Cobertura", f'{round(float(inicio_row.get("pct_cobertura") or 0) * 100, 1)}%')

            c5, c6, c7 = st.columns(3)
            c5.caption(f'Eventos total: {int(inicio_row.get("eventos_total") or 0)}')
            c6.caption(f'Con evidencia: {int(inicio_row.get("eventos_con_evidencia") or 0)}')
            c7.caption(f'Sin evidencia: {int(inicio_row.get("eventos_sin_evidencia") or 0)}')

            df_alertas_view = _rename_and_pick(
                df_alertas,
                {
                    "alerta_tipo": "ALERTA",
                    "fuente": "FUENTE",
                    "cod_rt": "COD_RT",
                    "cliente": "CLIENTE",
                    "persona": "PERSONA",
                    "fecha_visita": "FECHA VISITA",
                    "detalle": "DETALLE",
                    "prioridad": "PRIORIDAD",
                },
                ["ALERTA", "FUENTE", "COD_RT", "CLIENTE", "PERSONA", "FECHA VISITA", "DETALLE", "PRIORIDAD"],
            )
            _cg_show_df("Alertas prioritarias", df_alertas_view)
            st.info("Supervisión queda reservada fuera del contrato público B3 actual.")
            st.stop()

        if cg_mode == "v2":
            try:
                recent_weeks = list(db.get_cg_v2_recent_weeks(limit=3) or [])
                if not recent_weeks:
                    raise RuntimeError("CONTROL_GESTION v2 no devolvio semanas recientes.")

                semanas_map: dict[str, str] = {}
                for idx, raw_value in enumerate(recent_weeks):
                    semana_value = pd.to_datetime(raw_value).strftime("%Y-%m-%d")
                    prefix = "Actual" if idx == 0 else f"S-{idx}"
                    semanas_map[f"{prefix} | {semana_value}"] = semana_value

                semana_opts = list(semanas_map.keys())
                semana_key = "sel_cg_v2_semana_recent"
                semana_default = semana_opts[0]
                _cg_ensure_option_state(semana_key, semana_opts, semana_default)

                vista_map = {
                    "Por rutero": "RUTERO",
                    "Por local": "LOCAL",
                    "Por cliente": "CLIENTE",
                }
                vista_opts = list(vista_map.keys())
                vista_key = "sel_cg_v2_analysis_view"
                _cg_ensure_option_state(vista_key, vista_opts, vista_opts[0])

                f1, f2, f3 = st.columns(3)
                with f1:
                    semana_label = st.selectbox("SEMANA", semana_opts, key=semana_key)
                    semana_inicio = semanas_map.get(semana_label)
                with f2:
                    gestores_v2 = [cg_all_token] + list(db.get_cg_v2_gestores(semana_inicio=semana_inicio) or [])
                    gestor_key = "sel_cg_v2_gestor"
                    _cg_ensure_option_state(gestor_key, gestores_v2, cg_all_token)
                    gestor_label = st.selectbox("GESTOR", gestores_v2, key=gestor_key)
                    gestor_sel = None if gestor_label == cg_all_token else gestor_label
                with f3:
                    vista_label = st.selectbox("VISTA DE ANALISIS", vista_opts, key=vista_key)
                    vista_sel = vista_map.get(vista_label, "RUTERO")

                target_label = "RUTERO"
                target_key = "sel_cg_v2_target_rutero"
                target_opts = [cg_all_token]
                if gestor_sel is not None:
                    if vista_sel == "RUTERO":
                        target_label = "RUTERO"
                        target_key = "sel_cg_v2_target_rutero"
                        target_opts = [cg_all_token] + list(
                            db.get_cg_v2_ruteros(semana_inicio=semana_inicio, gestor=gestor_sel) or []
                        )
                    elif vista_sel == "LOCAL":
                        target_label = "LOCAL"
                        target_key = "sel_cg_v2_target_local"
                        target_opts = [cg_all_token] + list(
                            db.get_cg_v2_locales(semana_inicio=semana_inicio, gestor=gestor_sel) or []
                        )
                    else:
                        target_label = "CLIENTE"
                        target_key = "sel_cg_v2_target_cliente"
                        target_opts = [cg_all_token] + list(
                            db.get_cg_v2_clientes(semana_inicio=semana_inicio, gestor=gestor_sel) or []
                        )

                _cg_ensure_option_state(target_key, target_opts, cg_all_token)

                g1, g2 = st.columns([1.4, 1.0])
                with g1:
                    target_value = st.selectbox(target_label, target_opts, key=target_key)
                with g2:
                    alerta_opts = ["Todas"] + list(
                        db.get_cg_v2_alertas(
                            semana_inicio=semana_inicio,
                            gestor=gestor_sel,
                            cliente=(None if vista_sel != "CLIENTE" or target_value == cg_all_token else target_value),
                            rutero=(None if vista_sel != "RUTERO" or target_value == cg_all_token else target_value),
                            local=(None if vista_sel != "LOCAL" or target_value == cg_all_token else target_value),
                        ) or []
                    )
                    alerta_key = "sel_cg_v2_alerta"
                    _cg_ensure_option_state(alerta_key, alerta_opts, "Todas")
                    alerta_label = st.selectbox("ALERTA", alerta_opts, key=alerta_key)
                    alerta_sel = None if alerta_label == "Todas" else alerta_label

                rutero_sel = None
                local_sel = None
                cliente_sel = None
                if vista_sel == "RUTERO" and target_value != cg_all_token:
                    rutero_sel = target_value
                elif vista_sel == "LOCAL" and target_value != cg_all_token:
                    local_sel = target_value
                elif vista_sel == "CLIENTE" and target_value != cg_all_token:
                    cliente_sel = target_value

                gestor_ready = gestor_sel is not None
                detail_ready = gestor_ready and (
                    (vista_sel == "RUTERO" and rutero_sel is not None)
                    or (vista_sel == "LOCAL" and local_sel is not None)
                    or (vista_sel == "CLIENTE" and cliente_sel is not None)
                )

                st.caption("Semanas visibles: actual, S-1 y S-2.")

                kpi_df = db.get_cg_v2_scope_kpis(
                    semana_inicio=semana_inicio,
                    gestor=gestor_sel,
                    cliente=cliente_sel,
                    alerta=alerta_sel,
                    rutero=rutero_sel,
                    local=local_sel,
                )
                kpi_row = kpi_df.iloc[0].to_dict() if kpi_df is not None and not kpi_df.empty else {}
                _cg_v2_metric_cards(kpi_row)

                if not detail_ready:
                    st.info("Selecciona gestor y luego rutero/local/cliente para cargar la validación.")
                    st.stop()

                with _timed("PAGE cg_v2_daily_matrix_full", tag="PAGE"):
                    df_scope_v2 = db.get_cg_v2_daily_matrix_full(
                        semana_inicio=semana_inicio,
                        gestor=gestor_sel,
                        vista=vista_sel,
                        rutero=rutero_sel,
                        local=local_sel,
                        cliente=cliente_sel,
                        alerta=alerta_sel,
                    )

                st.markdown("#### Validación diaria")
                st.markdown("1 = visita exigida; &#10003; = evidencia registrada", unsafe_allow_html=True)

                if vista_sel == "RUTERO" and rutero_sel is not None and df_scope_v2 is not None and not df_scope_v2.empty:
                    rep_series = df_scope_v2["REPONEDOR"] if "REPONEDOR" in df_scope_v2.columns else pd.Series(dtype=object)
                    reponedores = sorted({str(v).strip() for v in rep_series.tolist() if str(v or "").strip()})
                    if len(reponedores) == 1:
                        st.caption(f"Rutero / Reponedor: {rutero_sel} / {reponedores[0]}")
                    elif len(reponedores) > 1:
                        preview = " | ".join(reponedores[:3])
                        suffix = " | ..." if len(reponedores) > 3 else ""
                        st.caption(f"Rutero / Reponedor: {rutero_sel} / {preview}{suffix}")
                    else:
                        st.caption(f"Rutero: {rutero_sel}")

                df_scope_v2_view = _rename_and_pick(
                    df_scope_v2,
                    {
                        "COD_RT": "COD_RT",
                        "LOCAL": "LOCAL",
                        "CLIENTE": "CLIENTE",
                        "VISITA": "VISITAS EXIGIDAS",
                        "LUN": "LUN",
                        "MAR": "MAR",
                        "MIE": "MIE",
                        "JUE": "JUE",
                        "VIE": "VIE",
                        "SAB": "SAB",
                        "DOM": "DOM",
                        "VISITAS_PENDIENTES": "PENDIENTE",
                        "ALERTA": "ALERTA",
                        "MODALIDAD": "MODALIDAD",
                        "GESTION_COMPARTIDA": "GESTION COMPARTIDA",
                    },
                    [
                        "COD_RT",
                        "LOCAL",
                        "CLIENTE",
                        "VISITAS EXIGIDAS",
                        "LUN",
                        "MAR",
                        "MIE",
                        "JUE",
                        "VIE",
                        "SAB",
                        "DOM",
                        "PENDIENTE",
                        "ALERTA",
                        "MODALIDAD",
                        "GESTION COMPARTIDA",
                    ],
                )
                _cg_v2_render_validation_table(df_scope_v2_view)

                show_audit = st.toggle("Ver auditoría detallada", value=False, key="cg_v2_show_audit")
                if show_audit:
                    audit_summary = db.get_cg_v2_audit_summary() or {}
                    _cg_v2_audit_cards(audit_summary)
                st.stop()
            except Exception as v2_exc:
                _dbg("WARN control_gestion_v2_fallback", err=repr(v2_exc), role=role_sel, module=module_sel)
                st.warning("CONTROL_GESTION v2 no pudo leerse completamente; uso fallback legacy.")
                with st.expander("Detalles tecnicos V2", expanded=False):
                    st.code(repr(v2_exc))
                    if DEBUG:
                        st.code(traceback.format_exc())

        semanas_raw = db.get_cg_scope_semanas()
        semanas_map: dict[str, str | None] = {cg_all_token: None}
        for value in semanas_raw:
            if pd.isna(value):
                continue
            label = pd.to_datetime(value).strftime("%Y-%m-%d")
            semanas_map[label] = label

        semana_opts = list(semanas_map.keys())
        semana_key = "sel_cg_semana"
        if st.session_state.get(semana_key) not in semana_opts:
            st.session_state[semana_key] = cg_all_token

        semana_inicio = None
        gestor_sel = None
        cliente_sel = None
        alerta_sel = None

        f1, f2, f3, f4 = st.columns(4)
        with f1:
            semana_label = st.selectbox("SEMANA", semana_opts, key=semana_key)
            semana_inicio = semanas_map.get(semana_label)

        gestores = db.get_cg_scope_gestores(semana_inicio=semana_inicio)
        gestor_opts = [cg_all_token] + gestores
        gestor_key = "sel_cg_gestor"
        if st.session_state.get(gestor_key) not in gestor_opts:
            st.session_state[gestor_key] = cg_all_token
        with f2:
            gestor_label = st.selectbox("GESTOR", gestor_opts, key=gestor_key)
            gestor_sel = None if gestor_label == cg_all_token else gestor_label

        clientes = db.get_cg_scope_clientes(semana_inicio=semana_inicio, gestor=gestor_sel)
        cliente_opts = [cg_all_token] + clientes
        cliente_key = "sel_cg_cliente"
        if st.session_state.get(cliente_key) not in cliente_opts:
            st.session_state[cliente_key] = cg_all_token
        with f3:
            cliente_label = st.selectbox("CLIENTE", cliente_opts, key=cliente_key)
            cliente_sel = None if cliente_label == cg_all_token else cliente_label

        alertas = db.get_cg_scope_alertas(
            semana_inicio=semana_inicio,
            gestor=gestor_sel,
            cliente=cliente_sel,
        )
        alerta_opts = ["Todas"] + alertas
        alerta_key = "sel_cg_alerta"
        if st.session_state.get(alerta_key) not in alerta_opts:
            st.session_state[alerta_key] = "Todas"
        with f4:
            alerta_label = st.selectbox("ALERTA", alerta_opts, key=alerta_key)
            alerta_sel = None if alerta_label == "Todas" else alerta_label

        kpi_df = db.get_cg_scope_kpis(
            semana_inicio=semana_inicio,
            gestor=gestor_sel,
            cliente=cliente_sel,
            alerta=alerta_sel,
        )
        kpi_row = kpi_df.iloc[0].to_dict() if kpi_df is not None and not kpi_df.empty else {}
        _cg_metric_cards(kpi_row)

        _, scope_offset = _cg_get_page_state("page_cg_scope", page_size=25)
        with _timed("PAGE cg_scope_page", tag="PAGE"):
            df_scope = db.get_cg_scope_page(
                semana_inicio=semana_inicio,
                gestor=gestor_sel,
                cliente=cliente_sel,
                alerta=alerta_sel,
                limit=25,
                offset=scope_offset,
            )

        st.markdown("#### Cumplimiento semanal")
        _cg_render_pager("page_cg_scope", _df_total_rows(df_scope), page_size=25)
        df_scope_view = _rename_and_pick(
            df_scope,
            {
                "COD_RT": "COD_RT",
                "CLIENTE": "CLIENTE",
                "LOCAL": "LOCAL",
                "GESTOR": "GESTOR",
                "REPONEDOR_SCOPE": "REPONEDOR",
                "MODALIDAD": "MODALIDAD",
                "SEMANA_INICIO": "SEMANA",
                "VISITA": "VISITA",
                "VISITA_REALIZADA": "VISITA REALIZADA",
                "DIFERENCIA": "DIFERENCIA",
                "DIAS_DOBLE_MARCAJE": "DOBLE MARCAJE",
                "DIAS_KPIONE": "DÍAS KPIONE",
                "DIAS_POWER_APP": "DÍAS POWER APP",
                "PERSONA_CONFLICTO_ROWS": "CONFLICTOS PERSONA",
                "ALERTA": "ALERTA",
            },
            [
                "SEMANA",
                "COD_RT",
                "LOCAL",
                "CLIENTE",
                "GESTOR",
                "REPONEDOR",
                "MODALIDAD",
                "VISITA",
                "VISITA REALIZADA",
                "DIFERENCIA",
                "DOBLE MARCAJE",
                "DÍAS KPIONE",
                "DÍAS POWER APP",
                "CONFLICTOS PERSONA",
                "ALERTA",
            ],
        )
        st.dataframe(df_scope_view, width="stretch", hide_index=True)

        with st.expander("Alertas", expanded=False):
            _, offset_alertas = _cg_get_page_state("page_cg_alertas", page_size=25)
            df_alertas = db.get_cg_alertas_page(
                semana_inicio=semana_inicio,
                cliente=cliente_sel,
                persona=gestor_sel,
                limit=25,
                offset=offset_alertas,
            )
            _cg_render_pager("page_cg_alertas", _df_total_rows(df_alertas), page_size=25)
            df_alertas_view = _rename_and_pick(
                df_alertas,
                {
                    "alerta_tipo": "ALERTA",
                    "fuente": "FUENTE",
                    "cod_rt": "COD_RT",
                    "cliente": "CLIENTE",
                    "persona": "PERSONA",
                    "fecha_visita": "FECHA VISITA",
                    "detalle": "DETALLE",
                    "prioridad": "PRIORIDAD",
                },
                ["ALERTA", "FUENTE", "COD_RT", "CLIENTE", "PERSONA", "FECHA VISITA", "DETALLE", "PRIORIDAD"],
            )
            _cg_show_df("Alertas", df_alertas_view)

        with st.expander("Detalle evidencia", expanded=False):
            _, offset_det = _cg_get_page_state("page_cg_detalle", page_size=25)
            df_detalle = db.get_cg_detalle_page(
                semana_inicio=semana_inicio,
                gestor=gestor_sel,
                cliente=cliente_sel,
                limit=25,
                offset=offset_det,
            )
            _cg_render_pager("page_cg_detalle", _df_total_rows(df_detalle), page_size=25)
            df_detalle_view = _rename_and_pick(
                df_detalle,
                {
                    "fuente": "FUENTE",
                    "gestor": "GESTOR",
                    "cliente": "CLIENTE",
                    "cod_rt": "COD_RT",
                    "local_nombre": "LOCAL",
                    "reponedor": "REPONEDOR",
                    "modalidad": "MODALIDAD",
                    "fecha_visita": "FECHA VISITA",
                    "has_evidence": "EVIDENCIA",
                    "match_status": "MATCH",
                    "brecha_tipo": "BRECHA",
                    "match_quality": "CALIDAD MATCH",
                    "persona_match_exacta": "MATCH PERSONA",
                },
                [
                    "FUENTE",
                    "GESTOR",
                    "CLIENTE",
                    "COD_RT",
                    "LOCAL",
                    "REPONEDOR",
                    "MODALIDAD",
                    "FECHA VISITA",
                    "EVIDENCIA",
                    "MATCH",
                    "BRECHA",
                    "CALIDAD MATCH",
                    "MATCH PERSONA",
                ],
            )
            _cg_show_df("Detalle evidencia", df_detalle_view)

        with st.expander("Parity", expanded=False):
            _, offset_parity = _cg_get_page_state("page_cg_parity", page_size=25)
            df_parity = db.get_cg_parity_page(
                semana_inicio=semana_inicio,
                gestor=gestor_sel,
                cliente=cliente_sel,
                alerta=alerta_sel,
                limit=25,
                offset=offset_parity,
            )
            _cg_render_pager("page_cg_parity", _df_total_rows(df_parity), page_size=25)
            df_parity_view = _rename_and_pick(
                df_parity,
                {
                    "COD_RT": "COD_RT",
                    "CLIENTE": "CLIENTE",
                    "LOCAL": "LOCAL",
                    "GESTOR": "GESTOR",
                    "REPONEDOR_SCOPE": "REPONEDOR",
                    "MODALIDAD": "MODALIDAD",
                    "SEMANA_INICIO": "SEMANA",
                    "VISITA": "VISITA",
                    "VISITA_REALIZADA": "VISITA REALIZADA",
                    "DIFERENCIA": "DIFERENCIA",
                    "DIAS_DOBLE_MARCAJE": "DOBLE MARCAJE",
                    "DIAS_KPIONE": "DÍAS KPIONE",
                    "DIAS_POWER_APP": "DÍAS POWER APP",
                    "PERSONA_CONFLICTO_ROWS": "CONFLICTOS PERSONA",
                    "ALERTA": "ALERTA",
                },
                [
                    "SEMANA",
                    "COD_RT",
                    "LOCAL",
                    "CLIENTE",
                    "GESTOR",
                    "REPONEDOR",
                    "MODALIDAD",
                    "VISITA",
                    "VISITA REALIZADA",
                    "DIFERENCIA",
                    "DOBLE MARCAJE",
                    "DÍAS KPIONE",
                    "DÍAS POWER APP",
                    "CONFLICTOS PERSONA",
                    "ALERTA",
                ],
            )
            _cg_show_df("Cumplimiento parity", df_parity_view)

    except Exception as e:
        _dbg("FAIL control_gestion_branch", err=repr(e), role=role_sel, module=module_sel)
        st.error("No pude leer una o más superficies del contrato B3.")
        with st.expander("Detalles técnicos"):
            st.code(repr(e))
            if DEBUG:
                st.code(traceback.format_exc())
        st.stop()

    st.info("Supervisión queda reservada fuera del contrato público B3 actual.")
    st.stop()
