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


def render_control_gestion(
    *,
    db,
    DEBUG,
    top2,
    top3,
    _dbg,
    _timed,
    _df_total_rows,
    _rename_and_pick
):
    cg_role_opts = ["JEFE OPERACIONES", "GESTOR"]
    cg_module_opts = ["Inicio", "Cumplimiento"]
    cg_all_token = "Todos"

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

    role_sel = (st.session_state.get("sel_cg_role") or "JEFE OPERACIONES").strip().upper()
    module_sel = (st.session_state.get("sel_cg_module") or "Inicio").strip()

    try:
        smoke = db.get_cg_contract_smoke()
    except Exception as e:
        _dbg("FAIL cg_contract_smoke", err=repr(e))
        st.error("No pude validar el contrato B3 en db.py.")
        with st.expander("Detalles técnicos"):
            st.code(repr(e))
            if DEBUG:
                st.code(traceback.format_exc())
        st.stop()

    smoke_status = str(smoke.get("smoke_status") or "unknown").upper()
    st.caption(f"B3 contrato público | smoke={smoke_status} | rol={role_sel} | módulo={module_sel}")

    with st.expander("Estado contrato B3", expanded=(smoke_status != "OK")):
        smoke_rows = pd.DataFrame(smoke.get("results") or [])
        if smoke_rows.empty:
            st.warning("Smoke sin resultados. Revisa visibilidad de views public.v_cg_*.")
        else:
            st.dataframe(smoke_rows, width="stretch", hide_index=True)

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
