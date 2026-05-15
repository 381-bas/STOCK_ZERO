import math
import traceback

import pandas as pd
import streamlit as st


CLIENTE_SCOPE_PLACEHOLDER = "Todos"
RESP_TIPO_PLACEHOLDER = "— Selecciona tipo —"
RESP_TIPO_ALL = "Todos"
RESP_LISTA_PLACEHOLDER = "Todos"


def _scope_value_or_none(value: str | None, all_token: str = "Todos") -> str | None:
    if value is None:
        return None
    v = str(value).strip()
    if not v:
        return None
    if v.upper() == str(all_token).strip().upper():
        return None
    if v.upper() == "— SELECCIONA TIPO —":
        return None
    return v


def _reset_cliente_state(
    reason: str,
    *,
    _dbg,
    _dbg_block,
    reset_cliente: bool = True,
    reset_tipo: bool = True,
    reset_responsable: bool = True,
) -> None:
    if reset_cliente:
        st.session_state["sel_cliente_home_scope"] = CLIENTE_SCOPE_PLACEHOLDER
    if reset_tipo:
        st.session_state["sel_responsable_tipo"] = RESP_TIPO_PLACEHOLDER
    if reset_responsable:
        st.session_state["sel_responsable_lista"] = RESP_LISTA_PLACEHOLDER

    st.session_state["scope_level_cliente_mode"] = "L0"
    st.session_state["page_cliente_scope"] = 1
    st.session_state["page_cliente_scope_ui"] = 1
    st.session_state["page_cliente_detalle"] = 1
    st.session_state["page_cliente_detalle_ui"] = 1
    _dbg(f"RESET cliente_state ({reason})")
    _dbg_block()


def _reset_on_cliente_change(*, _reset_filters_defaults, _dbg, _dbg_block) -> None:
    _reset_cliente_state(
        "cliente_change",
        _dbg=_dbg,
        _dbg_block=_dbg_block,
        reset_cliente=False,
        reset_tipo=True,
        reset_responsable=True,
    )
    _reset_filters_defaults("cliente_change")


def _reset_on_responsable_tipo_change(*, _reset_filters_defaults, _dbg, _dbg_block) -> None:
    _reset_cliente_state(
        "responsable_tipo_change",
        _dbg=_dbg,
        _dbg_block=_dbg_block,
        reset_cliente=False,
        reset_tipo=False,
        reset_responsable=True,
    )
    _reset_filters_defaults("responsable_tipo_change")


def _reset_on_responsable_lista_change(*, _reset_filters_defaults, _dbg, _dbg_block) -> None:
    _reset_cliente_state(
        "responsable_lista_change",
        _dbg=_dbg,
        _dbg_block=_dbg_block,
        reset_cliente=False,
        reset_tipo=False,
        reset_responsable=False,
    )
    _reset_filters_defaults("responsable_lista_change")


def _get_page_state(page_key: str, page_size: int = 25) -> tuple[int, int]:
    input_key = f"{page_key}_ui"

    st.session_state.setdefault(page_key, 1)
    st.session_state.setdefault(input_key, 1)

    try:
        current_page = int(st.session_state.get(page_key, 1))
    except Exception:
        current_page = 1

    current_page = max(1, current_page)
    st.session_state[page_key] = current_page

    if int(st.session_state.get(input_key, current_page)) != current_page:
        st.session_state[input_key] = current_page

    return current_page, (current_page - 1) * page_size


def _render_scope_pager(page_key: str, total_rows: int, page_size: int = 25) -> None:
    input_key = f"{page_key}_ui"
    total_pages = max(1, int(math.ceil(total_rows / page_size))) if total_rows > 0 else 1

    if int(st.session_state.get(page_key, 1)) > total_pages:
        st.session_state[page_key] = total_pages
    if int(st.session_state.get(page_key, 1)) < 1:
        st.session_state[page_key] = 1

    current_page = int(st.session_state.get(page_key, 1))
    if int(st.session_state.get(input_key, current_page)) != current_page:
        st.session_state[input_key] = current_page

    def _prev():
        st.session_state[page_key] = max(1, int(st.session_state.get(page_key, 1)) - 1)
        st.session_state[input_key] = st.session_state[page_key]

    def _next():
        st.session_state[page_key] = min(total_pages, int(st.session_state.get(page_key, 1)) + 1)
        st.session_state[input_key] = st.session_state[page_key]

    def _from_input():
        try:
            new_page = int(st.session_state.get(input_key, 1))
        except Exception:
            new_page = 1
        new_page = max(1, min(total_pages, new_page))
        st.session_state[page_key] = new_page
        st.session_state[input_key] = new_page

    if total_rows:
        start = (current_page - 1) * page_size + 1
        end = min(current_page * page_size, total_rows)
        pager_text = f"{start}-{end} de {total_rows} registros"
    else:
        pager_text = "Sin filas para el filtro aplicado."

    p1, p2, p3, p4 = st.columns([0.9, 1.3, 0.9, 2.2], gap="small")

    with p1:
        st.button(
            "◀",
            key=f"{page_key}_prev_btn",
            disabled=(current_page <= 1),
            on_click=_prev,
            use_container_width=True,
        )

    with p2:
        st.number_input(
            "Página",
            min_value=1,
            max_value=max(1, total_pages),
            step=1,
            key=input_key,
            on_change=_from_input,
            label_visibility="collapsed",
        )

    with p3:
        st.button(
            "▶",
            key=f"{page_key}_next_btn",
            disabled=(current_page >= total_pages),
            on_click=_next,
            use_container_width=True,
        )

    with p4:
        st.caption(pager_text)


def _render_rr_people_context(
    *,
    db,
    scope_level,
    responsable_tipo_sel,
    responsable_sel,
    marca_ap,
    cliente_sel,
    _dbg,
    _dbg_block,
    _timed,
) -> None:
    if scope_level not in {"L2", "L3"}:
        return
    if not responsable_tipo_sel or not responsable_sel:
        return

    try:
        with _timed("PAGE rr_people_scope", tag="PAGE"):
            rr_people = db.get_rr_people_scope(
                tipo=responsable_tipo_sel,
                responsable=responsable_sel,
                marca=marca_ap,
                cliente=cliente_sel,
            )
            _dbg("CLIENTE rr_people loaded", rows=0 if rr_people is None else len(rr_people))
            _dbg_block()
    except Exception as e:
        _dbg("FAIL rr_people_scope", err=repr(e))
        st.warning("No pude cargar MERCADERISTA A CARGO para este scope.")
        return

    if rr_people is None or rr_people.empty:
        return

    rr_view = rr_people.rename(
        columns={
            "rutero": "RUTERO",
            "reponedor": "REPONEDOR",
            "responsable": "RESPONSABLE",
            "cliente": "CLIENTE",
            "locales_relacionados": "LOCALES RELACIONADOS",
        }
    )

    with st.expander(f"MERCADERISTA A CARGO ({len(rr_view)} filas)", expanded=False):
        st.dataframe(rr_view, width="stretch", hide_index=True)


def _sanitize_export_token(value, fallback: str = "TODOS") -> str:
    raw = str(value or "").strip()
    if not raw:
        return fallback
    token = "".join(ch if str(ch).isalnum() else "_" for ch in raw)
    token = token.strip("_")
    return token[:60] or fallback


def _render_cliente_inventory_export(
    *,
    db,
    cliente_sel,
    responsable_tipo_all,
    responsable_tipo_sel,
    responsable_sel,
    file_stamp,
    scope_level,
    _dbg,
    _dbg_block,
    _timed,
    build_inventory_cliente_export_df,
    export_excel_generic,
) -> None:
    try:
        with _timed("EXPORT cliente_scope_inventory_query", tag="CACHE"):
            df_cliente_raw = db.get_export_inventario_cliente(
                cliente=cliente_sel,
                marca=None,
                responsable_tipo=None if responsable_tipo_all else responsable_tipo_sel,
                responsable=responsable_sel,
                focos=None,
                search="",
            )
        _dbg(
            "CLIENTE scope inventory export raw loaded",
            rows=0 if df_cliente_raw is None else len(df_cliente_raw),
        )
        _dbg_block()
    except Exception as e:
        _dbg("FAIL cliente_scope_inventory_export", err=repr(e))
        st.warning("No pude preparar los descargables globales del cliente.")
        return

    if df_cliente_raw is None or df_cliente_raw.empty:
        st.caption("Sin filas para exportar en el cliente seleccionado.")
        return

    df_inventory_cliente = build_inventory_cliente_export_df(df_cliente_raw)
    cliente_token = _sanitize_export_token(cliente_sel, fallback="TODOS")
    resp_tipo_token = _sanitize_export_token(
        None if responsable_tipo_all else responsable_tipo_sel,
        fallback="TODOS",
    )
    responsable_token = _sanitize_export_token(responsable_sel, fallback="TODOS")
    inventory_excel = export_excel_generic(
        f"CLIENTE_{cliente_token}_{resp_tipo_token}_{responsable_token}",
        df_inventory_cliente,
    )
    st.download_button(
        "Descargar inventario",
        data=inventory_excel,
        file_name=(
            f"STOCK_ZERO_INVENTARIO_CLIENTE_"
            f"{cliente_token}_{resp_tipo_token}_{responsable_token}_{file_stamp}.xlsx"
        ),
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        key=f"cliente_inventory_excel_{scope_level}_{cliente_token}_{resp_tipo_token}_{responsable_token}",
    )


def _render_responsables_scope_table(
    df_resp: pd.DataFrame | None,
    title: str,
    *,
    _rename_and_pick,
) -> None:
    st.markdown(f"#### {title}")
    df_resp_view = _rename_and_pick(
        df_resp,
        {
            "responsable_tipo": "RESP. TIPO",
            "responsable": "RESPONSABLE",
            "clientes": "CLIENTES",
            "locales": "LOCALES",
            "skus_scope": "TOTAL SKUS",
            "total_skus": "TOTAL SKUS",
            "venta_0": "VENTA 0",
            "negativos": "NEGATIVOS",
            "quiebres": "QUIEBRES OBS.",
            "otros": "OTROS",
            "skus_en_foco": "SKUS EN FOCO",
        },
        [
            "RESP. TIPO",
            "RESPONSABLE",
            "CLIENTES",
            "LOCALES",
            "TOTAL SKUS",
            "VENTA 0",
            "NEGATIVOS",
            "OTROS",
            "QUIEBRES OBS.",
            "SKUS EN FOCO",
        ],
    )
    st.dataframe(df_resp_view, width="stretch", hide_index=True)


def render_cliente(
    *,
    db,
    DEBUG,
    top2,
    top3,
    _dbg,
    _dbg_block,
    _timed,
    _render_kpi_cards,
    _invalidate_runtime_cache,
    _reset_filters_defaults,
    _df_total_rows,
    _drop_total_rows,
    _rename_and_pick,
    build_inventory_cliente_export_df,
    export_excel_generic,
):
    _ = _invalidate_runtime_cache

    try:
        _dbg("CLIENTE marcas selector skipped")
        _dbg_block()
    except Exception as e:
        _dbg("FAIL marcas_home_global", err=repr(e))
        st.error("No pude leer marcas para CLIENTE.")
        with st.expander("Detalles técnicos"):
            st.code(repr(e))
            if DEBUG:
                st.code(traceback.format_exc())
        st.stop()

    try:
        with _timed("SELECTOR clientes_home_scope", tag="SELECTOR"):
            clientes_scope = db.get_clientes_home_scope()
            _dbg("CLIENTE clientes loaded", rows=len(clientes_scope))
            _dbg_block()
    except Exception as e:
        _dbg("FAIL clientes_home_scope", err=repr(e))
        st.error("No pude leer clientes para el scope seleccionado.")
        with st.expander("Detalles técnicos"):
            st.code(repr(e))
            if DEBUG:
                st.code(traceback.format_exc())
        st.stop()

    cliente_opts = [CLIENTE_SCOPE_PLACEHOLDER] + list(clientes_scope or [])
    if st.session_state.get("sel_cliente_home_scope", CLIENTE_SCOPE_PLACEHOLDER) not in cliente_opts:
        st.session_state["sel_cliente_home_scope"] = CLIENTE_SCOPE_PLACEHOLDER

    with top2:
        st.selectbox(
            "CLIENTE",
            cliente_opts,
            key="sel_cliente_home_scope",
            on_change=_reset_on_cliente_change,
            kwargs={
                "_reset_filters_defaults": _reset_filters_defaults,
                "_dbg": _dbg,
                "_dbg_block": _dbg_block,
            },
        )

    cliente_sel = _scope_value_or_none(
        st.session_state.get("sel_cliente_home_scope"),
        CLIENTE_SCOPE_PLACEHOLDER,
    )

    with top3:
        st.selectbox(
            "RESPONSABLE TIPO",
            [RESP_TIPO_PLACEHOLDER, RESP_TIPO_ALL, "GESTOR", "SUPERVISOR"],
            key="sel_responsable_tipo",
            on_change=_reset_on_responsable_tipo_change,
            kwargs={
                "_reset_filters_defaults": _reset_filters_defaults,
                "_dbg": _dbg,
                "_dbg_block": _dbg_block,
            },
        )

    responsable_tipo_raw = str(st.session_state.get("sel_responsable_tipo") or "").strip()
    responsable_tipo_all = responsable_tipo_raw.upper() == RESP_TIPO_ALL.upper()
    responsable_tipo_sel = (
        None
        if responsable_tipo_raw in {"", RESP_TIPO_PLACEHOLDER, RESP_TIPO_ALL}
        else responsable_tipo_raw
    )
    kpi_only_tipo_scope = responsable_tipo_raw in {"", RESP_TIPO_PLACEHOLDER, RESP_TIPO_ALL}

    if responsable_tipo_sel:
        try:
            with _timed("SELECTOR responsables_home_scope", tag="SELECTOR"):
                responsables_scope = db.get_responsables_home_scope(
                    tipo=responsable_tipo_sel,
                    cliente=cliente_sel,
                )
                _dbg(
                    "CLIENTE responsables loaded",
                    rows=len(responsables_scope),
                    tipo=responsable_tipo_sel,
                    cliente=cliente_sel or "Todos",
                )
                _dbg_block()
        except Exception as e:
            _dbg("FAIL responsables_home_scope", err=repr(e))
            st.error("No pude leer responsables para el scope seleccionado.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()
    else:
        responsables_scope = []

    responsable_opts = [RESP_LISTA_PLACEHOLDER] + list(responsables_scope or [])
    if st.session_state.get("sel_responsable_lista", RESP_LISTA_PLACEHOLDER) not in responsable_opts:
        st.session_state["sel_responsable_lista"] = RESP_LISTA_PLACEHOLDER

    row2c1, row2c2 = st.columns([2.0, 1.0], gap="small")

    with row2c1:
        if responsable_tipo_sel:
            st.selectbox(
                "RESPONSABLE",
                responsable_opts,
                key="sel_responsable_lista",
                on_change=_reset_on_responsable_lista_change,
                kwargs={
                    "_reset_filters_defaults": _reset_filters_defaults,
                    "_dbg": _dbg,
                    "_dbg_block": _dbg_block,
                },
            )
        else:
            st.session_state["sel_responsable_lista"] = RESP_LISTA_PLACEHOLDER

    responsable_sel = _scope_value_or_none(
        st.session_state.get("sel_responsable_lista"),
        RESP_LISTA_PLACEHOLDER,
    )

    if not clientes_scope:
        st.warning("No hay clientes operativos disponibles en el scope actual.")
    if responsable_tipo_sel and not responsables_scope:
        st.warning("No hay responsables para el tipo y scope seleccionados.")

    scope_level = db.get_scope_level(
        cliente=cliente_sel,
        responsable=responsable_sel,
    )
    scope_label = scope_level if responsable_tipo_all else ("KPI" if kpi_only_tipo_scope else scope_level)
    st.session_state["scope_level_cliente_mode"] = scope_level

    marca_ap = None
    foco_ap = []
    search_ap = ""

    meta1, meta2, meta3, meta4, meta5 = st.columns([1.0, 1.3, 1.3, 1.2, 1.2], gap="small")
    meta1.caption(f"Scope: {scope_label}")
    meta2.caption(f"Cliente: {cliente_sel or 'Todos'}")
    meta3.caption(f"Resp. tipo: {responsable_tipo_raw or '-'}")
    meta4.caption(f"Responsable: {responsable_sel or 'Todos'}")
    meta5.caption(f"Marca filtro: {marca_ap or 'Todas'}")

    try:
        with _timed("PAGE kpis_scope_cliente", tag="PAGE"):
            kpis_scope = db.get_kpis_scope_cliente(
                marca=marca_ap,
                cliente=cliente_sel,
                responsable_tipo=responsable_tipo_sel,
                responsable=responsable_sel,
                focos=foco_ap,
                search=search_ap,
            )
            _dbg("CLIENTE KPIS loaded", rows=0 if kpis_scope is None else len(kpis_scope))
            _dbg_block()
    except Exception as e:
        _dbg("FAIL kpis_scope_cliente", err=repr(e))
        st.error("No pude leer KPIs del scope CLIENTE.")
        with st.expander("Detalles técnicos"):
            st.code(repr(e))
            if DEBUG:
                st.code(traceback.format_exc())
        st.stop()

    kpi_scope_row = dict(kpis_scope.iloc[0]) if (kpis_scope is not None and not kpis_scope.empty) else {}
    _render_kpi_cards(kpi_scope_row)

    fecha_stock_raw = (kpi_scope_row or {}).get("fecha_stock")
    fecha_stock_dt = pd.to_datetime(fecha_stock_raw, errors="coerce")
    file_stamp = fecha_stock_dt.strftime("%Y-%m-%d") if pd.notna(fecha_stock_dt) else "Sin stock"

    c1, c2, c3, c4, c5 = st.columns([1.1, 1.1, 1.1, 1.1, 1.1], gap="small")
    c1.caption(f"Fecha stock: {file_stamp}")
    c2.caption(f"Locales: {int((kpi_scope_row or {}).get('locales_scope') or 0)}")
    c3.caption(f"Clientes: {int((kpi_scope_row or {}).get('clientes_scope') or 0)}")
    c4.caption(f"Responsables: {int((kpi_scope_row or {}).get('responsables_scope') or 0)}")
    skus_scope_total = int(
        (kpi_scope_row or {}).get("total_skus")
        or (kpi_scope_row or {}).get("skus_scope")
        or 0
    )
    c5.caption(f"SKUs scope: {skus_scope_total}")
    total_skus_scope = skus_scope_total
    total_skus_scope = int((kpi_scope_row or {}).get("total_skus") or 0)
    if total_skus_scope == 0:
        st.warning("El scope operativo existe, pero no cruza con stock/fact para la selección actual.")

    if kpi_only_tipo_scope and not responsable_tipo_all:
        st.caption("Selecciona Todos, GESTOR o SUPERVISOR para habilitar la lectura del scope.")
        st.stop()

    if scope_level == "L0":
        try:
            if responsable_tipo_all:
                with _timed("PAGE tabla_scope_responsable_gestor_L0", tag="PAGE"):
                    df_resp_gestor = db.get_tabla_scope_responsable_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo="GESTOR",
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=500,
                        offset=0,
                    )

                with _timed("PAGE tabla_scope_responsable_supervisor_L0", tag="PAGE"):
                    df_resp_supervisor = db.get_tabla_scope_responsable_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo="SUPERVISOR",
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=500,
                        offset=0,
                    )
            else:
                with _timed("PAGE tabla_scope_responsable_L0", tag="PAGE"):
                    df_resp = db.get_tabla_scope_responsable_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=500,
                        offset=0,
                    )

            with _timed("PAGE tabla_scope_cliente_L0", tag="PAGE"):
                df_cli = db.get_tabla_scope_cliente_total_page(
                    marca=marca_ap,
                    cliente=cliente_sel,
                    responsable_tipo=responsable_tipo_sel,
                    responsable=responsable_sel,
                    focos=foco_ap,
                    search=search_ap,
                    limit=500,
                    offset=0,
                )
        except Exception as e:
            _dbg("FAIL tablas_scope_L0", err=repr(e))
            st.error("No pude leer tablas agregadas del scope global.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

        if responsable_tipo_all:
            _render_responsables_scope_table(df_resp_gestor, "Ranking gestores", _rename_and_pick=_rename_and_pick)
            _render_responsables_scope_table(df_resp_supervisor, "Ranking supervisores", _rename_and_pick=_rename_and_pick)
        elif responsable_tipo_sel == "GESTOR":
            _render_responsables_scope_table(df_resp, "Ranking gestores", _rename_and_pick=_rename_and_pick)
        else:
            _render_responsables_scope_table(df_resp, "Ranking supervisores", _rename_and_pick=_rename_and_pick)

        st.markdown("#### Ranking focos por cliente")
        df_cli_view = _rename_and_pick(
            df_cli,
            {
                "cliente": "CLIENTE",
                "responsables": "RESPONSABLES",
                "locales": "LOCALES",
                "skus_scope": "TOTAL SKUS",
                "total_skus": "TOTAL SKUS",
                "venta_0": "VENTA 0",
                "negativos": "NEGATIVOS",
                "quiebres": "QUIEBRES OBS.",
                "otros": "OTROS",
                "skus_en_foco": "SKUS EN FOCO",
            },
            [
                "CLIENTE",
                "RESPONSABLES",
                "LOCALES",
                "TOTAL SKUS",
                "VENTA 0",
                "NEGATIVOS",
                "OTROS",
                "QUIEBRES OBS.",
                "SKUS EN FOCO",
            ],
        )
        st.dataframe(df_cli_view, width="stretch", hide_index=True)
        _render_cliente_inventory_export(
            db=db,
            cliente_sel=cliente_sel,
            responsable_tipo_all=responsable_tipo_all,
            responsable_tipo_sel=responsable_tipo_sel,
            responsable_sel=responsable_sel,
            file_stamp=file_stamp,
            scope_level=scope_level,
            _dbg=_dbg,
            _dbg_block=_dbg_block,
            _timed=_timed,
            build_inventory_cliente_export_df=build_inventory_cliente_export_df,
            export_excel_generic=export_excel_generic,
        )

    elif scope_level == "L1":
        try:
            if responsable_tipo_all:
                with _timed("PAGE tabla_scope_responsable_gestor_L1", tag="PAGE"):
                    df_resp_gestor = db.get_tabla_scope_responsable_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo="GESTOR",
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=500,
                        offset=0,
                    )

                with _timed("PAGE tabla_scope_responsable_supervisor_L1", tag="PAGE"):
                    df_resp_supervisor = db.get_tabla_scope_responsable_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo="SUPERVISOR",
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=500,
                        offset=0,
                    )
            else:
                with _timed("PAGE tabla_scope_responsable_L1", tag="PAGE"):
                    df_resp = db.get_tabla_scope_responsable_total_page(
                        marca=marca_ap,
                        cliente=cliente_sel,
                        responsable_tipo=responsable_tipo_sel,
                        responsable=responsable_sel,
                        focos=foco_ap,
                        search=search_ap,
                        limit=500,
                        offset=0,
                    )

            _, offset = _get_page_state("page_cliente_scope", page_size=25)
            with _timed("PAGE tabla_scope_local_L1", tag="PAGE"):
                df_local = db.get_tabla_scope_local_total_page(
                    marca=marca_ap,
                    cliente=cliente_sel,
                    responsable_tipo=responsable_tipo_sel,
                    responsable=responsable_sel,
                    focos=foco_ap,
                    search=search_ap,
                    limit=25,
                    offset=offset,
                )
        except Exception as e:
            _dbg("FAIL tablas_scope_L1", err=repr(e))
            st.error("No pude leer la vista por cliente.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

        if responsable_tipo_all:
            _render_responsables_scope_table(df_resp_gestor, "Ranking gestores", _rename_and_pick=_rename_and_pick)
            _render_responsables_scope_table(df_resp_supervisor, "Ranking supervisores", _rename_and_pick=_rename_and_pick)
        elif responsable_tipo_sel == "GESTOR":
            _render_responsables_scope_table(df_resp, "Ranking gestores", _rename_and_pick=_rename_and_pick)
        else:
            _render_responsables_scope_table(df_resp, "Ranking supervisores", _rename_and_pick=_rename_and_pick)

        st.markdown("#### Locales del cliente")
        _render_scope_pager("page_cliente_scope", _df_total_rows(df_local), page_size=25)
        df_local_view = _rename_and_pick(
            df_local,
            {
                "cod_rt": "COD_RT",
                "local_nombre_rr": "LOCAL",
                "nombre_local_rr": "LOCAL",
                "cliente": "CLIENTE",
                "responsable_tipo": "RESP. TIPO",
                "responsable": "RESPONSABLE",
                "skus_scope": "TOTAL SKUS",
                "total_skus": "TOTAL SKUS",
                "venta_0": "VENTA 0",
                "negativos": "NEGATIVOS",
                "quiebres": "QUIEBRES OBS.",
                "otros": "OTROS",
                "skus_en_foco": "SKUS EN FOCO",
            },
            [
                "COD_RT",
                "LOCAL",
                "CLIENTE",
                "RESP. TIPO",
                "RESPONSABLE",
                "TOTAL SKUS",
                "VENTA 0",
                "NEGATIVOS",
                "OTROS",
                "QUIEBRES OBS.",
                "SKUS EN FOCO",
            ],
        )
        st.dataframe(df_local_view, width="stretch", hide_index=True)
        _render_cliente_inventory_export(
            db=db,
            cliente_sel=cliente_sel,
            responsable_tipo_all=responsable_tipo_all,
            responsable_tipo_sel=responsable_tipo_sel,
            responsable_sel=responsable_sel,
            file_stamp=file_stamp,
            scope_level=scope_level,
            _dbg=_dbg,
            _dbg_block=_dbg_block,
            _timed=_timed,
            build_inventory_cliente_export_df=build_inventory_cliente_export_df,
            export_excel_generic=export_excel_generic,
        )

    elif scope_level == "L2":
        try:
            with _timed("PAGE tabla_scope_cliente_L2", tag="PAGE"):
                df_cli = db.get_tabla_scope_cliente_total_page(
                    marca=marca_ap,
                    cliente=cliente_sel,
                    responsable_tipo=responsable_tipo_sel,
                    responsable=responsable_sel,
                    focos=foco_ap,
                    search=search_ap,
                    limit=500,
                    offset=0,
                )

            _, offset = _get_page_state("page_cliente_scope", page_size=25)
            with _timed("PAGE tabla_scope_local_L2", tag="PAGE"):
                df_local = db.get_tabla_scope_local_total_page(
                    marca=marca_ap,
                    cliente=cliente_sel,
                    responsable_tipo=responsable_tipo_sel,
                    responsable=responsable_sel,
                    focos=foco_ap,
                    search=search_ap,
                    limit=25,
                    offset=offset,
                )
        except Exception as e:
            _dbg("FAIL tablas_scope_L2", err=repr(e))
            st.error("No pude leer la vista por responsable.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

        st.markdown("#### Ranking focos por cliente")
        df_cli_view = _rename_and_pick(
            df_cli,
            {
                "cliente": "CLIENTE",
                "locales": "LOCALES",
                "skus_scope": "TOTAL SKUS",
                "total_skus": "TOTAL SKUS",
                "venta_0": "VENTA 0",
                "negativos": "NEGATIVOS",
                "quiebres": "QUIEBRES OBS.",
                "otros": "OTROS",
                "skus_en_foco": "SKUS EN FOCO",
            },
            [
                "CLIENTE",
                "LOCALES",
                "TOTAL SKUS",
                "VENTA 0",
                "NEGATIVOS",
                "OTROS",
                "QUIEBRES OBS.",
                "SKUS EN FOCO",
            ],
        )
        st.dataframe(df_cli_view, width="stretch", hide_index=True)

        st.markdown("#### Locales del responsable")
        _render_scope_pager("page_cliente_scope", _df_total_rows(df_local), page_size=25)
        df_local_view = _rename_and_pick(
            df_local,
            {
                "cod_rt": "COD_RT",
                "local_nombre_rr": "LOCAL",
                "nombre_local_rr": "LOCAL",
                "cliente": "CLIENTE",
                "rutero": "RUTERO",
                "reponedor": "REPONEDOR",
                "skus_scope": "TOTAL SKUS",
                "total_skus": "TOTAL SKUS",
                "venta_0": "VENTA 0",
                "negativos": "NEGATIVOS",
                "quiebres": "QUIEBRES OBS.",
                "otros": "OTROS",
                "skus_en_foco": "SKUS EN FOCO",
            },
            [
                "COD_RT",
                "LOCAL",
                "CLIENTE",
                "RUTERO",
                "REPONEDOR",
                "TOTAL SKUS",
                "VENTA 0",
                "NEGATIVOS",
                "OTROS",
                "QUIEBRES OBS.",
                "SKUS EN FOCO",
            ],
        )
        st.dataframe(df_local_view, width="stretch", hide_index=True)

        _render_cliente_inventory_export(
            db=db,
            cliente_sel=cliente_sel,
            responsable_tipo_all=responsable_tipo_all,
            responsable_tipo_sel=responsable_tipo_sel,
            responsable_sel=responsable_sel,
            file_stamp=file_stamp,
            scope_level=scope_level,
            _dbg=_dbg,
            _dbg_block=_dbg_block,
            _timed=_timed,
            build_inventory_cliente_export_df=build_inventory_cliente_export_df,
            export_excel_generic=export_excel_generic,
        )
        _render_rr_people_context(
            db=db,
            scope_level=scope_level,
            responsable_tipo_sel=responsable_tipo_sel,
            responsable_sel=responsable_sel,
            marca_ap=marca_ap,
            cliente_sel=cliente_sel,
            _dbg=_dbg,
            _dbg_block=_dbg_block,
            _timed=_timed,
        )

    else:
        try:
            _, offset = _get_page_state("page_cliente_scope", page_size=25)
            with _timed("PAGE tabla_scope_local_L3", tag="PAGE"):
                df_local = db.get_tabla_scope_local_total_page(
                    marca=marca_ap,
                    cliente=cliente_sel,
                    responsable_tipo=responsable_tipo_sel,
                    responsable=responsable_sel,
                    focos=foco_ap,
                    search=search_ap,
                    limit=25,
                    offset=offset,
                )

            _, offset_det = _get_page_state("page_cliente_detalle", page_size=25)
            with _timed("PAGE detalle_sku_scope_L3", tag="PAGE"):
                df_det = db.get_detalle_sku_scope_total_page(
                    marca=marca_ap,
                    cliente=cliente_sel,
                    responsable_tipo=responsable_tipo_sel,
                    responsable=responsable_sel,
                    focos=foco_ap,
                    search=search_ap,
                    limit=25,
                    offset=offset_det,
                )
        except Exception as e:
            _dbg("FAIL tablas_scope_L3", err=repr(e))
            st.error("No pude leer la vista detallada del scope.")
            with st.expander("Detalles técnicos"):
                st.code(repr(e))
                if DEBUG:
                    st.code(traceback.format_exc())
            st.stop()

        st.markdown("#### Locales accionables")
        _render_scope_pager("page_cliente_scope", _df_total_rows(df_local), page_size=25)
        df_local_view = _rename_and_pick(
            df_local,
            {
                "cod_rt": "COD_RT",
                "local_nombre_rr": "LOCAL",
                "nombre_local_rr": "LOCAL",
                "cliente": "CLIENTE",
                "rutero": "RUTERO",
                "reponedor": "REPONEDOR",
                "skus_scope": "TOTAL SKUS",
                "total_skus": "TOTAL SKUS",
                "venta_0": "VENTA 0",
                "negativos": "NEGATIVOS",
                "quiebres": "QUIEBRES OBS.",
                "otros": "OTROS",
                "skus_en_foco": "SKUS EN FOCO",
            },
            [
                "COD_RT",
                "LOCAL",
                "CLIENTE",
                "RUTERO",
                "REPONEDOR",
                "TOTAL SKUS",
                "VENTA 0",
                "NEGATIVOS",
                "OTROS",
                "QUIEBRES OBS.",
                "SKUS EN FOCO",
            ],
        )
        st.dataframe(df_local_view, width="stretch", hide_index=True)

        _render_cliente_inventory_export(
            db=db,
            cliente_sel=cliente_sel,
            responsable_tipo_all=responsable_tipo_all,
            responsable_tipo_sel=responsable_tipo_sel,
            responsable_sel=responsable_sel,
            file_stamp=file_stamp,
            scope_level=scope_level,
            _dbg=_dbg,
            _dbg_block=_dbg_block,
            _timed=_timed,
            build_inventory_cliente_export_df=build_inventory_cliente_export_df,
            export_excel_generic=export_excel_generic,
        )
        _render_rr_people_context(
            db=db,
            scope_level=scope_level,
            responsable_tipo_sel=responsable_tipo_sel,
            responsable_sel=responsable_sel,
            marca_ap=marca_ap,
            cliente_sel=cliente_sel,
            _dbg=_dbg,
            _dbg_block=_dbg_block,
            _timed=_timed,
        )

        st.markdown("#### Detalle SKU")
        _render_scope_pager("page_cliente_detalle", _df_total_rows(df_det), page_size=25)
        df_det_view = _drop_total_rows(df_det).rename(
            columns={
                "fecha": "FECHA STOCK",
                "cod_rt": "COD_RT",
                "local_nombre_rr": "LOCAL",
                "nombre_local_rr": "LOCAL",
                "cliente": "CLIENTE",
                "responsable_tipo": "RESP. TIPO",
                "responsable": "RESPONSABLE",
                "marca": "MARCA",
                "sku": "SKU",
                "producto": "PRODUCTO",
                "descripcion": "PRODUCTO",
                "stock": "STOCK",
                "venta_7": "VENTA(+7)",
                "negativo": "NEGATIVO",
                "riesgo_quiebre": "RIESGO DE QUIEBRE",
                "otros": "OTROS",
            }
        )
        st.dataframe(df_det_view, width="stretch", hide_index=True)

    st.stop()
