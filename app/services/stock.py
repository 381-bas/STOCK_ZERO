from __future__ import annotations

from app import db


def _validate_mode(modo: str) -> str:
    mode = str(modo or "").strip().upper()
    if mode not in {"LOCAL", "MERCADERISTA"}:
        raise ValueError(f"Modo no soportado para reposicion: {modo!r}. Usa 'LOCAL' o 'MERCADERISTA'.")
    return mode


def get_local_selector_data():
    return db.get_locales_home()


def get_clientes_local_home(cod_rt):
    return db.get_clientes_local_home(cod_rt)


def get_clientes_local_mercaderista(cod_rt, modalidad, rutero, reponedor):
    return db.get_clientes_local_mercaderista(
        cod_rt,
        modalidad,
        rutero,
        reponedor,
    )


def get_mercaderista_modalidades():
    return db.get_modalidades_home()


def get_mercaderista_selector_data(modalidad):
    return db.get_rutero_reponedor_por_modalidad(modalidad)


def get_locales_por_modalidad_rr(modalidad, rutero, reponedor):
    return db.get_locales_por_modalidad_rr(modalidad, rutero, reponedor)


def get_local_context(modo, cod_rt, rutero=None, reponedor=None, modalidad=None):
    mode = _validate_mode(modo)
    if mode == "LOCAL":
        return db.get_contexto_local_home(cod_rt)
    return db.get_contexto_local(
        rutero=rutero,
        reponedor=reponedor,
        cod_rt=cod_rt,
        modalidad=modalidad,
    )


def get_brand_options(modo, cod_rt, rutero=None, reponedor=None, modalidad=None):
    mode = _validate_mode(modo)
    if mode == "LOCAL":
        return db.get_marcas_local(cod_rt)
    return db.get_marcas(
        rutero=rutero,
        reponedor=reponedor,
        cod_rt=cod_rt,
        modalidad=modalidad,
    )


def get_data_version():
    return db.get_data_version()


def get_kpis(modo, cod_rt, marcas=None, rutero=None, reponedor=None, modalidad=None, cliente=None):
    mode = _validate_mode(modo)
    if mode == "LOCAL":
        return db.get_kpis_local_home(cod_rt, marcas, cliente=cliente)
    return db.get_kpis_local(
        rutero=rutero,
        reponedor=reponedor,
        cod_rt=cod_rt,
        marcas=marcas,
        modalidad=modalidad,
        cliente=cliente,
    )


def get_total_rows(
    modo,
    cod_rt,
    marcas=None,
    foco=None,
    search="",
    rutero=None,
    reponedor=None,
    modalidad=None,
    cliente=None,
):
    mode = _validate_mode(modo)
    if mode == "LOCAL":
        return db.get_tabla_ux_total_home(
            cod_rt=cod_rt,
            marcas=marcas,
            foco=foco,
            search=search,
            cliente=cliente,
        )
    return db.get_tabla_ux_total(
        rutero=rutero,
        reponedor=reponedor,
        cod_rt=cod_rt,
        marcas=marcas,
        foco=foco,
        search=search,
        modalidad=modalidad,
        cliente=cliente,
    )


def get_page(
    modo,
    cod_rt,
    marcas=None,
    foco=None,
    search="",
    page=1,
    page_size=100,
    rutero=None,
    reponedor=None,
    modalidad=None,
    cliente=None,
):
    mode = _validate_mode(modo)
    if mode == "LOCAL":
        return db.get_tabla_ux_page_home(
            cod_rt=cod_rt,
            marcas=marcas,
            page=page,
            page_size=page_size,
            foco=foco,
            search=search,
            cliente=cliente,
        )
    return db.get_tabla_ux_page(
        rutero=rutero,
        reponedor=reponedor,
        cod_rt=cod_rt,
        marcas=marcas,
        page=page,
        page_size=page_size,
        foco=foco,
        search=search,
        modalidad=modalidad,
        cliente=cliente,
    )


def get_export_raw(
    modo,
    cod_rt,
    marcas=None,
    foco=None,
    search="",
    rutero=None,
    reponedor=None,
    modalidad=None,
    cliente=None,
):
    mode = _validate_mode(modo)
    if mode == "LOCAL":
        return db.get_tabla_ux_export_home(
            cod_rt=cod_rt,
            marcas=marcas,
            foco=foco,
            search=search,
            cliente=cliente,
        )
    return db.get_tabla_ux_export(
        rutero=rutero,
        reponedor=reponedor,
        cod_rt=cod_rt,
        marcas=marcas,
        foco=foco,
        search=search,
        modalidad=modalidad,
        cliente=cliente,
    )


def get_export_inventario_local(cod_rt, cliente=None):
    return db.get_export_inventario_local(cod_rt, cliente=cliente)


def get_export_inventario_mercaderista_local(cod_rt, modalidad, rutero, reponedor, cliente=None):
    return db.get_export_inventario_mercaderista_local(
        cod_rt,
        modalidad,
        rutero,
        reponedor,
        cliente=cliente,
    )
