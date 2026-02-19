# streamlit_app.py
# Entry-point para Streamlit Cloud (main module).
# La app real vive en app/Home.py

import traceback
import streamlit as st

try:
    print("BOOT: import app.Home")
    import app.Home  # noqa: F401  (ejecuta la UI por side-effect)
    print("BOOT: app.Home OK")
except Exception:
    st.error("Error al iniciar STOCK_ZERO.")
    st.code(traceback.format_exc())
    raise