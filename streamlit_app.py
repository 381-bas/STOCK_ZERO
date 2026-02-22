# streamlit_app.py
# Entry-point para Streamlit Cloud (main module).

import traceback
import streamlit as st

try:
    from app.Home import main
    main()  # <-- se ejecuta en CADA rerun
except Exception:
    st.error("Error al iniciar STOCK_ZERO.")
    st.code(traceback.format_exc())
    raise