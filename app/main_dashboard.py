import sys
from pathlib import Path

import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.bitola.bitola_dashboard import get_dashboard_config as get_bitola_config
from app.dashboard import render_dashboard
from app.skopje.skopje_dashboard import get_dashboard_config as get_skopje_config


CITY_CONFIGS = {
    "Bitola": get_bitola_config,
    "Skopje": get_skopje_config,
}


def main():
    st.set_page_config(page_title="Air Quality Dashboard", layout="wide")

    selected_city = st.sidebar.selectbox(
        "City",
        options=list(CITY_CONFIGS),
        index=0,
    )

    config = CITY_CONFIGS[selected_city]()
    render_dashboard(config, configure_page=False)


if __name__ == "__main__":
    main()
