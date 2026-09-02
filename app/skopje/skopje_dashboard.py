import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from app.dashboard import DashboardConfig, render_dashboard


SKOPJE_DIR = Path(os.getenv("SKOPJE_PROJECT_DIR", str(BASE_DIR / "skopje"))).expanduser()


def get_dashboard_config():
    return DashboardConfig(
        city="Skopje",
        db_path=Path(os.getenv("PROJECT_DB_PATH", str(SKOPJE_DIR / "data" / "skopje.db"))).expanduser(),
        page_title="Skopje Air Quality Dashboard",
        online_model_versions=(
            (
                "Fine-tuned",
                (
                    "chronos2_pm10_skopje_fine_tuned_24h",
                    "chronos2_pm25_skopje_fine_tuned_24h",
                ),
            ),
            (
                "Zero-shot",
                (
                    "chronos2_pm10_skopje_zero_shot_24h",
                    "chronos2_pm25_skopje_zero_shot_24h",
                ),
            ),
        ),
    )


def main():
    render_dashboard(get_dashboard_config())


if __name__ == "__main__":
    main()
