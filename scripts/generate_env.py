"""Genera un archivo .env local con contraseñas aleatorias y seguras."""

from pathlib import Path
from secrets import token_urlsafe

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / ".env.example"
TARGET = ROOT / ".env"


def main() -> None:
    if TARGET.exists():
        raise SystemExit(".env ya existe. No se sobrescribió el archivo.")

    content = SOURCE.read_text(encoding="utf-8")
    content = content.replace("REPLACE_WITH_A_SECURE_PASSWORD", token_urlsafe(24))
    content = content.replace(
        "REPLACE_WITH_A_DIFFERENT_SECURE_PASSWORD", token_urlsafe(24)
    )
    TARGET.write_text(content, encoding="utf-8")
    print("Archivo .env creado con contraseñas aleatorias.")
    print("Consulte AIRFLOW_PASSWORD y GRAFANA_PASSWORD dentro de .env.")


if __name__ == "__main__":
    main()
