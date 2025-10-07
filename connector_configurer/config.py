import json
import argparse
import requests
import time
import sys
import os
from pathlib import Path


url = os.getenv("CONNECT_URL", "http://connect:8083/connectors")


def create_or_update_connector(config_path: Path):
    """Wysyła konfigurację connectora do Debezium Connect"""
    if not config_path.exists():
        print(f"[error] Plik {config_path} nie istnieje.")
        sys.exit(1)

    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)

    connector_name = config.get("name")
    if not connector_name:
        print("[error] Brak pola 'name' w konfiguracji JSON.")
        sys.exit(1)

    print(f"[info] Tworzenie / aktualizacja connectora '{connector_name}'...")

    # Wysyłamy PUT (upsert) – działa zarówno przy tworzeniu, jak i aktualizacji connectora
    resp = requests.put(f"{url}/{connector_name}/config", json=config["config"])

    if resp.status_code in (200, 201):
        print(f"[ok] Connector '{connector_name}' został utworzony lub zaktualizowany.")
    else:
        print(f"[error] Nie udało się utworzyć connectora. Kod: {resp.status_code}")
        print(resp.text)
        sys.exit(1)

    # Poczekaj chwilę, aż connector się ustabilizuje
    time.sleep(3)
    check_connector_status(connector_name)


def check_connector_status(connector_name: str):
    """Sprawdza status connectora"""
    status_url = f"{url}/{connector_name}/status"
    try:
        resp = requests.get(status_url)
        if resp.status_code != 200:
            print(f"[warn] Nie udało się pobrać statusu connectora ({resp.status_code})")
            return

        status_data = resp.json()
        connector_state = status_data.get("connector", {}).get("state")
        tasks = status_data.get("tasks", [])

        print(f"[status] Connector: {connector_state}")
        for task in tasks:
            print(f"  - Task {task['id']}: {task['state']}")

        if connector_state == "RUNNING" and all(t["state"] == "RUNNING" for t in tasks):
            print(f"[done] Connector '{connector_name}' działa poprawnie ✅")
        else:
            print(f"[warn] Connector '{connector_name}' nie jest w pełni aktywny ⚠️")

    except requests.RequestException as e:
        print(f"[error] Błąd przy sprawdzaniu statusu: {e}")


def main():
    parser = argparse.ArgumentParser(description="Tworzy connector Debezium z pliku JSON")
    parser.add_argument("--config-file", required=True, help="Ścieżka do pliku konfiguracyjnego JSON")
    args = parser.parse_args()

    config_path = Path(args.config_file)
    create_or_update_connector(config_path)


if __name__ == "__main__":
    main()