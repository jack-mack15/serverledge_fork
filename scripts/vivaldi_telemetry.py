import csv
from datetime import datetime
import os
import re
import socket
import time

# Configurazione dinamica
HOSTNAME = socket.gethostname()
CSV_FILE = f"log_{HOSTNAME}.csv"
SERVERLEDGE_LOG_FILE = "/home/ubuntu/serverledge-tesi/serverledge.log"

# Regex flessibile: gestisce spazi opzionali (\s*) e notazione scientifica/esponenziale ([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)
FLOAT_PATTERN = r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?"
LOG_PATTERN = re.compile(
    r"node:\s*(?P<node>[^;]+);\s*"
    r"count:\s*(?P<count>\d+);\s*"
    rf"X:\s*(?P<x>{FLOAT_PATTERN});\s*"
    rf"Y:\s*(?P<y>{FLOAT_PATTERN});\s*"
    rf"Z:\s*(?P<z>{FLOAT_PATTERN});\s*"
    rf"Adj:\s*(?P<adj>{FLOAT_PATTERN});\s*"
    rf"Height:\s*(?P<height>{FLOAT_PATTERN})"
)

def main():
    print(f"Avvio telemetria su {HOSTNAME}.")
    print(f"Lettura da '{SERVERLEDGE_LOG_FILE}', salvataggio su '{CSV_FILE}'...")

    # Creazione header CSV se il file non esiste
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "node_id", "name", "counter", "x", "y", "z", "adjustment", "height"])
            f.flush()

    seen_entries = set()

    # 1. Attesa attiva: se il log non esiste ancora, aspetta che Serverledge lo crei
    while not os.path.exists(SERVERLEDGE_LOG_FILE):
        print(f"Attesa creazione di '{SERVERLEDGE_LOG_FILE}'...")
        time.sleep(1)

    print("File log trovato. Inizio monitoraggio continuo...")

    # 2. Ciclo di lettura continuo (tail -f like)
    try:
        with open(SERVERLEDGE_LOG_FILE, "r") as log_file:
            # Opzionale: se vuoi leggere tutto dall'inizio NON fare seek(0, os.SEEK_END)
            while True:
                line = log_file.readline()

                if not line:
                    time.sleep(0.5)
                    continue

                match = LOG_PATTERN.search(line)
                if match:
                    data = match.groupdict()
                    entry_key = data["count"]

                    if entry_key not in seen_entries:
                        seen_entries.add(entry_key)
                        current_time = datetime.now().isoformat()

                        with open(CSV_FILE, mode="a", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerow([
                                current_time,
                                HOSTNAME,
                                data["node"].strip(),
                                data["count"],
                                data["x"],
                                data["y"],
                                data["z"],
                                data["adj"],
                                data["height"]
                            ])
                            f.flush()  # Assicura persistenza immediata su disco

    except KeyboardInterrupt:
        print(f"\nTelemetria interrotta dall'utente. Dati salvati in {CSV_FILE}.")

if __name__ == "__main__":
    main()