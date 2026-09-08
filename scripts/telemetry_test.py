import subprocess
import re
import csv
import time
import socket
from datetime import datetime

# Configurazione dinamica del nome file in base alla VM
HOSTNAME = socket.gethostname()
CSV_FILE = f"log_{HOSTNAME}.csv"
INTERVAL_SECONDS = 1  # Frequenza di polling dei log

# Regex aggiornata per catturare count, X, Y, Z, Adj e Height
LOG_PATTERN = re.compile(
    r"count:(?P<count>\d+);\s*"
    r"X:\s*(?P<x>-?[\d\.]+);\s*"
    r"Y:\s*(?P<y>-?[\d\.]+);\s*"
    r"Z:\s*(?P<z>-?[\d\.]+);\s*"
    r"Adj:\s*(?P<adj>-?[\d\.]+);\s*"
    r"Height:\s*(?P<height>-?[\d\.]+)"
)

def get_cluster_nodes():
    """Recupera l'elenco dei container worker attivi tramite Docker."""
    try:
        output = subprocess.check_output(
            ["docker", "ps", "--filter", "name=worker", "--format", "{{.Names}}"]
        )
        return output.decode("utf-8").splitlines()
    except Exception as e:
        print(f"Errore nel recupero dei nodi Docker: {e}")
        return []

def main():
    print(f"Avvio telemetria Vivaldi. Salvataggio su {CSV_FILE}...")
    
    # Inizializza il file CSV con l'intestazione completa
    with open(CSV_FILE, mode="w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "node_id", "counter", "x", "y", "z", "adjustment", "height"])

    seen_entries = set() # Per evitare di duplicare righe già lette

    try:
        while True:
            nodes = get_cluster_nodes()
            current_time = datetime.now().isoformat()

            for node in nodes:
                try:
                    # Legge gli ultimi log del container
                    logs = subprocess.check_output(
                        ["docker", "logs", "--tail", "5", node],
                        stderr=subprocess.STDOUT
                    ).decode("utf-8")

                    for line in logs.splitlines():
                        match = LOG_PATTERN.search(line)
                        if match:
                            data = match.groupdict()
                            # Chiave univoca per evitare duplicati dallo stream dei log
                            entry_key = (node, data["count"])
                            
                            if entry_key not in seen_entries:
                                seen_entries.add(entry_key)
                                with open(CSV_FILE, mode="a", newline="") as f:
                                    writer = csv.writer(f)
                                    writer.writerow([
                                        current_time,
                                        node,
                                        data["count"],
                                        data["x"],
                                        data["y"],
                                        data["z"],
                                        data["adj"],
                                        data["height"]
                                    ])
                except Exception as e:
                    # Ignora errori temporanei sui singoli container spenti/riavviati
                    pass

            time.sleep(INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        print(f"\nTelemetria interrotta dall'utente. Dati salvati in {CSV_FILE}.")

if __name__ == "__main__":
    main()