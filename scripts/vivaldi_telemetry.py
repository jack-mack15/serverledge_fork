import re
import csv
import time
import socket
import os
from datetime import datetime

# Configurazione dinamica
HOSTNAME = socket.gethostname()
CSV_FILE = f"log_{HOSTNAME}.csv"
SERVERLEDGE_LOG_FILE = "/home/ubuntu/serverledge-tesi/serverledge.log"  # Percorso assoluto

# La regex va bene: re.search ignorerà in automatico la data iniziale (2026/09/08 08:58:04)
LOG_PATTERN = re.compile(
    r"count:(?P<count>\d+);\s*"
    r"X:\s*(?P<x>-?[\d\.]+);\s*"
    r"Y:\s*(?P<y>-?[\d\.]+);\s*"
    r"Z:\s*(?P<z>-?[\d\.]+);\s*"
    r"Adj:\s*(?P<adj>-?[\d\.]+);\s*"
    r"Height:\s*(?P<height>-?[\d\.]+)"
)

def main():
    print(f"Avvio telemetria. Lettura da '{SERVERLEDGE_LOG_FILE}', salvataggio su '{CSV_FILE}'...")
    
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "node_id", "counter", "x", "y", "z", "adjustment", "height"])

    seen_entries = set()

    try:
        with open(SERVERLEDGE_LOG_FILE, "r") as log_file:
            while True:
                line = log_file.readline()
                
                # Se non ci sono nuove righe, aspetta mezzo secondo
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
                                data["count"],
                                data["x"],
                                data["y"],
                                data["z"],
                                data["adj"],
                                data["height"]
                            ])
                            
    except FileNotFoundError:
        print(f"Errore: Il file '{SERVERLEDGE_LOG_FILE}' non esiste. Avvia prima Serverledge reindirizzando l'output su file.")
    except KeyboardInterrupt:
        print(f"\nTelemetria interrotta dall'utente. Dati salvati in {CSV_FILE}.")

if __name__ == "__main__":
    main()