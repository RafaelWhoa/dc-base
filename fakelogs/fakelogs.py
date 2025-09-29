import json
import random
import socket
import string
import time
import uuid
import grpc
from datetime import datetime

from protos import fakelogs_pb2
from protos import fakelogs_pb2_grpc

# Configurações
LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
SERVICES = ["auth", "payments", "search", "recommendation", "ingestion", "ui"]
HOSTNAME = socket.gethostname()    

def grpc_send_log(log_entry):
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = fakelogs_pb2_grpc.FakeLogsServiceStub(channel)
        response = stub.FakeLogs(fakelogs_pb2.LogsRequest(logs=json.dumps(log_entry)))
        print(f"gRPC response: {response.message}")



def random_message(length=50): # Gera uma mensagem aleatória
    return "".join(random.choices(string.ascii_letters + "     ", k=length)).strip()


def make_log_entry(service=None): # Cria uma entrada de log
    service = service or random.choice(SERVICES)
    entry = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "service": service,
        "host": HOSTNAME,
        "level": random.choices(LOG_LEVELS, weights=[15, 50, 20, 10, 5])[0],
        "message": random_message(random.randint(30, 120)),
        "request_id": str(uuid.uuid4()),
        "user_id": random.choice([None] + [str(random.randint(1000, 9999)) for _ in range(20)]),
        "extra": {
            "lat": round(random.uniform(-90, 90), 6),
            "lon": round(random.uniform(-180, 180), 6),
        },
    }
    return entry


def generate_and_send_logs(rate_per_second=10, total=0):
    import time
    sent = 0
    interval = 1.0 / rate_per_second
    next_ts = time.time()
    try:
        while True:
            if total and sent >= total:
                break
            log = make_log_entry()
            # envia o JSON para o microserviço via gRPC
            grpc_send_log(log)
            sent += 1
            next_ts += interval
            sleep_for = next_ts - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_ts = time.time()
    except KeyboardInterrupt:
        print(f"Interrompido pelo usuário. Logs enviados: {sent}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Gerador de logs falsos em JSON")
    parser.add_argument("--rate", "-r", type=int, default=10, help="logs por segundo")
    parser.add_argument("--count", "-c", type=int, default=0, help="total de logs (0 = infinito)")
    args = parser.parse_args()

    generate_and_send_logs(rate_per_second=args.rate, total=args.count)