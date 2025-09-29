from kafka import KafkaProducer
from concurrent import futures
import logging

import grpc
from protos import fakelogs_pb2
from protos import fakelogs_pb2_grpc

class FakeLogsGrpc(fakelogs_pb2_grpc.FakeLogsServiceServicer):
    def FakeLogs(self, request, context):
        producer = KafkaProducer(bootstrap_servers='localhost:29092')
        future = producer.send('topico-teste', request.logs.encode('utf-8'))
        result = future.get(timeout=60)
        return fakelogs_pb2.LogsReply(message="Log received")

def serve():
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    fakelogs_pb2_grpc.add_FakeLogsServiceServicer_to_server(FakeLogsGrpc(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()

if __name__ == "__main__":
    logging.basicConfig()
    serve()