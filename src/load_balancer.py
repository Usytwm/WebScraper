import socket
import threading
import json


class LoadBalancer:
    def __init__(self, host="localhost", port=5000):
        self.host = host
        self.port = port
        self.nodes = {}  # Registro de nodos scrapper
        self.kademlia_nodes = {}  # Registro de nodos Kademlia
        self.tasks = []  # Tareas pendientes
        self.lock = threading.Lock()

    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            print(f"Server started at {self.host}:{self.port}")
            while True:
                conn, addr = s.accept()
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()

    def handle_client(self, conn, addr):
        with conn:
            data = conn.recv(1024)
            if not data:
                return
            message = json.loads(data.decode("utf-8"))
            action = message["action"]

            if action == "register_scrapper":
                self.register_scrapper(message["node_id"], addr)
            elif action == "register_kademlia":
                self.register_kademlia(message["node_id"], addr)
            elif action == "get_task":
                task = self.assign_task(message["node_id"])
                conn.sendall(json.dumps({"task": task}).encode("utf-8"))

    def register_scrapper(self, node_id, addr):
        with self.lock:
            self.nodes[node_id] = addr

    def register_kademlia(self, node_id, addr):
        with self.lock:
            self.kademlia_nodes[node_id] = addr

    def assign_task(self, node_id):
        with self.lock:
            if self.tasks:
                return self.tasks.pop(0)
            else:
                return None


# Usage
if __name__ == "__main__":
    lb = LoadBalancer()
    lb.start_server()
