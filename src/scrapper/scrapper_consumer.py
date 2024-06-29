import socket
import json


def register_node(node_id):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", 5000))
        s.sendall(
            json.dumps({"action": "register_scrapper", "node_id": node_id}).encode(
                "utf-8"
            )
        )


def request_task(node_id):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect(("localhost", 5000))
        s.sendall(
            json.dumps({"action": "get_task", "node_id": node_id}).encode("utf-8")
        )
        data = s.recv(1024)
        task = json.loads(data.decode("utf-8"))
        return task["task"]


# Usage
if __name__ == "__main__":
    register_node("scrapper1")
    task = request_task("scrapper1")
    print(f"Assigned task: {task}")
