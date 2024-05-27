import hashlib
import requests
from .bucket import KBucket
from loguru import logger


class Node:
    def __init__(self, ip, port):
        self.id = self.generate_node_id(ip, port)
        self.ip = ip
        self.port = port
        self.k_buckets = [KBucket(k_size=20) for _ in range(160)]
        logger.info(f"Node created at {self.ip}:{self.port}")

    def handle_store(self, key, value, ttl):
        key_id = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        closest_nodes = self.get_closest_nodes(key_id)
        logger.info(
            f"Handling store for key={key} to closest nodes: {[node for node in closest_nodes]}"
        )
        for node in closest_nodes:
            url = f"http://{node.ip}:{node.port}/store"
            data = {"key": key, "value": value, "ttl": ttl}
            requests.post(url, json=data)

    def handle_find_value(self, key):
        key_id = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        closest_nodes = self.get_closest_nodes(key_id)
        logger.info(
            f"Handling find_value for key={key} from closest nodes: {[node.id for node in closest_nodes]}"
        )
        for node in closest_nodes:
            url = f"http://{node.ip}:{node.port}/find_value"
            data = {"key": key}
            response = requests.post(url, json=data)
            if response.json().get("value"):
                return response.json().get("value")
        return None

    def add_peer(self, node_id, ip, port):
        distance = self.calculate_distance(
            self.cut_node_id(self.id), self.cut_node_id(node_id)
        )
        bucket_index = self.get_bucket_index(distance)
        node_instance = Node(ip=ip, port=port)
        node_instance.id = node_id

        existing_nodes = self.k_buckets[bucket_index].nodes
        if any(n.id == node_id for n in existing_nodes):
            logger.info(
                f"Peer with ID {node_id}, IP {ip}, Port {port} already in the bucket"
            )
        elif len(self.k_buckets[bucket_index].nodes) == 20:
            logger.warning("Bucket is already full")
        else:
            self.k_buckets[bucket_index].add(node_instance)
            logger.info(f"Peer added with  IP: {ip}, Port: {port}")

    def get_peers(self):
        peers = []
        for bucket in self.k_buckets:
            peers.extend([(node.id, node.ip, node.port) for node in bucket.nodes])
        logger.info(f"Current peers: {peers}")
        self.visualize_network()
        return peers

    def visualize_network(self):
        logger.info("Current network state:")
        for i, bucket in enumerate(self.k_buckets):
            if len(bucket.nodes) > 0:
                logger.info(f"Bucket {i}:")
                bucket.visualize_k_buckets()

    @staticmethod
    def cut_node_id(node_id):
        binary_representation = bin(node_id)[2:]
        last_five_bits = binary_representation[-160:]
        cut_node_id = int(last_five_bits, 2)
        return cut_node_id

    @staticmethod
    def calculate_distance(id1, id2):
        return id1 ^ id2

    @staticmethod
    def get_bucket_index(distance: int) -> int:
        if distance == 0:
            return 0
        return distance.bit_length() - 1

    def get_closest_nodes(self, target_node_id, k=20):
        distance_to_nodes = [
            (self.calculate_distance(node.id, target_node_id), node)
            for bucket in self.k_buckets
            for node in bucket.nodes
        ]
        distance_to_nodes.sort(key=lambda x: x[0])
        return [node for _, node in distance_to_nodes[:k]]

    @staticmethod
    def generate_node_id(ip, port):
        unique_str = f"{ip}:{port}"
        return int(hashlib.sha256(unique_str.encode()).hexdigest(), 16)
