from operator import itemgetter
import heapq


class Node:
    def __init__(self, node_id, ip=None, port=None):
        # Inicializa un nodo con su ID, IP y puerto
        self.id = node_id
        self.ip = ip
        self.port = port
        self.long_id = int(node_id.hex(), 16)

    def same_home_as(self, node):
        # Verifica si este nodo está en la misma dirección IP y puerto que otro nodo
        return self.ip == node.ip and self.port == node.port

    def distance_to(self, node):
        # Calcula la distancia a otro nodo usando XOR
        return self.long_id ^ node.long_id

    def __iter__(self):
        return iter([self.id, self.ip, self.port])

    def __repr__(self):
        return repr([self.long_id, self.ip, self.port])

    def __str__(self):
        return "%s:%s" % (self.ip, str(self.port))


class NodeHeap:
    def __init__(self, node, maxsize):
        # Inicializa un heap de nodos con un tamaño máximo
        self.node = node
        self.heap = []
        self.contacted = set()
        self.maxsize = maxsize

    def remove(self, peers):
        # Remueve nodos del heap
        peers = set(peers)
        if not peers:
            return
        nheap = []
        for distance, node in self.heap:
            if node.id not in peers:
                heapq.heappush(nheap, (distance, node))
        self.heap = nheap

    def get_node(self, node_id):
        # Obtiene un nodo por su ID
        for _, node in self.heap:
            if node.id == node_id:
                return node
        return None

    def have_contacted_all(self):
        # Verifica si todos los nodos han sido contactados
        return len(self.get_uncontacted()) == 0

    def get_ids(self):
        # Devuelve los IDs de los nodos en el heap
        return [n.id for n in self]

    def mark_contacted(self, node):
        # Marca un nodo como contactado
        self.contacted.add(node.id)

    def popleft(self):
        # Elimina y devuelve el primer nodo en el heap
        return heapq.heappop(self.heap)[1] if self else None

    def push(self, nodes):
        # Añade nodos al heap
        if not isinstance(nodes, list):
            nodes = [nodes]

        for node in nodes:
            if node not in self:
                distance = self.node.distance_to(node)
                heapq.heappush(self.heap, (distance, node))

    def __len__(self):
        return min(len(self.heap), self.maxsize)

    def __iter__(self):
        nodes = heapq.nsmallest(self.maxsize, self.heap)
        return iter(map(itemgetter(1), nodes))

    def __contains__(self, node):
        for _, other in self.heap:
            if node.id == other.id:
                return True
        return False

    def get_uncontacted(self):
        # Devuelve una lista de nodos no contactados
        return [n for n in self if n.id not in self.contacted]
