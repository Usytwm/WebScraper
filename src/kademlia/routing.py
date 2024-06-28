import heapq
import time
import operator
import asyncio

from itertools import chain
from collections import OrderedDict
from utils.utils import shared_prefix, bytes_to_bit_string


class KBucket:
    def __init__(self, rangeLower, rangeUpper, ksize, replacementNodeFactor=5):
        # Inicializa un bucket con un rango, tamaño k y factor de nodo de reemplazo
        self.range = (rangeLower, rangeUpper)
        self.nodes = OrderedDict()
        self.replacement_nodes = OrderedDict()
        self.touch_last_updated()
        self.ksize = ksize
        self.max_replacement_nodes = self.ksize * replacementNodeFactor

    def touch_last_updated(self):
        # Actualiza la marca de tiempo de la última actualización
        self.last_updated = time.monotonic()

    def get_nodes(self):
        # Devuelve una lista de nodos en el bucket
        return list(self.nodes.values())

    def split(self):
        # Divide el bucket en dos
        midpoint = (self.range[0] + self.range[1]) // 2
        one = KBucket(self.range[0], midpoint, self.ksize)
        two = KBucket(midpoint + 1, self.range[1], self.ksize)
        nodes = chain(self.nodes.values(), self.replacement_nodes.values())
        for node in nodes:
            bucket = one if node.long_id <= midpoint else two
            bucket.add_node(node)

        return (one, two)

    def remove_node(self, node):
        # Elimina un nodo del bucket
        if node.id in self.replacement_nodes:
            del self.replacement_nodes[node.id]

        if node.id in self.nodes:
            del self.nodes[node.id]

            if self.replacement_nodes:
                newnode_id, newnode = self.replacement_nodes.popitem()
                self.nodes[newnode_id] = newnode

    def has_in_range(self, node):
        # Verifica si un nodo está dentro del rango del bucket
        return self.range[0] <= node.long_id <= self.range[1]

    def is_new_node(self, node):
        # Verifica si un nodo es nuevo en el bucket
        return node.id not in self.nodes

    def add_node(self, node):
        # Añade un nodo al bucket
        if node.id in self.nodes:
            del self.nodes[node.id]
            self.nodes[node.id] = node
        elif len(self) < self.ksize:
            self.nodes[node.id] = node
        else:
            if node.id in self.replacement_nodes:
                del self.replacement_nodes[node.id]
            self.replacement_nodes[node.id] = node
            while len(self.replacement_nodes) > self.max_replacement_nodes:
                self.replacement_nodes.popitem(last=False)
            return False
        return True

    def depth(self):
        # Calcula la profundidad del bucket basada en el prefijo compartido
        vals = self.nodes.values()
        sprefix = shared_prefix([bytes_to_bit_string(n.id) for n in vals])
        return len(sprefix)

    def head(self):
        # Devuelve el primer nodo en el bucket
        return list(self.nodes.values())[0]

    def __getitem__(self, node_id):
        return self.nodes.get(node_id, None)

    def __len__(self):
        return len(self.nodes)


class TableTraverser:
    def __init__(self, table, startNode):
        # Inicializa el traverser con la tabla de enrutamiento y el nodo de inicio
        index = table.get_bucket_for(startNode)
        table.buckets[index].touch_last_updated()
        self.current_nodes = table.buckets[index].get_nodes()
        self.left_buckets = table.buckets[:index]
        self.right_buckets = table.buckets[(index + 1) :]
        self.left = True

    def __iter__(self):
        return self

    def __next__(self):
        # Devuelve el siguiente nodo en el recorrido de la tabla
        if self.current_nodes:
            return self.current_nodes.pop()

        if self.left and self.left_buckets:
            self.current_nodes = self.left_buckets.pop().get_nodes()
            self.left = False
            return next(self)

        if self.right_buckets:
            self.current_nodes = self.right_buckets.pop(0).get_nodes()
            self.left = True
            return next(self)

        raise StopIteration


class RoutingTable:
    def __init__(self, protocol, ksize, node):
        # Inicializa la tabla de enrutamiento con el protocolo, tamaño k y nodo
        self.node = node
        self.protocol = protocol
        self.ksize = ksize
        self.flush()

    def flush(self):
        # Limpia y reinicia la tabla de enrutamiento
        self.buckets = [KBucket(0, 2**160, self.ksize)]

    def split_bucket(self, index):
        # Divide un bucket en dos
        one, two = self.buckets[index].split()
        self.buckets[index] = one
        self.buckets.insert(index + 1, two)

    def lonely_buckets(self):
        # Devuelve los buckets que no se han actualizado recientemente
        hrago = time.monotonic() - 3600
        return [b for b in self.buckets if b.last_updated < hrago]

    def remove_contact(self, node):
        # Elimina un contacto de la tabla de enrutamiento
        index = self.get_bucket_for(node)
        self.buckets[index].remove_node(node)

    def is_new_node(self, node):
        # Verifica si un nodo es nuevo en la tabla de enrutamiento
        index = self.get_bucket_for(node)
        return self.buckets[index].is_new_node(node)

    def add_contact(self, node):
        # Añade un contacto a la tabla de enrutamiento
        index = self.get_bucket_for(node)
        bucket = self.buckets[index]

        if bucket.add_node(node):
            return

        if bucket.has_in_range(self.node) or bucket.depth() % 5 != 0:
            self.split_bucket(index)
            self.add_contact(node)
        else:
            asyncio.ensure_future(self.protocol.call_ping(bucket.head()))

    def get_bucket_for(self, node):
        # Devuelve el índice del bucket que contiene al nodo
        for index, bucket in enumerate(self.buckets):
            if node.long_id < bucket.range[1]:
                return index
        return None

    def find_neighbors(self, node, k=None, exclude=None):
        # Encuentra los vecinos más cercanos a un nodo
        k = k or self.ksize
        nodes = []
        for neighbor in TableTraverser(self, node):
            notexcluded = exclude is None or not neighbor.same_home_as(exclude)
            if neighbor.id != node.id and notexcluded:
                heapq.heappush(nodes, (node.distance_to(neighbor), neighbor))
            if len(nodes) == k:
                break

        return list(map(operator.itemgetter(1), heapq.nsmallest(k, nodes)))
