from collections import Counter
import logging

from kademlia.node import Node, NodeHeap
from utils.utils import gather_dict

log = logging.getLogger(__name__)


class SpiderCrawl:
    def __init__(self, protocol, node, peers, ksize, alpha):
        self.protocol = protocol
        self.ksize = ksize
        self.alpha = alpha
        self.node = node
        self.nearest = NodeHeap(self.node, self.ksize)
        self.last_ids_crawled = []
        log.info("creando spider con pares: %s", peers)
        self.nearest.push(peers)

    async def _find(self, rpcmethod):
        log.info("rastreando la red con los más cercanos: %s", str(tuple(self.nearest)))
        count = self.alpha
        if self.nearest.get_ids() == self.last_ids_crawled:
            count = len(self.nearest)
        self.last_ids_crawled = self.nearest.get_ids()

        dicts = {}
        for peer in self.nearest.get_uncontacted()[:count]:
            dicts[peer.id] = rpcmethod(peer, self.node)
            self.nearest.mark_contacted(peer)
        found = await gather_dict(dicts)
        return await self._nodes_found(found)

    async def _nodes_found(self, responses):
        raise NotImplementedError


class ValueSpiderCrawl(SpiderCrawl):
    def __init__(self, protocol, node, peers, ksize, alpha):
        SpiderCrawl.__init__(self, protocol, node, peers, ksize, alpha)
        self.nearest_without_value = NodeHeap(self.node, 1)

    async def find(self):
        return await self._find(self.protocol.call_find_value)

    async def _nodes_found(self, responses):
        toremove = []
        found_values = []
        for peerid, response in responses.items():
            response = RPCFindResponse(response)
            if not response.happened():
                toremove.append(peerid)
            elif response.has_value():
                found_values.append(response.get_value())
            else:
                peer = self.nearest.get_node(peerid)
                self.nearest_without_value.push(peer)
                self.nearest.push(response.get_node_list())
        self.nearest.remove(toremove)

        if found_values:
            return await self._handle_found_values(found_values)
        if self.nearest.have_contacted_all():
            return None
        return await self.find()

    async def _handle_found_values(self, values):
        value_counts = Counter(values)
        if len(value_counts) != 1:
            log.warning(
                "Se obtuvieron múltiples valores para la clave %i: %s",
                self.node.long_id,
                str(values),
            )
        value = value_counts.most_common(1)[0][0]

        peer = self.nearest_without_value.popleft()
        if peer:
            await self.protocol.call_store(peer, self.node.id, value)
        return value


class NodeSpiderCrawl(SpiderCrawl):
    async def find(self):
        return await self._find(self.protocol.call_find_node)

    async def _nodes_found(self, responses):
        toremove = []
        for peerid, response in responses.items():
            response = RPCFindResponse(response)
            if not response.happened():
                toremove.append(peerid)
            else:
                self.nearest.push(response.get_node_list())
        self.nearest.remove(toremove)

        if self.nearest.have_contacted_all():
            return list(self.nearest)
        return await self.find()


class RPCFindResponse:
    def __init__(self, response):
        self.response = response

    def happened(self):
        return self.response[0]

    def has_value(self):
        return isinstance(self.response[1], dict)

    def get_value(self):
        return self.response[1]["value"]

    def get_node_list(self):
        nodelist = self.response[1] or []
        return [Node(*nodeple) for nodeple in nodelist]
