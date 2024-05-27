import argparse
import asyncio
from loguru import logger
from kadmelia.node import Node
from kadmelia.network import NetworkManager

# logger.add("kademlia.log")


async def main(host, port, bootstrap):
    node = Node(ip=host, port=port)
    network_manager = NetworkManager(node)

    if bootstrap:
        known_nodes = [("127.0.0.1", 5000)]
        await network_manager.bootstrap(known_nodes)
    else:
        known_nodes = []

    node.visualize_network()

    network_manager.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run a Kademlia node.")
    parser.add_argument("--host", type=str, required=True, help="Host IP")
    parser.add_argument("--port", type=int, required=True, help="Host port")
    parser.add_argument(
        "--bootstrap", action="store_true", help="Enable bootstrap mode"
    )

    args = parser.parse_args()

    asyncio.run(main(args.host, args.port, args.bootstrap))
