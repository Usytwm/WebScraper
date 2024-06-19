import argparse
from kademlia.network import main as kademlia_main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Client of a distributed scraper")
    parser.add_argument(
        "-a", "--address", type=str, default="127.0.0.1", help="node address"
    )
    parser.add_argument("-p", "--port", type=int, default=4142, help="connection port")
    parser.add_argument("-l", "--level", type=str, default="INFO", help="log level")
    parser.add_argument(
        "-d", "--depth", type=int, default=1, help="depth of recursive downloads"
    )
    parser.add_argument(
        "-u",
        "--urls",
        type=str,
        default="urls",
        help="path of file that contains the urls set",
    )
    parser.add_argument(
        "-m",
        "--master",
        type=str,
        default=None,
        help="address of an existing master node. Insert as ip_address:port_number",
    )

    args = parser.parse_args()

    kademlia_main(args)
