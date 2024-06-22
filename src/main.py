import asyncio
import logging
import argparse
from typing import List
from routes import create_app
from kademlia.network import Server
from kademlia.node import Node
from hypercorn.config import Config
from hypercorn.asyncio import serve


async def run_server(address, port, log_level, bootstrap_nodes: List[Node]):
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    log = logging.getLogger(__name__)
    server = Server()
    await server.listen(port, address)
    print(f"Servidor Kademlia iniciado en {address}:{port}")

    if bootstrap_nodes:
        nodes = [
            (node.split(":")[0], int(node.split(":")[1])) for node in bootstrap_nodes
        ]
        await server.bootstrap(nodes)
        print("Bootstrap completado")

    while True:
        await asyncio.sleep(3600)


async def run_flask():
    flask_app = create_app()
    config = Config()
    config.bind = ["127.0.0.1:9000"]
    await serve(flask_app, config)


async def main(args):
    # lanzamiento de ambos servidores
    kademlia_server = run_server(args.address, args.port, args.level, args.bootstrap)
    flask_server = run_flask()

    # Ejecutar ambos servidores asincrónicamente
    await asyncio.gather(kademlia_server, flask_server)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nodo Kademlia")
    parser.add_argument(
        "-a", "--address", type=str, default="127.0.0.1", help="Dirección IP del nodo"
    )
    parser.add_argument(
        "-p", "--port", type=int, default=8468, help="Puerto de conexión"
    )
    parser.add_argument("-l", "--level", type=str, default="INFO", help="Nivel de log")
    parser.add_argument(
        "-b", "--bootstrap", type=str, nargs="*", help="Dirección de nodos de bootstrap"
    )

    args = parser.parse_args()
    asyncio.run(main(args))

# import argparse
# import asyncio
# import logging

# from kademlia.network import Server

# from routes import create_app

# app = create_app()

# if __name__ == "__main__":
#     app.run(debug=True)


# async def run_server(address, port, log_level, bootstrap_nodes):
#     # Configurar el nivel de logging
#     logging.basicConfig(level=getattr(logging, log_level.upper()))
#     log = logging.getLogger(__name__)
#     # Crear e iniciar el servidor Kademlia
#     server = Server()
#     await server.listen(port, address)
#     print(f"Servidor Kademlia iniciado en {address}:{port}")

#     # Realizar bootstrap si se proporcionaron nodos de bootstrap
#     if bootstrap_nodes:
#         nodes = [
#             (node.split(":")[0], int(node.split(":")[1])) for node in bootstrap_nodes
#         ]
#         await server.bootstrap(nodes)
#         print("Bootstrap completado")

#     # Mantener el servidor en ejecución
#     while True:
#         await asyncio.sleep(3600)


# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description="Nodo Kademlia")
#     parser.add_argument(
#         "-a", "--address", type=str, default="127.0.0.1", help="Dirección IP del nodo"
#     )
#     parser.add_argument(
#         "-p", "--port", type=int, default=8468, help="Puerto de conexión"
#     )
#     parser.add_argument(
#         "-l",
#         "--level",
#         type=str,
#         default="INFO",
#         help="Nivel de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
#     )
#     parser.add_argument(
#         "-b",
#         "--bootstrap",
#         type=str,
#         nargs="*",
#         help="Dirección de nodos de bootstrap. Formato: ip_address:port",
#     )

#     args = parser.parse_args()

#     # Ejecutar el servidor Kademlia con los argumentos proporcionados
#     asyncio.run(run_server(args.address, args.port, args.level, args.bootstrap))

# # import argparse
# # from kademlia.network import main as kademlia_main

# # if __name__ == "__main__":
# #     parser = argparse.ArgumentParser(description="Client of a distributed scraper")
# #     parser.add_argument(
# #         "-a", "--address", type=str, default="127.0.0.1", help="node address"
# #     )
# #     parser.add_argument("-p", "--port", type=int, default=4142, help="connection port")
# #     parser.add_argument("-l", "--level", type=str, default="INFO", help="log level")
# #     parser.add_argument(
# #         "-d", "--depth", type=int, default=1, help="depth of recursive downloads"
# #     )
# #     parser.add_argument(
# #         "-u",
# #         "--urls",
# #         type=str,
# #         default="urls",
# #         help="path of file that contains the urls set",
# #     )
# #     parser.add_argument(
# #         "-m",
# #         "--master",
# #         type=str,
# #         default=None,
# #         help="address of an existing master node. Insert as ip_address:port_number",
# #     )

# #     args = parser.parse_args()

# #     kademlia_main(args)
