import argparse
import asyncio
import logging

from network import Server


async def run_server(address, port, log_level, bootstrap_nodes):
    # Configurar el nivel de logging
    logging.basicConfig(level=getattr(logging, log_level.upper()))
    log = logging.getLogger(__name__)
    # Crear e iniciar el servidor Kademlia
    server = Server()
    await server.listen(port, address)
    print(f"Servidor Kademlia iniciado en {address}:{port}")

    # Realizar bootstrap si se proporcionaron nodos de bootstrap
    if bootstrap_nodes:
        nodes = [
            (node.split(":")[0], int(node.split(":")[1])) for node in bootstrap_nodes
        ]
        await server.bootstrap(nodes)
        print("Bootstrap completado")

    # Mantener el servidor en ejecución
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Nodo Kademlia")
    parser.add_argument(
        "-a", "--address", type=str, default="127.0.0.1", help="Dirección IP del nodo"
    )
    parser.add_argument(
        "-p", "--port", type=int, default=8468, help="Puerto de conexión"
    )
    parser.add_argument(
        "-l",
        "--level",
        type=str,
        default="INFO",
        help="Nivel de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    parser.add_argument(
        "-b",
        "--bootstrap",
        type=str,
        nargs="*",
        help="Dirección de nodos de bootstrap. Formato: ip_address:port",
    )

    args = parser.parse_args()

    # Ejecutar el servidor Kademlia con los argumentos proporcionados
    asyncio.run(run_server(args.address, args.port, args.level, args.bootstrap))
