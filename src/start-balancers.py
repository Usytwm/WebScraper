import subprocess


def start_servers(num_servers, start_port):
    servers = []
    ports = [start_port + i for i in range(num_servers)]
    peers = [{"host": "localhost", "port": p} for p in ports if p != start_port]

    for port in ports:
        # Comando para iniciar cada servidor
        command = f"python src/load_balancer.py {port} " + " ".join(
            f"{peer['host']}:{peer['port']}" for peer in peers if peer["port"] != port
        )
        process = subprocess.Popen(command, shell=True)
        servers.append(process)
        print(f"Started load balancer on port {port}")

    for server in servers:
        server.wait()


if __name__ == "__main__":
    start_servers(3, 6000)  # Inicia 3 servidores comenzando desde el puerto 5000
