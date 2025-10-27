import socket
import struct
import json
import threading
import sys
from datetime import datetime

# ======================
# CONFIGURAÇÕES
# ======================
HEADER_FMT = '!BBH'
HEADER_SIZE = 4
VERSION = 1
FORMAT = 'utf-8'
DEFAULT_PORT = 50000

# ======================
# COMANDOS
# ======================
CMD_LOGIN = 1
CMD_POST_MESSAGE = 2
CMD_GET_HISTORY = 3
CMD_LOGOUT = 4

CMD_LOGIN_RESPONSE_OK = 101
CMD_BROADCAST_NEW_MESSAGE = 102
CMD_HISTORY_RESPONSE = 103
CMD_ERROR = 200


# ======================
# FUNÇÕES AUXILIARES
# ======================
def pack_message(cmd, payload):
    payload_bytes = json.dumps(payload, separators=(',', ':')).encode(FORMAT)
    header = struct.pack(HEADER_FMT, cmd, VERSION, len(payload_bytes))
    return header + payload_bytes


def recv_all(sock, n):
    data = b''
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def recv_message(sock):
    header = recv_all(sock, HEADER_SIZE)
    if not header:
        return None, None
    try:
        cmd, version, payload_len = struct.unpack(HEADER_FMT, header)
    except struct.error:
        return None, None
    payload = recv_all(sock, payload_len) if payload_len > 0 else b''
    return cmd, payload


# ======================
# THREAD DE RECEBIMENTO
# ======================
def receive_loop(sock):
    while True:
        msg = recv_message(sock)
        if msg is None:
            print("\n[Servidor desconectou]")
            break
        cmd, payload = msg
        if cmd is None:
            break
        try:
            data = json.loads(payload.decode(FORMAT)) if payload else {}
        except:
            data = {}

        if cmd == CMD_LOGIN_RESPONSE_OK:
            print(f"[Servidor] {data.get('message', '')}")
        elif cmd == CMD_HISTORY_RESPONSE:
            print("\n--- Histórico ---")
            for m in data.get("messages", []):
                print(f"[{m['timestamp']}] {m['author']}: {m['message']}")
            print("-----------------")
        elif cmd == CMD_BROADCAST_NEW_MESSAGE:
            print(f"\n[{data['timestamp']}] {data['author']}: {data['message']}")
        elif cmd == CMD_ERROR:
            print(f"[ERRO] {data.get('message','')}")
        else:
            print(f"[Comando desconhecido: {cmd}]")

    sock.close()
    sys.exit(0)


# ======================
# FUNÇÃO PRINCIPAL
# ======================
def main():
    print("=== Cliente do Mural de Recados ===")
    server_ip = input("IP do servidor [localhost]: ") or "localhost"
    port_str = input("Porta [50000]: ") or "50000"
    try:
        port = int(port_str)
    except:
        port = DEFAULT_PORT
    username = input("Nome de usuário: ").strip()
    if not username:
        print("Nome de usuário é obrigatório!")
        return

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((server_ip, port))
    except Exception as e:
        print(f"Erro ao conectar: {e}")
        return

    # LOGIN
    sock.sendall(pack_message(CMD_LOGIN, {"username": username}))
    print("Conectado. Use /sair para sair.\n")

    # Thread para receber mensagens
    threading.Thread(target=receive_loop, args=(sock,), daemon=True).start()

    # Loop principal
    while True:
        try:
            msg = input()
        except EOFError:
            break

        if msg.strip() == "/sair":
            sock.sendall(pack_message(CMD_LOGOUT, {}))
            print("Desconectando...")
            break
        elif msg.strip() == "/hist":
            sock.sendall(pack_message(CMD_GET_HISTORY, {}))
        elif msg.strip():
            sock.sendall(pack_message(CMD_POST_MESSAGE, {"message": msg.strip()}))

    sock.close()


if __name__ == "__main__":
    main()
