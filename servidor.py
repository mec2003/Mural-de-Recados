import socket
import threading
import struct
import json
from datetime import datetime, timezone

# ======================
# CONFIGURAÇÕES
# ======================
HOST = '0.0.0.0'
PORT = 50000

HEADER_FMT = '!BBH'
HEADER_SIZE = 4
VERSION = 1
FORMAT = 'utf-8'

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
# ESTRUTURAS GLOBAIS
# ======================
clients = {}  # conn -> {'username': str, 'addr': (ip,port)}
history = []  # mensagens anteriores
lock = threading.Lock()


# ======================
# FUNÇÕES AUXILIARES
# ======================
def recv_all(conn, n):
    data = b''
    while len(data) < n:
        part = conn.recv(n - len(data))
        if not part:
            return None
        data += part
    return data


def recv_message(conn):
    header = recv_all(conn, HEADER_SIZE)
    if not header:
        return None, None, None
    cmd, version, payload_len = struct.unpack(HEADER_FMT, header)
    payload = recv_all(conn, payload_len) if payload_len > 0 else b''
    return cmd, version, payload


def send_message(conn, cmd, payload):
    payload_bytes = json.dumps(payload, separators=(',', ':')).encode(FORMAT)
    header = struct.pack(HEADER_FMT, cmd, VERSION, len(payload_bytes))
    conn.sendall(header + payload_bytes)


def broadcast(cmd, payload):
    """Envia a mesma mensagem a todos os clientes conectados."""
    with lock:
        disconnected = []
        for c in clients.keys():
            try:
                send_message(c, cmd, payload)
            except Exception:
                disconnected.append(c)
        for c in disconnected:
            clients.pop(c, None)
            try:
                c.close()
            except:
                pass


# ======================
# HANDLER DE CLIENTE
# ======================
def handle_client(conn, addr):
    try:
        cmd, version, payload = recv_message(conn)
        if cmd != CMD_LOGIN:
            send_message(conn, CMD_ERROR, {"message": "Primeiro comando deve ser LOGIN"})
            conn.close()
            return

        try:
            data = json.loads(payload.decode(FORMAT))
            username = data.get("username")
            if not username:
                raise ValueError
        except Exception:
            send_message(conn, CMD_ERROR, {"message": "JSON de LOGIN inválido"})
            conn.close()
            return

        # verifica nome duplicado
        with lock:
            if any(u['username'] == username for u in clients.values()):
                send_message(conn, CMD_ERROR, {"message": "Username já está em uso"})
                conn.close()
                return
            clients[conn] = {'username': username, 'addr': addr}

        print(f"[LOGIN] {addr} como {username}")

        send_message(conn, CMD_LOGIN_RESPONSE_OK, {"message": "Login realizado com sucesso!"})
        send_message(conn, CMD_HISTORY_RESPONSE, {"messages": history})

        while True:
            msg = recv_message(conn)
            if not msg[0]:
                break

            cmd, version, payload = msg
            if cmd == CMD_POST_MESSAGE:
                try:
                    data = json.loads(payload.decode(FORMAT))
                    text = data.get("message")
                except:
                    text = None
                if not text:
                    send_message(conn, CMD_ERROR, {"message": "Mensagem inválida"})
                    continue

                timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
                record = {"author": username, "message": text, "timestamp": timestamp}

                with lock:
                    history.append(record)
                broadcast(CMD_BROADCAST_NEW_MESSAGE, record)
                print(f"[{timestamp}] {username}: {text}")

            elif cmd == CMD_GET_HISTORY:
                send_message(conn, CMD_HISTORY_RESPONSE, {"messages": history})

            elif cmd == CMD_LOGOUT:
                print(f"[LOGOUT] {username} saiu.")
                break

            else:
                send_message(conn, CMD_ERROR, {"message": f"Comando desconhecido ({cmd})"})

    except Exception as e:
        print(f"[ERRO] {addr}: {e}")
    finally:
        with lock:
            clients.pop(conn, None)
        conn.close()


# ======================
# MAIN SERVER LOOP
# ======================
def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()
    print(f"Servidor rodando em {HOST}:{PORT}")
    try:
        while True:
            conn, addr = server.accept()
            t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
            t.start()
    except KeyboardInterrupt:
        print("\nEncerrando servidor...")
    finally:
        server.close()


if __name__ == "__main__":
    start_server()
