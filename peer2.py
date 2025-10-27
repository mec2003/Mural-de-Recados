#!/usr/bin/env python3
"""
peer.py - Nó P2P para Mural de Recados (Opção A)
Rodar em múltiplos terminais / máquinas. Cada nó atua como servidor e cliente.
Segue o protocolo: header 4 bytes + payload JSON (UTF-8). Commands conforme PDF.
"""

import socket
import struct
import json
import threading
import sys
from datetime import datetime, timezone

# -------------------------
# Protocolo / constantes
# -------------------------
HEADER_FMT = '!BBH'   # COMANDO:uint8, VERSAO:uint8, TAMANHO:uint16 (big-endian)
HEADER_SIZE = 4
VERSION = 1
FORMAT = 'utf-8'

# Comandos (conforme PDF)
CMD_LOGIN = 1
CMD_POST_MESSAGE = 2
CMD_GET_HISTORY = 3
CMD_LOGOUT = 4

CMD_LOGIN_RESPONSE_OK = 101
CMD_BROADCAST_NEW_MESSAGE = 102
CMD_HISTORY_RESPONSE = 103
CMD_ERROR = 200

# default bind port
DEFAULT_PORT = 50000

# -------------------------
# Estado do peer
# -------------------------
peers_lock = threading.Lock()
# mapping: conn -> {'username': str, 'addr': (ip,port)}
peer_conns = {}
# For outgoing connections we also store socket objects in peer_conns (same structure)
history_lock = threading.Lock()
history = []  # list of dicts: {'author':..., 'message':..., 'timestamp':...}
seen_lock = threading.Lock()
seen_messages = set()  # set of message keys to avoid loops (author + message)

# -------------------------
# util: recv/send precisely
# -------------------------
def recv_all(sock, n):
    data = b''
    while len(data) < n:
        try:
            chunk = sock.recv(n - len(data))
        except Exception:
            return None
        if not chunk:
            return None
        data += chunk
    return data

def recv_message(sock):
    header = recv_all(sock, HEADER_SIZE)
    if not header:
        return None, None, None
    try:
        cmd, version, payload_len = struct.unpack(HEADER_FMT, header)
    except struct.error:
        return None, None, None
    payload = b''
    if payload_len > 0:
        payload = recv_all(sock, payload_len)
        if payload is None:
            return None, None, None
    return cmd, version, payload

def send_message(sock, cmd, payload_obj):
    try:
        payload_bytes = json.dumps(payload_obj, separators=(',', ':')).encode(FORMAT)
        header = struct.pack(HEADER_FMT, cmd, VERSION, len(payload_bytes))
        sock.sendall(header + payload_bytes)
    except Exception as e:
        raise

# -------------------------
# message helpers
# -------------------------
def message_key(author, message):
    # dedupe by author + message text (ignore timestamp)
    return f"{author}||{message}"

def add_to_history_if_new(record):
    key = message_key(record.get('author',''), record.get('message',''))
    with seen_lock:
        if key in seen_messages:
            return False
        seen_messages.add(key)
    with history_lock:
        history.append(record)
    return True

# -------------------------
# Broadcast propagation
# -------------------------
def propagate_broadcast(record, exclude_sock=None):
    """
    Envia CMD_BROADCAST_NEW_MESSAGE para todos peers exceto exclude_sock.
    """
    with peers_lock:
        dead = []
        for conn in list(peer_conns.keys()):
            if conn is exclude_sock:
                continue
            try:
                send_message(conn, CMD_BROADCAST_NEW_MESSAGE, record)
            except Exception:
                dead.append(conn)
        for d in dead:
            peer_conns.pop(d, None)
            try:
                d.close()
            except:
                pass

# -------------------------
# Loop de recebimento principal (compartilhado)
# -------------------------
def peer_recv_loop(conn, addr):
    """
    Loop principal para processar mensagens recebidas após o handshake inicial.
    Usado por handle_incoming e handle_outgoing.
    """
    while True:
        msg = recv_message(conn)
        if msg is None:
            break
        cmd, version, payload = msg
        if cmd == CMD_POST_MESSAGE:
            # someone sent a POST_MESSAGE (should be {"message": "..."}). We'll convert it to a broadcast record
            try:
                obj = json.loads(payload.decode(FORMAT)) if payload else {}
                text = obj.get('message')
            except:
                text = None
            if not text:
                send_message(conn, CMD_ERROR, {"message": "POST_MESSAGE inválido"})
                continue
            author = peer_conns.get(conn, {}).get('username', 'unknown')
            timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
            record = {"author": author, "message": text, "timestamp": timestamp}
            added = add_to_history_if_new(record)
            if added:
                print(f"[MSG] {author}: {text}")
                propagate_broadcast(record, exclude_sock=conn)
            # Note: we don't send a separate ACK; peers receive broadcast
        elif cmd == CMD_BROADCAST_NEW_MESSAGE:
            # Receiving a broadcast from a peer: store if new and forward to others
            try:
                obj = json.loads(payload.decode(FORMAT)) if payload else {}
                author = obj.get('author')
                text = obj.get('message')
                timestamp = obj.get('timestamp')
            except:
                author = text = timestamp = None
            if not author or not text:
                send_message(conn, CMD_ERROR, {"message": "BROADCAST payload inválido"})
                continue
            record = {"author": author, "message": text, "timestamp": timestamp}
            added = add_to_history_if_new(record)
            if added:
                print(f"[BCAST] {author}: {text}")
                # forward to others
                propagate_broadcast(record, exclude_sock=conn)
            # else ignore duplicate
        elif cmd == CMD_GET_HISTORY:
            with history_lock:
                send_message(conn, CMD_HISTORY_RESPONSE, {"messages": history})
        elif cmd == CMD_LOGOUT:
            break
        elif cmd == CMD_LOGIN_RESPONSE_OK:
            # Pode ocorrer se um par responder ao nosso LOGIN em handle_outgoing. Apenas ignora.
            pass
        elif cmd == CMD_HISTORY_RESPONSE:
            # Recebemos histórico. Adiciona ao nosso.
            try:
                obj = json.loads(payload.decode(FORMAT)) if payload else {}
                messages = obj.get('messages', [])
                for record in messages:
                    author = record.get('author')
                    text = record.get('message')
                    if author and text:
                        add_to_history_if_new(record)
            except:
                pass
        elif cmd == CMD_ERROR:
            # Recebemos erro de um peer
            try:
                obj = json.loads(payload.decode(FORMAT)) if payload else {}
                msg_text = obj.get('message', 'Erro desconhecido')
                print(f"[PEER ERROR] Recebido erro de {addr}: {msg_text}")
            except:
                pass
        else:
            send_message(conn, CMD_ERROR, {"message": f"Comando desconhecido: {cmd}"})

# -------------------------
# Handler para conexões recebidas (incoming)
# -------------------------
def handle_incoming(conn, addr):
    """
    Executa para conexões que aceitaram no servidor local. (A)
    Primeiro espera um LOGIN (cmd=1) para identificar o peer.
    Depois responde LOGIN_RESPONSE_OK e envia HISTORY.
    """
    try:
        cmd, version, payload = recv_message(conn)
        if cmd is None:
            return
        if cmd != CMD_LOGIN:
            # protocolo: expect login first
            send_message(conn, CMD_ERROR, {"message": "LOGIN esperado como primeiro comando"})
            return
        try:
            data = json.loads(payload.decode(FORMAT)) if payload else {}
            username = data.get('username')
            if not username or not isinstance(username, str):
                raise ValueError
        except Exception:
            send_message(conn, CMD_ERROR, {"message": "JSON de LOGIN inválido"})
            return

        with peers_lock:
            peer_conns[conn] = {'username': username, 'addr': addr}

        print(f"[+] Conexão recebida de {addr} (username={username})")

        # send LOGIN_RESPONSE_OK and HISTORY
        send_message(conn, CMD_LOGIN_RESPONSE_OK, {"message": "Login recebido"})
        # send HISTORY_RESPONSE with local history
        with history_lock:
            send_message(conn, CMD_HISTORY_RESPONSE, {"messages": history})

        # Loop de recebimento
        peer_recv_loop(conn, addr)

    except Exception as e:
        # print(f"Erro handler {addr}: {e}")
        pass
    finally:
        with peers_lock:
            if conn in peer_conns:
                info = peer_conns.pop(conn)
                print(f"Conexão encerrada: {info.get('username')} @ {addr}")
        try:
            conn.close()
        except:
            pass

# -------------------------
# Novo Handler para conexões ativas (outgoing)
# -------------------------
def handle_outgoing(conn, addr, local_username):
    """
    Executa para conexões ativas (outgoing). (B)
    O LOGIN já foi enviado pelo connect_to_peer.
    Espera CMD_LOGIN_RESPONSE_OK e CMD_HISTORY_RESPONSE antes de iniciar o loop.
    """
    try:
        # Espera a resposta do nosso LOGIN (CMD_LOGIN_RESPONSE_OK)
        cmd, version, payload = recv_message(conn)
        if cmd != CMD_LOGIN_RESPONSE_OK:
            print(f"[OUT] Falha no login para {addr}. Recebido: {cmd}. Fechando.")
            return

        # O peer remoto não nos informa o nome de usuário (username) explicitamente no protocolo.
        # Mantemos o nosso username de forma temporária/interna para este socket.
        with peers_lock:
            peer_conns[conn] = {'username': local_username, 'addr': addr}

        print(f"[OUT] Login OK com {addr}. Esperando histórico...")

        # O servidor remoto deve enviar HISTORY_RESPONSE em seguida.
        cmd, version, payload = recv_message(conn)
        if cmd == CMD_HISTORY_RESPONSE:
            try:
                obj = json.loads(payload.decode(FORMAT)) if payload else {}
                messages = obj.get('messages', [])
                for record in messages:
                    author = record.get('author')
                    text = record.get('message')
                    if author and text:
                        if add_to_history_if_new(record):
                            print(f"[HIST] Novo histórico de {addr}: {author}: {text[:20]}...")
            except:
                pass
        else:
            print(f"[OUT] Não recebemos HISTORY_RESPONSE de {addr} após LOGIN_OK. Recebido: {cmd}")
            # Não é fatal, continua para o loop principal.
        
        # Loop de recebimento
        peer_recv_loop(conn, addr)

    except Exception as e:
        # print(f"Erro handler outgoing {addr}: {e}")
        pass
    finally:
        with peers_lock:
            if conn in peer_conns:
                info = peer_conns.pop(conn)
                print(f"Conexão encerrada: {info.get('username')} @ {addr}")
        try:
            conn.close()
        except:
            pass

# -------------------------
# Thread para aceitar conexões
# -------------------------
def server_listen(bind_ip, bind_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) # Permite reuso rápido do endereço
    sock.bind((bind_ip, bind_port))
    sock.listen()
    print(f"[SERVIDOR] ouvindo em {bind_ip}:{bind_port}")
    try:
        while True:
            conn, addr = sock.accept()
            t = threading.Thread(target=handle_incoming, args=(conn, addr), daemon=True)
            t.start()
    except Exception as e:
        print(f"[SERVIDOR] encerrando: {e}")
    finally:
        sock.close()

# -------------------------
# Conectar ativamente a um peer (outgoing)
# -------------------------
def connect_to_peer(remote_ip, remote_port, local_username):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((remote_ip, remote_port))
        # send LOGIN
        send_message(s, CMD_LOGIN, {"username": local_username})
        
        # O estado do peer_conns é definido dentro de handle_outgoing após o handshake.
        # Agora usamos o handler de conexão de saída
        t = threading.Thread(target=handle_outgoing, args=(s, (remote_ip, remote_port), local_username), daemon=True)
        t.start()
        print(f"[OUT] Conectando ativamente a {remote_ip}:{remote_port}...")
    except Exception as e:
        print(f"[OUT] Falha conectar a {remote_ip}:{remote_port} - {e}")

# -------------------------
# CLI main loop (inalterado)
# -------------------------
def print_help():
    print("""
Comandos:
 /connect ip:port   -> conecta a outro peer
 /peers             -> lista peers conectados
 /hist              -> mostra histórico local
 /sair              -> desconecta e sai
 /help              -> mostrar este menu
Qualquer outra linha é enviada como novo recado local (propagado pela rede).
""")

def main():
    if len(sys.argv) >= 2:
        bind_port = int(sys.argv[1])
    else:
        bind_port = DEFAULT_PORT

    bind_ip = '0.0.0.0'

    print("=== Nó P2P - Mural de Recados (Opção A) ===")
    username = input("Seu nome de usuário: ").strip()
    if not username:
        print("Usuário obrigatório.")
        return

    # start server thread
    threading.Thread(target=server_listen, args=(bind_ip, bind_port), daemon=True).start()

    print("Digite peers iniciais separados por vírgula (ex: 127.0.0.1:50001,192.168.0.5:50000) ou Enter para pular:")
    initial = input().strip()
    if initial:
        items = [it.strip() for it in initial.split(',') if it.strip()]
        for it in items:
            try:
                ip, port = it.split(':')
                connect_to_peer(ip, int(port), username)
            except:
                print("Ignorando endpoint inválido:", it)

    print_help()
    # main CLI loop
    try:
        while True:
            line = input("> ").strip()
            if not line:
                continue
            if line.startswith('/'):
                cmd = line.split()[0].lower()
                if cmd == '/connect':
                    parts = line.split()
                    if len(parts) < 2:
                        print("Uso: /connect ip:port")
                        continue
                    try:
                        ipport = parts[1]
                        ip, port = ipport.split(':')
                        connect_to_peer(ip, int(port), username)
                    except Exception as e:
                        print("Formato inválido. Use ip:port")
                elif cmd == '/peers':
                    with peers_lock:
                        if not peer_conns:
                            print("(nenhum peer conectado)")
                        else:
                            for c, info in peer_conns.items():
                                print(f"- {info.get('username')} @ {info.get('addr')}")
                elif cmd == '/hist':
                    with history_lock:
                        if not history:
                            print("(histórico vazio)")
                        else:
                            print("----- Histórico local -----")
                            for m in history:
                                print(f"[{m.get('timestamp')}] {m.get('author')}: {m.get('message')}")
                            print("--------------------------")
                elif cmd == '/sair':
                    print("Desconectando de todos os peers e saindo...")
                    with peers_lock:
                        for c in list(peer_conns.keys()):
                            try:
                                send_message(c, CMD_LOGOUT, {})
                            except:
                                pass
                            try:
                                c.close()
                            except:
                                pass
                        peer_conns.clear()
                    break
                elif cmd == '/help':
                    print_help()
                else:
                    print("Comando desconhecido. Use /help.")
            else:
                # user typed a message -> create record and broadcast
                text = line
                timestamp = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
                record = {"author": username, "message": text, "timestamp": timestamp}
                added = add_to_history_if_new(record)
                if added:
                    print(f"[Você @{timestamp}] {text}")
                    # propagate as BROADCAST_NEW_MESSAGE so author/timestamp preserved
                    propagate_broadcast(record, exclude_sock=None)
                else:
                    print("[ignorado: mensagem duplicada]")
    except KeyboardInterrupt:
        print("\nCtrl+C recebido. Saindo...")
    finally:
        print("Bye.")

if __name__ == "__main__":
    main()