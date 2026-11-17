# 1. Get the kind Docker bridge gateway IP on the Raspberry Pi (usually 172.18.0.1)
KIND_GW=$(docker inspect kind-control-plane -f '{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}')
echo "Kind Host Gateway IP: $KIND_GW"

# 2. Forward $KIND_GW:18081 on the Pi -> 127.0.0.1:18080 (your SSH reverse tunnel)
pkill -f "18081" 2>/dev/null || true
nohup python3 -c "
import socket, threading
def pipe(a, b):
    try:
        while d := a.recv(65536): b.sendall(d)
    except Exception: pass
    finally: a.close(); b.close()
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('0.0.0.0', 18081))
s.listen(64)
while True:
    c, _ = s.accept()
    u = socket.create_connection(('127.0.0.1', 18080))
    threading.Thread(target=pipe, args=(c, u), daemon=True).start()
    threading.Thread(target=pipe, args=(u, c), daemon=True).start()
" >/dev/null 2>&1 &

# 3. Point kube-doctor inside kind to http://$KIND_GW:18081
kubectl -n kube-doctor-system set env deployment/kube-doctor \
  KUBE_DOCTOR_HUB_URL=http://${KIND_GW}:18081