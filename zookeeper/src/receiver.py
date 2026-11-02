import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from core import ROOT, connect, encode, commit, log
from kazoo.exceptions import BadVersionError

p = argparse.ArgumentParser()
p.add_argument("--run", required=True)
p.add_argument("--port", type=int, default=18089)
a = p.parse_args()
z = connect()
base = "/hw/" + a.run
mutex = threading.Lock()
journal = ROOT / "logs" / a.run / "accepted.jsonl"


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        with mutex:
            try:
                if body["fencing"]:
                    tx = z.transaction()
                    tx.check(base + "/epoch", version=body["epoch_version"])
                    tx.set_data(base + "/result", encode(body))
                    commit(tx)
                else:
                    z.set(base + "/result", encode(body))
                _, stat = z.get(base + "/result")
                with journal.open("a") as f:
                    f.write(
                        json.dumps(
                            dict(body, pointer_version=stat.version), ensure_ascii=False
                        )
                        + "\n"
                    )
                log(
                    "accepted",
                    coordinator=body["coordinator"],
                    epoch=body["epoch"],
                    pointer_version=stat.version,
                )
                status, reply = 200, {"accepted": True, "pointer_version": stat.version}
            except BadVersionError:
                log(
                    "BadVersionError",
                    coordinator=body["coordinator"],
                    epoch=body["epoch"],
                )
                status, reply = 409, {"accepted": False, "error": "BadVersionError"}
        self.send_response(status)
        self.end_headers()
        self.wfile.write(encode(reply))


log("receiver_ready", port=a.port)
try:
    ThreadingHTTPServer(("127.0.0.1", a.port), Handler).serve_forever()
finally:
    z.stop()
    z.close()
