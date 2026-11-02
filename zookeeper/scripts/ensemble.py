import json
import logging
import socket
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from core import ROOT, connect, log
from kazoo.client import KazooClient
from kazoo.handlers.threading import KazooTimeoutError

logging.basicConfig(level=logging.ERROR)


def command(*args, input=None, expected_code=0):
    log("command", argv=list(args))
    r = subprocess.run(
        args,
        cwd=ROOT,
        input=input,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    print(r.stdout, flush=True)
    assert r.returncode == expected_code, (r.returncode, expected_code)
    return r.stdout


def srvr(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=3) as s:
            s.sendall(b"srvr")
            s.shutdown(socket.SHUT_WR)
            data = b""
            while part := s.recv(4096):
                data += part
        return data.decode()
    except OSError as e:
        return str(e)


def roles():
    for i in range(1, 4):
        log("server_state", server=i, output=srvr(2180 + i))


command("docker", "compose", "up", "-d")
z = connect()
command("docker", "compose", "exec", "-T", "zk1", "java", "-version")
command(
    "docker",
    "compose",
    "exec",
    "-T",
    "zk1",
    "sh",
    "-c",
    "echo JAVA_HOME=$JAVA_HOME; cat /conf/zoo.cfg",
)
for i in range(1, 4):
    command(
        "docker",
        "compose",
        "exec",
        "-T",
        f"zk{i}",
        "sh",
        "-c",
        "cat /data/myid; zkServer.sh status",
    )
roles()
followers = [i for i in range(1, 4) if "Mode: follower" in srvr(2180 + i)]
assert len(followers) == 2
command("docker", "compose", "stop", f"zk{followers[0]}")
time.sleep(3)
z.ensure_path("/hw-quorum")
z.set("/hw-quorum", b"write with 2 of 3")
assert z.get("/hw-quorum")[0] == b"write with 2 of 3"
log("two_servers_write_read", success=True)
roles()
z.stop()
z.close()
command("docker", "compose", "stop", f"zk{followers[1]}")
time.sleep(3)
roles()
last = [i for i in range(1, 4) if i not in followers][0]
probe = KazooClient(hosts=f"127.0.0.1:{2180+last}", timeout=4, read_only=False)
try:
    probe.start(timeout=5)
except KazooTimeoutError:
    log("one_server_no_quorum", connected=False, timeout_seconds=5)
else:
    raise AssertionError("Unexpected writable connection without quorum")
finally:
    probe.stop()
    probe.close()
command("docker", "compose", "start", *[f"zk{i}" for i in followers])
z = connect()
time.sleep(3)
roles()
cli = subprocess.Popen(
    ["docker", "compose", "exec", "-T", "zk1", "zkCli.sh", "-server", "zk1:2181"],
    cwd=ROOT,
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
)
try:
    cli.stdin.write("create -e /hw-ephemeral session-owned\n")
    cli.stdin.flush()
    deadline = time.monotonic() + 20
    while not z.exists("/hw-ephemeral"):
        assert time.monotonic() < deadline
        time.sleep(0.1)
    command(
        "docker",
        "compose",
        "exec",
        "-T",
        "zk2",
        "zkCli.sh",
        "-server",
        "zk2:2181",
        input="get -s /hw-ephemeral\nquit\n",
    )
    cli.stdin.write("quit\n")
    cli.stdin.flush()
    output, _ = cli.communicate(timeout=20)
    print("CREATING zkCli session (create -e, then quit):\n" + output, flush=True)
    deadline = time.monotonic() + 15
    while z.exists("/hw-ephemeral"):
        assert time.monotonic() < deadline
        time.sleep(0.1)
    command(
        "docker",
        "compose",
        "exec",
        "-T",
        "zk2",
        "zkCli.sh",
        "-server",
        "zk2:2181",
        input="get /hw-ephemeral\nquit\n",
        expected_code=1,
    )
    log("ephemeral_deleted_after_cli_close", success=True)
finally:
    if cli.poll() is None:
        cli.kill()
        cli.wait()
    z.stop()
    z.close()
log("ensemble_checks_passed")
