"""Mission upload (and an optional flight) against ArduPilot SITL, on real sockets.

    arducopter --serial0--> [air bridge] <=> udp_proxy <=> [ground bridge] --> this script
    arducopter --serial1--> this script (clean side channel, used only to check results)

With `--mode direct` the bridges are left out and the proxy sits straight
between SITL and the script, which is what a plain radio does.

The proxy applies the same link model as the simulator: by default 30% loss
each way, 80 ms delay, 30 ms jitter and 5,760 B/s. The script asks SITL for
all telemetry streams at `--stream-hz`, so telemetry competes for the radio.

The upload is a ground station's: MISSION_COUNT, one MISSION_ITEM_INT per
request, resend the last message after 1.5 s of silence. If the vehicle
rejects or abandons the upload, the script starts again, like a user pressing
upload again, until `--cap` seconds have passed. The uploaded mission is then
downloaded over the clean side channel and compared item by item.

These runs use the real clock and the real autopilot, so they are slow and
each run differs. Run from Linux or WSL with SITL built:

    python bench/sitl_mission.py --ardupilot ~/ardupilot --mode gateway --runs 3
    python bench/sitl_mission.py --ardupilot ~/ardupilot --mode gateway --fly
"""

from __future__ import annotations

import argparse
import math
import os
import statistics
import subprocess
import sys
import tempfile
import time
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pymavlink import mavutil

HOME = (-35.363261, 149.165230, 584.0)
P = {  # local ports
    "sitl_out": 14560,  # SITL serial0 sends here (air bridge, or the proxy in direct mode)
    "proxy_a": 14570,
    "proxy_b": 14571,
    "air_link": 14600,
    "ground_link": 14601,
    "ground_mav": 14580,
    "gcs": 14550,
    "side": 14590,  # SITL serial1, the clean channel
}
LOCAL = "127.0.0.1"
mavlink: Any = mavutil.mavlink


@dataclass
class Upload:
    ok: bool
    seconds: float
    attempts: int
    verified: bool


def addr(port: int) -> str:
    return f"{LOCAL}:{port}"


def start_rig(stack: ExitStack, args: argparse.Namespace, workdir: Path) -> None:
    def spawn(cmd: list[str], name: str) -> None:
        log = open(workdir / f"{name}.log", "w")
        stack.callback(log.close)
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, cwd=workdir)
        stack.callback(proc.wait, 10)
        stack.callback(proc.terminate)

    if args.instance:  # run beside another rig: shift every port and the SITL instance
        for name in P:
            P[name] += 100 * args.instance
    py = sys.executable
    gateway = args.mode == "gateway"
    proxy = [py, "-m", "mavgate.udp_proxy", "--a-listen", addr(P["proxy_a"]),
             "--b-listen", addr(P["proxy_b"]), "--loss", str(args.loss), "--delay", "0.08",
             "--jitter", "0.03", "--bandwidth", "5760", "--seed", str(args.seed), "--report", "10"]
    if not gateway:  # the script listens and never speaks first, so the proxy must know it
        proxy += ["--b-peer", addr(P["gcs"])]
    spawn(proxy, "proxy")
    if gateway:
        spawn([py, "-m", "mavgate.mavlink_bridge", "--mavlink-listen", addr(P["sitl_out"]),
               "--link-local", addr(P["air_link"]), "--link-remote", addr(P["proxy_a"]),
               "--ttl", "5", "--report", "10"], "air_bridge")
        spawn([py, "-m", "mavgate.mavlink_bridge", "--mavlink-listen", addr(P["ground_mav"]),
               "--mavlink-peer", addr(P["gcs"]), "--link-local", addr(P["ground_link"]),
               "--link-remote", addr(P["proxy_b"]), "--ttl", "5", "--report", "10"],
              "ground_bridge")
    time.sleep(1.0)
    sitl_target = P["sitl_out"] if gateway else P["proxy_a"]
    ap = Path(args.ardupilot).expanduser()
    spawn([str(ap / "build/sitl/bin/arducopter"), "--model", "+", "--speedup", "1", "--wipe",
           "-I", str(args.instance),
           "--defaults", str(ap / "Tools/autotest/default_params/copter.parm"),
           "--home", ",".join(map(str, HOME)) + ",0",
           "--serial0", f"udpclient:{addr(sitl_target)}",
           "--serial1", f"udpclient:{addr(P['side'])}"], "sitl")


def connect(port: int) -> Any:
    conn = mavutil.mavlink_connection(f"udpin:{addr(port)}", source_system=255)
    if conn.wait_heartbeat(timeout=90) is None:
        raise SystemExit(f"no heartbeat on port {port}; see the logs")
    return conn


def request_streams(conn: Any, hz: int) -> None:
    for _ in range(5):  # the request itself may be lost
        conn.mav.request_data_stream_send(1, 0, mavlink.MAV_DATA_STREAM_ALL, hz, 1)
        time.sleep(0.2)


def survey(n: int) -> list[tuple[int, float, float, float]]:
    """Item 0 is home; then waypoints on a circle. Returns (command, lat, lon, alt)."""
    items = [(mavlink.MAV_CMD_NAV_WAYPOINT, HOME[0], HOME[1], 0.0)]
    for i in range(1, n):
        a = 2 * math.pi * i / n
        items.append((mavlink.MAV_CMD_NAV_WAYPOINT, HOME[0] + 0.001 * math.cos(a),
                      HOME[1] + 0.001 * math.sin(a), 30.0))
    return items


def flight(n: int) -> list[tuple[int, float, float, float]]:
    items = [(mavlink.MAV_CMD_NAV_WAYPOINT, HOME[0], HOME[1], 0.0),
             (mavlink.MAV_CMD_NAV_TAKEOFF, 0.0, 0.0, 20.0)]
    for i in range(n):
        a = 2 * math.pi * i / n
        items.append((mavlink.MAV_CMD_NAV_WAYPOINT, HOME[0] + 0.0005 * math.cos(a),
                      HOME[1] + 0.0005 * math.sin(a), 20.0))
    items.append((mavlink.MAV_CMD_NAV_RETURN_TO_LAUNCH, 0.0, 0.0, 0.0))
    return items


def send_item(conn: Any, seq: int, item: tuple[int, float, float, float]) -> None:
    cmd, lat, lon, alt = item
    conn.mav.mission_item_int_send(
        1, 1, seq, mavlink.MAV_FRAME_GLOBAL_RELATIVE_ALT_INT, cmd, 0, 1, 0, 0, 0, 0,
        int(lat * 1e7), int(lon * 1e7), alt, mavlink.MAV_MISSION_TYPE_MISSION)


def upload(conn: Any, items: list[tuple[int, float, float, float]], cap: float) -> tuple[bool, float, int]:
    start = time.monotonic()
    attempts = 0
    while time.monotonic() - start < cap:
        attempts += 1
        last: Any = lambda: conn.mav.mission_count_send(1, 1, len(items), 0)  # noqa: E731
        last()
        heard, retries = time.monotonic(), 0
        while time.monotonic() - start < cap:
            msg = conn.recv_match(type=["MISSION_REQUEST", "MISSION_REQUEST_INT", "MISSION_ACK"],
                                  blocking=True, timeout=0.1)
            if msg is not None and msg.get_srcSystem() == 1:
                if msg.get_type() == "MISSION_ACK":
                    if msg.type == mavlink.MAV_MISSION_ACCEPTED:
                        return True, time.monotonic() - start, attempts
                    if msg.type == mavlink.MAV_MISSION_INVALID_SEQUENCE:
                        continue  # an item arrived out of turn; ArduPilot carries on
                    break  # rejected or abandoned: start again
                seq = msg.seq
                if 0 <= seq < len(items):
                    last = lambda s=seq: send_item(conn, s, items[s])  # noqa: E731
                    last()
                    heard, retries = time.monotonic(), 0
                continue
            if time.monotonic() - heard > 1.5:
                retries += 1
                if retries > 5:
                    break
                last()
                heard = time.monotonic()
    return False, cap, attempts


def download(conn: Any) -> list[tuple[int, float, float, float]]:
    """Read the mission back over the clean side channel."""
    conn.mav.mission_request_list_send(1, 1, 0)
    count = conn.recv_match(type="MISSION_COUNT", blocking=True, timeout=5)
    if count is None:
        return []
    out: list[tuple[int, float, float, float]] = []
    for seq in range(count.count):
        conn.mav.mission_request_int_send(1, 1, seq, 0)
        item = conn.recv_match(type="MISSION_ITEM_INT", blocking=True, timeout=5)
        if item is None or item.seq != seq:
            return out
        out.append((item.command, item.x / 1e7, item.y / 1e7, item.z))
    conn.mav.mission_ack_send(1, 1, 0, 0)
    return out


def same(a: list[tuple[int, float, float, float]], b: list[tuple[int, float, float, float]]) -> bool:
    if len(a) != len(b):
        return False
    for i, (x, y) in enumerate(zip(a, b)):
        if i == 0:
            continue  # ArduPilot replaces item 0 with its own home position
        if x[0] != y[0] or any(abs(p - q) > 1e-6 for p, q in zip(x[1:3], y[1:3])):
            return False
        if abs(x[3] - y[3]) > 0.01:
            return False
    return True


def run_upload(args: argparse.Namespace, run: int) -> Upload:
    with ExitStack() as stack, tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        start_rig(stack, args, workdir)
        gcs = connect(P["gcs"])
        side = connect(P["side"])
        request_streams(gcs, args.stream_hz)
        time.sleep(10)  # let telemetry fill the radio
        items = survey(args.items)
        ok, seconds, attempts = upload(gcs, items, args.cap)
        verified = ok and same(items, download(side))
        if args.keep_logs:
            for log in workdir.glob("*.log"):
                (Path(args.keep_logs) / f"{args.mode}-run{run}-{log.name}").write_bytes(log.read_bytes())
        return Upload(ok, seconds, attempts, verified)


def wait_until(conn: Any, cond: Any, timeout: float, what: str) -> Any:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        msg = conn.recv_match(blocking=True, timeout=0.5)
        if msg is not None and msg.get_srcSystem() == 1 and cond(msg):
            return msg
    raise SystemExit(f"timed out waiting for {what}")


def armed(msg: Any) -> bool:
    return msg.get_type() == "HEARTBEAT" and bool(msg.base_mode & mavlink.MAV_MODE_FLAG_SAFETY_ARMED)


def fly(args: argparse.Namespace) -> None:
    with ExitStack() as stack, tempfile.TemporaryDirectory() as tmp:
        start_rig(stack, args, Path(tmp))
        gcs = connect(P["gcs"])
        side = connect(P["side"])
        request_streams(gcs, args.stream_hz)
        request_streams(side, 2)  # position on the clean channel, to check where it landed
        items = flight(6)
        ok, seconds, attempts = upload(gcs, items, args.cap)
        print(f"upload: ok={ok} in {seconds:.1f} s, {attempts} attempt(s), "
              f"verified={ok and same(items, download(side))}", flush=True)
        if not ok:
            raise SystemExit(1)
        gcs.set_mode("GUIDED")
        t0 = time.monotonic()
        while time.monotonic() - t0 < 120:  # pre-arm checks pass once the EKF settles
            gcs.mav.command_long_send(1, 1, mavlink.MAV_CMD_COMPONENT_ARM_DISARM, 0, 1, 0, 0, 0, 0, 0, 0)
            if gcs.recv_match(type="HEARTBEAT", blocking=True, timeout=3) and gcs.motors_armed():
                break
        else:
            raise SystemExit("could not arm")
        print(f"armed after {time.monotonic() - t0:.0f} s", flush=True)
        gcs.mav.command_long_send(1, 1, mavlink.MAV_CMD_NAV_TAKEOFF, 0, 0, 0, 0, 0, 0, 0, 20)
        wait_until(gcs, lambda m: m.get_type() == "GLOBAL_POSITION_INT"
                   and m.relative_alt > 15000, 60, "takeoff")
        print("airborne; switching to AUTO", flush=True)
        gcs.set_mode("AUTO")
        last = len(items) - 1
        reached: set[int] = set()
        end = time.monotonic() + 300
        while time.monotonic() < end:
            msg = side.recv_match(type=["MISSION_ITEM_REACHED", "HEARTBEAT"], blocking=True, timeout=1)
            if msg is None:
                continue
            if msg.get_type() == "MISSION_ITEM_REACHED" and msg.seq not in reached:
                reached.add(msg.seq)
                print(f"reached item {msg.seq}", flush=True)
            if reached and msg.get_type() == "HEARTBEAT" and not armed(msg):
                break
        pos = side.recv_match(type="GLOBAL_POSITION_INT", blocking=True, timeout=5)
        dist = float("nan")
        if pos is not None:
            north = (pos.lat / 1e7 - HOME[0]) * 111_320
            east = (pos.lon / 1e7 - HOME[1]) * 111_320 * math.cos(math.radians(HOME[0]))
            dist = math.hypot(north, east)
        print(f"disarmed after the flight; items reached {sorted(reached)}; "
              f"{dist:.1f} m from home", flush=True)

def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ardupilot", default=os.environ.get("ARDUPILOT", "~/ardupilot"))
    p.add_argument("--mode", choices=["gateway", "direct"], default="gateway")
    p.add_argument("--loss", type=float, default=0.3)
    p.add_argument("--items", type=int, default=101, help="mission items including home")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--cap", type=float, default=300.0)
    p.add_argument("--stream-hz", type=int, default=10)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--fly", action="store_true", help="upload a short mission and fly it")
    p.add_argument("--keep-logs", help="copy each run's process logs into this directory")
    p.add_argument("--instance", type=int, default=0, help="shift ports to run beside another rig")
    args = p.parse_args()

    if args.fly:
        fly(args)
        return
    results = []
    for run in range(args.runs):
        args.seed = run
        r = run_upload(args, run)
        print(f"run {run}: ok={r.ok} verified={r.verified} {r.seconds:.1f} s, "
              f"{r.attempts} attempt(s)", flush=True)
        results.append(r)
    ok = [r.seconds for r in results if r.ok and r.verified]
    print(f"\n{args.mode}: {len(ok)}/{len(results)} uploads completed and verified, "
          f"{args.items - 1} waypoints, {args.loss:.0%} loss, telemetry at {args.stream_hz} Hz")
    if ok:
        print(f"times: {', '.join(f'{s:.1f} s' for s in ok)}; median {statistics.median(ok):.1f} s")


if __name__ == "__main__":
    main()
