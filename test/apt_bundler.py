#!/usr/bin/env python3
import os
import json
import tarfile
import tempfile
import shutil
import subprocess
import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# -----------------------------
# Config (env overridable)
# -----------------------------
DEF_OUT_DIR = Path(os.getenv("OUT_DIR", "/tmp"))
DEF_OUT_DIR.mkdir(parents=True, exist_ok=True)

DEF_ARCH = os.getenv("ARCH", "amd64")
DEF_INCLUDE_RECOMMENDS = os.getenv("INCLUDE_RECOMMENDS", "1").lower() in ("1", "true", "yes")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_CONSUME_TOPIC = os.getenv("CONSUME_TOPIC", "apt.requests")
KAFKA_PRODUCE_TOPIC = os.getenv("PRODUCE_TOPIC", "apt.results")
KAFKA_GROUP_ID = os.getenv("GROUP_ID", "apt-bundler-1")


# -----------------------------
# Core APT bundling logic
# -----------------------------
def _run(cmd: List[str]) -> None:
    subprocess.run(cmd, check=True)

def _apt_opts(root: Path, arch: str, include_recommends: bool) -> List[str]:
    """
    Use isolated apt state/cache under 'root' but reuse the container/host sources:
      /etc/apt/sources.list and /etc/apt/sources.list.d/*
    """
    return [
        "-o", "Debug::NoLocking=1",
        "-o", f"Dir={root}",
        "-o", f"Dir::State={root}/var/lib/apt",
        "-o", f"Dir::Cache={root}/var/cache/apt",
        "-o", "Dir::Etc::sourcelist=/etc/apt/sources.list",
        "-o", "Dir::Etc::sourceparts=/etc/apt/sources.list.d",
        "-o", f"Dir::Etc::Preferences={root}/etc/apt/preferences",
        "-o", f"Dir::State::status={root}/var/lib/dpkg/status",
        "-o", "Dir::Etc::trusted=/etc/apt/trusted.gpg",
        "-o", "Dir::Etc::trustedparts=/etc/apt/trusted.gpg.d",
        "-o", f"APT::Architecture={arch}",
        "-o", f"APT::Install-Recommends={'1' if include_recommends else '0'}",
    ]

def _bundle_debs(deb_dir: Path, out_path: Path) -> int:
    debs = sorted(deb_dir.glob("*.deb"))
    if not debs:
        return 0
    with tarfile.open(out_path, "w:gz") as tar:
        for p in debs:
            tar.add(p, arcname=p.name)
    return len(debs)

def process_job(job: Dict[str, Any],
                default_out: Path = DEF_OUT_DIR,
                default_arch: str = DEF_ARCH,
                default_include_recommends: bool = DEF_INCLUDE_RECOMMENDS) -> Dict[str, Any]:
    """
    job: {
      "distro": "jammy",
      "package": "git",
      "arch": "amd64",                 # optional
      "include_recommends": true,      # optional
      "out_dir": "/out"                # optional
    }
    Returns result dict.
    """
    suite = (job.get("distro") or job.get("suite") or "").strip()
    pkg   = (job.get("package") or "").strip()
    arch  = (job.get("arch") or default_arch).strip()
    include_recommends = bool(job.get("include_recommends", default_include_recommends))
    out_dir = Path(job.get("out_dir", str(default_out)))
    out_dir.mkdir(parents=True, exist_ok=True)

    if not suite or not pkg:
        return {"status": "error", "error": "missing 'distro' or 'package'", "job": job}

    root = Path(tempfile.mkdtemp(prefix=f"aptroot-{suite}-{pkg}-"))
    try:
        # minimal dpkg/apt structure
        (root / "var/lib/dpkg").mkdir(parents=True, exist_ok=True)
        (root / "var/lib/apt/lists/partial").mkdir(parents=True, exist_ok=True)
        (root / "var/cache/apt/archives/partial").mkdir(parents=True, exist_ok=True)
        (root / "etc/apt").mkdir(parents=True, exist_ok=True)
        (root / "var/lib/dpkg/status").write_text("")

        opts = _apt_opts(root, arch, include_recommends)

        # refresh indices from your preconfigured multi-suite sources
        _run(["apt-get", *opts, "update"])

        # download package + deps for the requested suite (no install into system)
        try:
            _run(["apt-get", *opts, "-t", suite, "install", "-y", "--download-only", pkg])
        except subprocess.CalledProcessError:
            return {
                "status": "error",
                "error": f"apt download failed for {pkg} on {suite} ({arch}); "
                         f"check suite/package and your /etc/apt/sources*.list",
                "job": job,
            }

        deb_dir = root / "var/cache/apt/archives"
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        tar_name = f"{suite}-{arch}-{pkg}-{ts}.tar.gz"
        out_tar = out_dir / tar_name
        count = _bundle_debs(deb_dir, out_tar)
        if count == 0:
            return {"status": "error",
                    "error": f"No debs produced for {pkg} on {suite} ({arch})",
                    "job": job}

        return {
            "status": "ok",
            "distro": suite,
            "package": pkg,
            "arch": arch,
            "include_recommends": include_recommends,
            "files_count": count,
            "tar_path": str(out_tar),
            "tar_name": tar_name,
        }
    finally:
        shutil.rmtree(root, ignore_errors=True)


# -----------------------------
# Kafka helpers (lazy import)
# -----------------------------
def kafka_consume_and_process(bootstrap: str,
                              consume_topic: str,
                              produce_topic: str,
                              group_id: str) -> None:
    from confluent_kafka import Consumer, Producer  # lazy import

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": bootstrap})
    consumer.subscribe([consume_topic])

    print(f"[worker] listening on '{consume_topic}', producing to '{produce_topic}', out -> {DEF_OUT_DIR}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[worker] consumer error: {msg.error()}")
                continue

            try:
                job = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                err = {"status": "error", "error": f"invalid JSON: {e}"}
                producer.produce(produce_topic, json.dumps(err).encode("utf-8"))
                producer.flush()
                consumer.commit(msg)
                continue

            result = process_job(job)
            producer.produce(produce_topic, json.dumps(result).encode("utf-8"))
            producer.flush()
            consumer.commit(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def kafka_produce_test(bootstrap: str,
                       topic: str,
                       distro: str,
                       package: str,
                       arch: str,
                       include_recommends: bool,
                       out_dir: Optional[str]) -> None:
    from confluent_kafka import Producer  # lazy import
    producer = Producer({"bootstrap.servers": bootstrap})
    job = {
        "distro": distro,
        "package": package,
        "arch": arch,
        "include_recommends": include_recommends,
    }
    if out_dir:
        job["out_dir"] = out_dir
    producer.produce(topic, json.dumps(job).encode("utf-8"))
    producer.flush()
    print(f"[test-produce] sent -> topic='{topic}': {json.dumps(job)}")


# -----------------------------
# CLI
# -----------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Download Ubuntu APT package + deps for a target distro into a tar.gz"
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    # Local single-run (no Kafka)
    once = sub.add_parser("once", help="Run a single job locally and print JSON result")
    once.add_argument("--distro", required=True, help="Ubuntu suite (e.g. bionic, focal, jammy, noble)")
    once.add_argument("--package", required=True, help="Package name (e.g. git, curl)")
    once.add_argument("--arch", default=DEF_ARCH, help=f"Target arch (default: {DEF_ARCH})")
    once.add_argument("--include-recommends", action="store_true",
                      default=DEF_INCLUDE_RECOMMENDS,
                      help=f"Include APT Recommends (default: {DEF_INCLUDE_RECOMMENDS})")
    once.add_argument("--out-dir", default=str(DEF_OUT_DIR), help=f"Output dir (default: {DEF_OUT_DIR})")

    # Kafka worker
    k = sub.add_parser("kafka", help="Run as Kafka worker (consume requests, produce results)")
    k.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    k.add_argument("--consume-topic", default=KAFKA_CONSUME_TOPIC)
    k.add_argument("--produce-topic", default=KAFKA_PRODUCE_TOPIC)
    k.add_argument("--group-id", default=KAFKA_GROUP_ID)

    # Produce a test request to Kafka
    tp = sub.add_parser("test-produce", help="Produce a sample request to the input Kafka topic")
    tp.add_argument("--bootstrap", default=KAFKA_BOOTSTRAP)
    tp.add_argument("--topic", default=KAFKA_CONSUME_TOPIC)
    tp.add_argument("--distro", required=True)
    tp.add_argument("--package", required=True)
    tp.add_argument("--arch", default=DEF_ARCH)
    tp.add_argument("--include-recommends", action="store_true", default=DEF_INCLUDE_RECOMMENDS)
    tp.add_argument("--out-dir", default=None)

    return p

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "once":
        job = {
            "distro": args.distro,
            "package": args.package,
            "arch": args.arch,
            "include_recommends": bool(args.include_recommends),
            "out_dir": args.out_dir,
        }
        result = process_job(job)
        print(json.dumps(result, indent=2))
        return

    if args.cmd == "kafka":
        kafka_consume_and_process(
            bootstrap=args.bootstrap,
            consume_topic=args.consume_topic,
            produce_topic=args.produce_topic,
            group_id=args.group_id,
        )
        return

    if args.cmd == "test-produce":
        kafka_produce_test(
            bootstrap=args.bootstrap,
            topic=args.topic,
            distro=args.distro,
            package=args.package,
            arch=args.arch,
            include_recommends=bool(args.include_recommends),
            out_dir=args.out_dir,
        )
        return

if __name__ == "__main__":
    main()

