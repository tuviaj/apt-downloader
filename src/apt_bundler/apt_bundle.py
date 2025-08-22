from __future__ import annotations
import tarfile
import tempfile
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from config import DEFAULT_ARCH, INCLUDE_RECOMMENDS, OUT_DIR, STATUS_OK, STATUS_ERR

def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)

def _apt_opts(root: Path, arch: str, include_recommends: bool) -> list[str]:
    # Isolated state/cache; re-use container’s /etc/apt sources as-is.
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

def process_job(job: dict) -> dict:
    """
    job: {"distro":"jammy","package":"git","arch":"amd64","include_recommends":true}
    returns a result dict with status, tar_name/path, etc.
    """
    suite = (job.get("distro") or job.get("suite") or "").strip()
    pkg   = (job.get("package") or "").strip()
    arch  = (job.get("arch") or DEFAULT_ARCH).strip()
    include_recommends = bool(job.get("include_recommends", INCLUDE_RECOMMENDS))

    if not suite or not pkg:
        return {"status": STATUS_ERR, "error": "missing 'distro' or 'package'", "job": job}

    root = Path(tempfile.mkdtemp(prefix=f"aptroot-{suite}-{pkg}-"))
    try:
        # minimal dirs
        (root / "var/lib/dpkg").mkdir(parents=True, exist_ok=True)
        (root / "var/lib/apt/lists/partial").mkdir(parents=True, exist_ok=True)
        (root / "var/cache/apt/archives/partial").mkdir(parents=True, exist_ok=True)
        (root / "etc/apt").mkdir(parents=True, exist_ok=True)
        (root / "var/lib/dpkg/status").write_text("")

        opts = _apt_opts(root, arch, include_recommends)

        # refresh indices
        _run(["apt-get", *opts, "update"])

        # download package + deps for the chosen suite, no install
        try:
            _run(["apt-get", *opts, "-t", suite, "install", "-y", "--download-only", pkg])
        except subprocess.CalledProcessError:
            return {
                "status": STATUS_ERR,
                "error": f"apt download failed for {pkg} on {suite} ({arch}); check suite/package and sources",
                "job": job,
            }

        deb_dir = root / "var/cache/apt/archives"
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        tar_name = f"{suite}-{arch}-{pkg}-{ts}.tar.gz"
        out_tar = OUT_DIR / tar_name
        count = _bundle_debs(deb_dir, out_tar)
        if count == 0:
            return {"status": STATUS_ERR, "error": f"No debs produced for {pkg} on {suite} ({arch})", "job": job}

        return {
            "status": STATUS_OK,
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
