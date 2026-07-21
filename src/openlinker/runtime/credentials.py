from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import tempfile
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from .types import (
    RUNTIME_CONTRACT_DIGEST,
    RUNTIME_CONTRACT_ID,
    RUNTIME_PROTOCOL_VERSION,
    RUNTIME_REQUIRED_FEATURES,
    RuntimeMTLS,
)


_CREDENTIAL_FILE = "runtime-credential.json"
_CERT_FILE = "runtime-client-chain.pem"
_KEY_FILE = "runtime-client-key.pem"
_CA_FILE = "runtime-ca.pem"
_FORMAT = 1


@dataclass
class RuntimeCredentialIdentity:
    node_id: str
    agent_id: str


class RuntimeCredentialManager:
    def __init__(
        self,
        *,
        data_dir: Path,
        credential_endpoint: str,
        agent_token: str,
        node_id: str,
        agent_id: str,
        node_version: str,
        capacity: int,
        logger: Any = None,
    ) -> None:
        self.data_dir = data_dir.resolve()
        self.endpoint = _validate_endpoint(credential_endpoint)
        self.agent_token = agent_token
        self.configured_node_id = node_id
        self.configured_agent_id = agent_id
        self.node_version = node_version
        self.capacity = capacity
        self.logger = logger
        self._disk: dict[str, Any] = {}
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None

    async def open(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(self.data_dir, 0o700)
        self._disk = await asyncio.to_thread(self._load_or_create)

    @property
    def identity(self) -> RuntimeCredentialIdentity:
        agent_id = str(self._disk.get("agent_id", ""))
        if not agent_id:
            raise RuntimeError("Runtime credential has not been enrolled")
        return RuntimeCredentialIdentity(str(self._disk["node_id"]), agent_id)

    @property
    def mtls(self) -> RuntimeMTLS:
        if not self._disk.get("certificate_chain_pem"):
            raise RuntimeError("Runtime mTLS credential is unavailable")
        return RuntimeMTLS(
            cert_file=str(self.data_dir / _CERT_FILE),
            key_file=str(self.data_dir / _KEY_FILE),
            ca_file=str(self.data_dir / _CA_FILE),
        )

    async def ensure(self, force: bool = False) -> None:
        async with self._lock:
            if not force and not self._needs_renewal():
                return
            await self._issue()

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._renew_loop())

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _renew_loop(self) -> None:
        while True:
            renew_after = _parse_time(self._disk.get("renew_after"))
            wait = max(1.0, (renew_after - datetime.now(timezone.utc)).total_seconds())
            await asyncio.sleep(wait)
            try:
                await self.ensure()
            except Exception as exc:  # pragma: no cover - logging branch
                if self.logger is not None:
                    self.logger.warning("Runtime certificate renewal failed; retrying: %s", exc)
                await asyncio.sleep(300)

    def _needs_renewal(self) -> bool:
        if not self._disk.get("certificate_chain_pem"):
            return True
        now = datetime.now(timezone.utc)
        return now + timedelta(minutes=5) >= _parse_time(
            self._disk.get("not_after")
        ) or now >= _parse_time(self._disk.get("renew_after"))

    async def _issue(self) -> None:
        private_key = serialization.load_pem_private_key(
            str(self._disk["private_key_pem"]).encode(), password=None
        )
        csr = (
            x509.CertificateSigningRequestBuilder()
            .subject_name(
                x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "OpenLinker Runtime Node")])
            )
            .sign(private_key, hashes.SHA256())
        )
        payload = {
            "node_id": self._disk["node_id"],
            "display_name": f"runtime-{str(self._disk['node_id']).replace('-', '')[:12]}",
            "node_version": self.node_version,
            "protocol_version": RUNTIME_PROTOCOL_VERSION,
            "runtime_contract_id": RUNTIME_CONTRACT_ID,
            "runtime_contract_digest": RUNTIME_CONTRACT_DIGEST,
            "features": list(RUNTIME_REQUIRED_FEATURES),
            "capacity": self.capacity,
            "csr_pem": csr.public_bytes(serialization.Encoding.PEM).decode(),
        }
        async with httpx.AsyncClient(timeout=15, follow_redirects=False, trust_env=False) as client:
            response = await client.post(
                self.endpoint,
                json=payload,
                headers={
                    "Authorization": f"Bearer {self.agent_token}",
                    "Accept": "application/json",
                    "X-OpenLinker-SDK": "openlinker-python/runtime-worker",
                },
            )
        if response.status_code != 200:
            raise RuntimeError(
                f"Runtime certificate request failed with HTTP {response.status_code}"
            )
        if len(response.content) > 64 * 1024:
            raise RuntimeError("Runtime certificate response exceeds 64 KiB")
        issued = response.json()
        self._validate_response(issued)
        self._disk.update(
            agent_id=issued["agent_id"],
            certificate_chain_pem=issued["certificate_chain_pem"],
            trust_bundle_pem=issued["trust_bundle_pem"],
            certificate_serial=str(issued["certificate_serial"]).lower(),
            not_before=issued["not_before"],
            not_after=issued["not_after"],
            renew_after=issued["renew_after"],
        )
        await asyncio.to_thread(self._persist)

    def _validate_response(self, issued: Any) -> None:
        if not isinstance(issued, dict):
            raise RuntimeError("Runtime certificate response is invalid")
        not_before = _parse_time(issued.get("not_before"))
        not_after = _parse_time(issued.get("not_after"))
        renew_after = _parse_time(issued.get("renew_after"))
        if (
            issued.get("node_id") != self._disk["node_id"]
            or not _uuid(str(issued.get("agent_id", "")))
            or issued.get("public_key_thumbprint") != self._disk["public_key_thumbprint"]
            or "BEGIN CERTIFICATE" not in str(issued.get("certificate_chain_pem", ""))
            or "BEGIN CERTIFICATE" not in str(issued.get("trust_bundle_pem", ""))
            or not_after <= not_before
            or not timedelta(hours=23, minutes=50)
            <= not_after - not_before
            <= timedelta(hours=24, minutes=10)
            or not (not_before < renew_after < not_after)
        ):
            raise RuntimeError("Runtime certificate response is invalid")

    def _load_or_create(self) -> dict[str, Any]:
        path = self.data_dir / _CREDENTIAL_FILE
        if path.exists():
            stat = path.lstat()
            if not path.is_file() or stat.st_mode & 0o077 or not 0 < stat.st_size <= 64 * 1024:
                raise RuntimeError("Runtime credential file is corrupt or not private")
            disk = json.loads(path.read_text())
            checksum = str(disk.pop("checksum", ""))
            if disk.get("format") != _FORMAT or not _uuid(str(disk.get("node_id", ""))):
                raise RuntimeError("Runtime credential file is corrupt")
            if not hmac.compare_digest(checksum, _checksum(disk)):
                raise RuntimeError("Runtime credential file checksum is invalid")
            disk["checksum"] = checksum
            if self.configured_node_id and disk["node_id"] != self.configured_node_id:
                raise RuntimeError("configured node_id differs from the key bound to data_dir")
            if self.configured_agent_id and disk.get("agent_id") not in (
                None,
                "",
                self.configured_agent_id,
            ):
                raise RuntimeError(
                    "configured agent_id differs from the credential bound to data_dir"
                )
            return disk
        node_id = self.configured_node_id or str(uuid.uuid4())
        if not _uuid(node_id):
            raise ValueError("node_id must be a non-zero lowercase UUID")
        key = ec.generate_private_key(ec.SECP256R1())
        private_pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode()
        spki = key.public_key().public_bytes(
            serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        disk = {
            "format": _FORMAT,
            "node_id": node_id,
            "private_key_pem": private_pem,
            "public_key_thumbprint": hashlib.sha256(spki).hexdigest(),
            "checksum": "",
        }
        self._disk = disk
        self._persist()
        return disk

    def _persist(self) -> None:
        disk = dict(self._disk)
        disk.pop("checksum", None)
        disk["checksum"] = _checksum(disk)
        self._disk["checksum"] = disk["checksum"]
        _atomic_private(
            self.data_dir / _CREDENTIAL_FILE, json.dumps(disk, separators=(",", ":")).encode()
        )
        _atomic_private(self.data_dir / _KEY_FILE, str(disk["private_key_pem"]).encode())
        if disk.get("certificate_chain_pem"):
            _atomic_private(self.data_dir / _CERT_FILE, str(disk["certificate_chain_pem"]).encode())
            _atomic_private(self.data_dir / _CA_FILE, str(disk["trust_bundle_pem"]).encode())


def _atomic_private(path: Path, value: bytes) -> None:
    fd, temporary = tempfile.mkstemp(prefix=".openlinker-", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb", closefd=True) as output:
            output.write(value)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _checksum(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _parse_time(value: Any) -> datetime:
    if not isinstance(value, str) or not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _validate_endpoint(value: str) -> str:
    parsed = urlparse(value.strip())
    loopback = parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    if (
        not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.fragment
        or not (parsed.scheme == "https" or (parsed.scheme == "http" and loopback))
    ):
        raise ValueError("Runtime credential endpoint must use HTTPS")
    return parsed.geturl()


def _uuid(value: str) -> bool:
    try:
        parsed = uuid.UUID(value)
    except ValueError:
        return False
    return parsed.int != 0 and str(parsed) == value
