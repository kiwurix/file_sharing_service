"""Persistent self-signed TLS certificate creation."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

def ensure_certificate(root: Path) -> tuple[Path, Path]:
    cert, key = root / "server-cert.pem", root / "server-key.pem"
    if cert.exists() and key.exists():
        return cert, key
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID
    except ImportError as exc:
        raise RuntimeError("Install dependencies first: pip install ./shared ./server") from exc
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "File Sharing Relay")])
    now = datetime.now(timezone.utc)
    certificate = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(private.public_key())
                   .serial_number(x509.random_serial_number()).not_valid_before(now - timedelta(minutes=1))
                   .not_valid_after(now + timedelta(days=3650)).add_extension(x509.BasicConstraints(ca=True, path_length=None), True)
                   .sign(private, hashes.SHA256()))
    key.write_bytes(private.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    key.chmod(0o600)
    cert.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    return cert, key
