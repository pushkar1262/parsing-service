"""Address validation for fetches over HTTP, and why it is not optional.

A presigned URL arrives in a job message. That message was produced by the
user-facing backend, from a reference that traces back to something a user did. If
this service will GET any URL it is handed, then whoever can influence that reference
can make the worker issue requests from *inside* the VPC, with the worker's identity.

The specific attack that matters:

    http://169.254.169.254/latest/meta-data/iam/security-credentials/

That is the EC2 instance metadata service. On IMDSv1 it answers, unauthenticated,
with the credentials of the worker's IAM role — which can read every document in the
bucket. The response would come back to us as a "document", get parsed, stored as
parsed content, and served over the content API to anything that can ask. A
credential exfiltration path dressed as a text file.

The same shape reaches Redis on localhost, a Postgres port, the Kubernetes API, and
any internal admin page that trusts the network. So:

- **Resolve first, judge the addresses.** A hostname says nothing; `evil.test` can
  have an A record of 127.0.0.1. Every address the name resolves to must be public.
- **Refuse redirects.** A 302 from a permitted host to `169.254.169.254` defeats a
  host check performed only on the original URL, which is why the check has to apply
  to whatever we actually connect to — and the cheapest way to guarantee that is to
  never follow a redirect.
- **Deny by default, allow explicitly.** `allow_private_networks` exists because a
  deployment behind an S3 VPC endpoint resolves the bucket to a private address, and
  that is legitimate. It is off unless configured, and it is the kind of flag that
  belongs in a reviewed config file rather than an environment default.

Residual risk, stated rather than hidden: between validating the resolved addresses
and the socket connecting, the name could be re-resolved to a different address (DNS
rebinding). Closing that fully means connecting to the validated IP and carrying the
hostname in the `Host` header, which breaks TLS certificate validation unless done
carefully. The window is narrow and the high-value targets (IMDS, loopback) are also
reachable only via addresses this check rejects, so the practical exposure is small —
but a deployment handling genuinely untrusted URLs should prefer an egress proxy with
an allowlist over relying on this alone.
"""

from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass, field
from urllib.parse import urlsplit

from domain.errors import BlockedAddress

# Ranges that must never be fetched from. `is_global` covers most of this, but the
# metadata addresses are called out because they are the reason this module exists and
# a future refactor should not be able to quietly drop them.
_METADATA_ADDRESSES = frozenset(
    {
        "169.254.169.254",  # AWS / Azure / GCP instance metadata
        "fd00:ec2::254",  # AWS IMDS over IPv6
        "100.100.100.200",  # Alibaba Cloud metadata
    }
)


@dataclass(frozen=True)
class NetPolicy:
    """What this deployment is allowed to fetch over HTTP.

    Defaults are the strict ones. A deployment that needs more says so explicitly.
    """

    allow_schemes: frozenset[str] = frozenset({"https"})
    # Empty means "any public host". A non-empty tuple is an allowlist, which is what
    # you want once you know the one bucket domain documents arrive from.
    allowed_host_suffixes: tuple[str, ...] = ()
    allow_private_networks: bool = False
    connect_timeout_s: float = 10.0
    read_timeout_s: float = 60.0
    resolver: object = field(default=None, repr=False, compare=False)

    def resolve(self, host: str, port: int) -> list[str]:
        """DNS lookup, injectable so the guard can be tested without a network."""
        if self.resolver is not None:
            return list(self.resolver(host, port))  # type: ignore[operator]
        try:
            infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
        except socket.gaierror as exc:
            raise BlockedAddress(f"cannot resolve {host!r}: {exc}") from exc
        return [info[4][0] for info in infos]


def _address_is_permitted(address: str) -> tuple[bool, str]:
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError:
        return False, f"{address!r} is not an IP address"

    if address in _METADATA_ADDRESSES or parsed.is_link_local:
        return False, (
            f"{address} is a link-local address — this is the cloud instance "
            f"metadata range, and fetching from it would expose the worker's own "
            f"credentials"
        )
    if parsed.is_loopback:
        return False, f"{address} is loopback"
    if parsed.is_private:
        return False, f"{address} is a private address"
    if parsed.is_reserved or parsed.is_multicast or parsed.is_unspecified:
        return False, f"{address} is reserved, multicast or unspecified"

    # IPv4-mapped and translated IPv6 forms are a common way to smuggle a private
    # address past a naive check (::ffff:169.254.169.254).
    mapped = getattr(parsed, "ipv4_mapped", None) or getattr(parsed, "sixtofour", None)
    if mapped is not None:
        return _address_is_permitted(str(mapped))

    return True, ""


def validate_url(url: str, policy: NetPolicy) -> list[str]:
    """Check a URL is safe to fetch and return the addresses it resolves to.

    Raises `BlockedAddress` — a permanent failure — rather than returning a boolean,
    because there is no caller for whom "blocked" is a recoverable condition worth
    branching on, and an ignored boolean is how this class of control gets bypassed.
    """
    parts = urlsplit(url)
    scheme = (parts.scheme or "").lower()

    if scheme not in policy.allow_schemes:
        raise BlockedAddress(
            f"scheme {scheme!r} is not permitted "
            f"(allowed: {', '.join(sorted(policy.allow_schemes))})"
        )
    if not parts.hostname:
        raise BlockedAddress(f"no host in {url!r}")

    host = parts.hostname.lower().rstrip(".")

    if policy.allowed_host_suffixes and not any(
        host == suffix.lstrip(".") or host.endswith(suffix)
        for suffix in policy.allowed_host_suffixes
    ):
        raise BlockedAddress(
                f"host {host!r} is not in the allowlist "
                f"({', '.join(policy.allowed_host_suffixes)})"
            )

    port = parts.port or (443 if scheme == "https" else 80)
    addresses = policy.resolve(host, port)
    if not addresses:
        raise BlockedAddress(f"{host!r} resolved to no addresses")

    if policy.allow_private_networks:
        return addresses

    # Every address, not just the first: a name with both a public and a private A
    # record must not be fetchable, because which one the socket picks is not ours to
    # decide.
    for address in addresses:
        permitted, reason = _address_is_permitted(address)
        if not permitted:
            raise BlockedAddress(f"{host!r} resolves to a blocked address: {reason}")
    return addresses
