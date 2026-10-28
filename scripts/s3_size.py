#!/usr/bin/env python3
"""List or delete objects under a prefix with only the Python standard library.

The benchmark needs object counts and byte sizes for storage/compression ratios.
Pulling in an SDK just for that is unnecessary, so this implements the small
SigV4 subset required by S3's ListObjectsV2 and DeleteObject APIs.

Environment:
  S3_ENDPOINT      default http://127.0.0.1:9000
  S3_BUCKET        default indexlake
  S3_ACCESS_KEY    default admin
  S3_SECRET_KEY    default password
  S3_REGION        default us-east-1
"""

from __future__ import annotations

import argparse
import datetime
import hashlib
import hmac
import os
import urllib.parse
import urllib.request
from xml.etree import ElementTree as ET

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:9000").rstrip("/")
BUCKET = os.environ.get("S3_BUCKET", "indexlake")
ACCESS = os.environ.get("S3_ACCESS_KEY", "admin")
SECRET = os.environ.get("S3_SECRET_KEY", "password")
REGION = os.environ.get("S3_REGION", "us-east-1")
HOST = urllib.parse.urlparse(ENDPOINT).netloc or ENDPOINT
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


def _sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def _signing_key(date: str) -> bytes:
    key = _sign(("AWS4" + SECRET).encode(), date)
    key = _sign(key, REGION)
    key = _sign(key, "s3")
    return _sign(key, "aws4_request")


def _request(method: str, path: str, query: dict[str, str] | None = None, body: bytes = b"") -> bytes:
    canonical_query = urllib.parse.urlencode(
        sorted((query or {}).items()), quote_via=urllib.parse.quote
    )
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    canonical_headers = (
        f"host:{HOST}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join(
        [method, path, canonical_query, canonical_headers, signed_headers, payload_hash]
    )
    scope = f"{date}/{REGION}/s3/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )
    signature = hmac.new(
        _signing_key(date), string_to_sign.encode(), hashlib.sha256
    ).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={ACCESS}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    url = ENDPOINT + path + ("?" + canonical_query if canonical_query else "")
    request = urllib.request.Request(
        url,
        data=body or None,
        method=method,
        headers={
            "Host": HOST,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
            "Authorization": authorization,
        },
    )
    # Never send MinIO traffic through a configured HTTP proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request) as response:
        return response.read()


def _list_page(prefix: str, token: str | None) -> tuple[list[tuple[str, int]], str | None]:
    query = {"list-type": "2", "prefix": prefix, "max-keys": "1000"}
    if token:
        query["continuation-token"] = token
    root = ET.fromstring(_request("GET", f"/{BUCKET}", query))
    objects = []
    for item in root.findall("s3:Contents", S3_NS):
        key = item.findtext("s3:Key", "", S3_NS)
        size = int(item.findtext("s3:Size", "0", S3_NS))
        objects.append((key, size))
    next_token = None
    if root.findtext("s3:IsTruncated", "false", S3_NS) == "true":
        next_token = root.findtext("s3:NextContinuationToken", None, S3_NS)
    return objects, next_token


def list_objects(prefix: str) -> list[tuple[str, int]]:
    objects: list[tuple[str, int]] = []
    token = None
    while True:
        page, token = _list_page(prefix, token)
        objects.extend(page)
        if token is None:
            return objects


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    size = sub.add_parser("size", help="print object count and bytes under a prefix")
    size.add_argument("prefix")
    listing = sub.add_parser("ls", help="print object keys and sizes under a prefix")
    listing.add_argument("prefix")
    remove = sub.add_parser("rm", help="delete every object under a prefix")
    remove.add_argument("prefix")
    args = parser.parse_args()

    if args.command == "ls":
        for key, byte_size in list_objects(args.prefix):
            print(key, byte_size)
    elif args.command == "size":
        objects = list_objects(args.prefix)
        total = sum(size for _, size in objects)
        print(
            f"objects={len(objects)} bytes={total} "
            f"mib={total / 1024 / 1024:.2f}"
        )
    elif args.command == "rm":
        objects = list_objects(args.prefix)
        for key, _ in objects:
            _request("DELETE", f"/{BUCKET}/{urllib.parse.quote(key)}")
        print(f"deleted {len(objects)} objects under {args.prefix!r}")


if __name__ == "__main__":
    main()
