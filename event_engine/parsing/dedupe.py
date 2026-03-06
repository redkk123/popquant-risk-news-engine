from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


TRACKING_PARAMS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
}


def canonicalize_url(url: str) -> str:
    """Normalize URLs to improve duplicate detection."""
    if not url:
        return ""

    split = urlsplit(url.strip())
    netloc = split.netloc.lower()
    path = split.path.rstrip("/")
    filtered_query = [
        (key, value)
        for key, value in parse_qsl(split.query, keep_blank_values=True)
        if key.lower() not in TRACKING_PARAMS
    ]
    query = urlencode(filtered_query)
    return urlunsplit((split.scheme.lower(), netloc, path, query, ""))


def normalized_title_key(title: str) -> str:
    """Generate a normalized hash key from a title."""
    normalized = re.sub(r"[^a-z0-9 ]+", " ", title.lower()).strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()

