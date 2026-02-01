from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

from .vendor_dolma import ensure_vendor_dolma_on_path


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


_WS_RE = re.compile(r"\s+")


def normalize_text(text: str, *, unicode_form: str = "NFC", collapse_whitespace: bool = True) -> str:
    if text is None:
        return ""
    if unicode_form and unicode_form.lower() != "none":
        text = unicodedata.normalize(unicode_form, text)
    # Remove NULs which can break some downstream tooling
    text = text.replace("\x00", "")
    if collapse_whitespace:
        text = _WS_RE.sub(" ", text).strip()
    return text


def stable_id(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        if p is None:
            continue
        h.update(p.encode("utf-8", errors="ignore"))
        h.update(b"\0")
    return h.hexdigest()


def _merge_spans(spans: Iterable[Tuple[int, int]]) -> List[Tuple[int, int]]:
    merged: List[Tuple[int, int]] = []
    for s, e in sorted(spans, key=lambda x: (x[0], x[1])):
        if e <= s:
            continue
        if not merged or s > merged[-1][1]:
            merged.append((s, e))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
    return merged


def mask_spans(text: str, spans: Iterable[Tuple[int, int]], *, mask_token: str = "<PII>") -> str:
    merged = _merge_spans(spans)
    if not merged:
        return text
    out: List[str] = []
    cur = 0
    for s, e in merged:
        if s > cur:
            out.append(text[cur:s])
        out.append(mask_token)
        cur = e
    if cur < len(text):
        out.append(text[cur:])
    return "".join(out)


class DolmaToolkitPIIMasker:
    """
    Bridge that uses Dolma span taggers (e.g. pii_regex_v2) to produce spans,
    then masks those spans in the text.

    Works with either:
    - `pip install dolma`
    - vendored clone at `vendor/dolma/python`
    """

    def __init__(self, tagger_name: str = "pii_regex_v2") -> None:
        ensure_vendor_dolma_on_path()
        from dolma import TaggerRegistry  # type: ignore

        tagger_cls = TaggerRegistry.get(tagger_name)
        self.tagger_name = tagger_name
        self.tagger = tagger_cls()

    def find_pii_spans(self, *, doc_id: str, text: str, source: str) -> List[Tuple[int, int]]:
        ensure_vendor_dolma_on_path()
        from dolma.core.data_types import InputSpec  # type: ignore

        spec = InputSpec(id=doc_id, text=text, source=source, created="", added=_iso_now(), version=None)
        tag_out: Dict[str, List[Tuple[int, int, float]]] = self.tagger.tag(spec)  # type: ignore[arg-type]
        spans: List[Tuple[int, int]] = []
        for _, vals in tag_out.items():
            for s, e, _score in vals:
                spans.append((int(s), int(e)))
        return spans

