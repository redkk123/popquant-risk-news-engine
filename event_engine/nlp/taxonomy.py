from __future__ import annotations

import re
from typing import Any


EVENT_RULES: list[tuple[str, int, list[str]]] = [
    ("m_and_a", 95, [r"\bacquires?\b", r"\bmerger\b", r"\bacquisition\b", r"\bbuyout\b"]),
    (
        "earnings",
        92,
        [
            r"\bearnings\b",
            r"\bquarter results\b",
            r"\bq[1-4]\b",
            r"\beps\b",
            r"\breports? stronger [0-9]{4} results\b",
            r"\bfull year [0-9]{4} results\b",
            r"\bfiscal year [0-9]{4} results\b",
        ],
    ),
    (
        "credit_liquidity",
        91,
        [
            r"\bliquidity\b",
            r"\bdeposit outflows?\b",
            r"\bfunding pressure\b",
            r"\bcredit downgrade\b",
            r"\brating downgrade\b",
            r"\brefinancing risk\b",
            r"\bcapital shortfall\b",
        ],
    ),
    (
        "regulatory_policy",
        90,
        [
            r"\btariff[s]?\b",
            r"\bsanction[s]?\b",
            r"\bexport control[s]?\b",
            r"\bnew rule[s]?\b",
            r"\bpolicy change\b",
            r"\bpolicy proposal\b",
            r"\bregulatory policy\b",
            r"\bgovernment policy\b",
        ],
    ),
    (
        "legal",
        89,
        [
            r"\blawsuit\b",
            r"\bsettlement\b",
            r"\bprobe\b",
            r"\bregulator\b",
            r"\bantitrust\b",
            r"\bjudge blocks?\b",
            r"\btrial\b",
            r"\bclaims?\b",
            r"\bpentagon\b",
            r"\bdefense official\b",
            r"\bsafeguards\b",
            r"\bsame rules as banks\b",
            r"\bregulated like a bank\b",
            r"\bbank charter\b",
            r"\bcharter\b",
            r"\boversight\b",
        ],
    ),
    (
        "guidance",
        88,
        [r"\bguidance\b", r"\boutlook\b", r"\bforecast\b", r"\braises outlook\b", r"\bcuts guidance\b"],
    ),
    (
        "supply_chain",
        87,
        [
            r"\bsupply[- ]chain\b",
            r"\bshipment delay\b",
            r"\bproduction delay\b",
            r"\bcomponent shortage\b",
            r"\bsupplier issue\b",
            r"\bfactory disruption\b",
        ],
    ),
    (
        "downgrade",
        86,
        [
            r"\bdowngrade\b",
            r"\bcut to (hold|sell|underperform)\b",
            r"\blowered rating\b",
            r"\bmaintains? (sell|underperform|underweight) rating\b",
            r"\bcuts? price target\b",
            r"\blowered price target\b",
        ],
    ),
    (
        "upgrade",
        84,
        [
            r"\bupgrade\b",
            r"\braised to buy\b",
            r"\bboosted rating\b",
            r"\bmaintains? buy rating\b",
            r"\breiterates? buy\b",
            r"\braises? price target\b",
            r"\bmaintains? (overweight|outperform) rating\b",
        ],
    ),
    (
        "macro",
        82,
        [
            r"\bcentral bank\b",
            r"\bfed\b",
            r"\bcpi\b",
            r"\binflation\b",
            r"\bgdp\b",
            r"\bpayrolls\b",
            r"\bgeopolitical\b",
            r"\bmiddle east\b",
            r"\boil (prices?|higher|surge|crisis|soaring)\b",
            r"\bmarket concentration\b",
            r"\bwall street\b",
            r"\bu\.s\. stocks lower at close of trade\b",
            r"\bdow jones industrial average down\b",
            r"\bsell[- ]off\b",
            r"\bmarket lower\b",
            r"\bmarket slump[s]?\b",
            r"\bequity market[s]?\b",
            r"\bconflict in iran\b",
            r"\biran attack\b",
            r"\bgeopolitical tensions?\b",
            r"\bcrowded trade\b",
            r"\bbreadth concern\b",
        ],
    ),
    (
        "analyst_note",
        80,
        [
            r"\binitiates? coverage\b",
            r"\bcoverage resumed\b",
            r"\bprice target reiterated\b",
            r"\banalyst note\b",
            r"\bmaintains? neutral rating\b",
            r"\bkeeps? neutral\b",
        ],
    ),
    (
        "capital_return",
        79,
        [
            r"\bbuyback\b",
            r"\bshare repurchase\b",
            r"\brepurchase program\b",
            r"\bdividend increase\b",
            r"\braises? dividend\b",
            r"\bcapital return\b",
        ],
    ),
    ("management", 75, [r"\bceo\b", r"\bcfo\b", r"\bresigns?\b", r"\bappoints?\b", r"\bsteps down\b"]),
    (
        "product_issue",
        74,
        [
            r"\brecall\b",
            r"\boutage\b",
            r"\bservice disruption\b",
            r"\bbug\b",
            r"\bdefect\b",
            r"\bsafety issue\b",
            r"\bproduct issue\b",
        ],
    ),
    (
        "commentary",
        72,
        [
            r"\blong[- ]term buy\b",
            r"\bshould you\b",
            r"\bhere(?:'| i)?s the proof\b",
            r"\bhow to think about\b",
            r"\bstocks always rebound\b",
            r"\bpresents at\b",
            r"\bconference transcript\b",
            r"\btranscript\b",
            r"\bwhat to do about it\b",
            r"\bjoin in\b",
            r"\bdeclines more than market\b",
            r"\bfell more than broader market\b",
            r"\bmore than broader market\b",
            r"\bsome information for investors\b",
            r"\bwhat investors need to know\b",
            r"\bgood stock to buy\b",
            r"\bbetter buy\b",
            r"\bbull case theory\b",
            r"\bweek in review\b",
            r"\bmost popular stocks among hedge funds\b",
            r"\brankings\b",
            r"\bmagnificent seven\b",
            r"\btop stocks\b",
            r"\bmarket recap\b",
            r"\bpriced attractive\b",
            r"\bvaluation opportunity\b",
            r"\bmovement within algorithmic entry frameworks\b",
            r"\bmonthly dividend reits? to buy now\b",
            r"\bform 424b2\b",
            r"\bshares fall amid\b",
        ],
    ),
    (
        "product",
        70,
        [
            r"\blaunch\b",
            r"\brelease\b",
            r"\bproduct\b",
            r"\bdevice\b",
            r"\bplatform\b",
            r"\bunveils?\b",
            r"\bmacbook\b",
            r"\barchitecture\b",
            r"\bchip(s)?\b",
        ],
    ),
]

STRONG_MACRO_PATTERNS = [
    r"\bwall street\b",
    r"\bsell[- ]off\b",
    r"\bmarket lower\b",
    r"\bmarket slump[s]?\b",
    r"\bgeopolitical tensions?\b",
    r"\bmiddle east\b",
    r"\bconflict in iran\b",
    r"\boil (prices?|higher|surge|crisis|soaring)\b",
]

MACRO_SUBTYPE_RULES: list[tuple[str, list[str]]] = [
    ("oil_geopolitical", [r"\boil\b", r"\bgeopolitical\b", r"\bmiddle east\b", r"\bconflict in iran\b"]),
    ("policy_inflation", [r"\bfed\b", r"\bcpi\b", r"\binflation\b", r"\brate cut\b", r"\bcentral bank\b"]),
    (
        "positioning_concentration",
        [r"\bmarket concentration\b", r"\bcrowded trade\b", r"\bmagnificent seven\b", r"\bbreadth concern\b"],
    ),
    (
        "market_color",
        [
            r"\bwall street\b",
            r"\bsell[- ]off\b",
            r"\bmarket lower\b",
            r"\bmarket slump[s]?\b",
            r"\bu\.s\. stocks lower at close of trade\b",
            r"\bdow jones industrial average down\b",
        ],
    ),
]

COMMENTARY_SUBTYPE_RULES: list[tuple[str, list[str]]] = [
    ("opinion", [r"\bshould you\b", r"\bbetter buy\b", r"\blong[- ]term buy\b", r"\bbull case theory\b"]),
    ("recap", [r"\bweek in review\b", r"\brankings\b", r"\bmost popular stocks among hedge funds\b"]),
    ("market_color", [r"\bdeclines more than market\b", r"\bmore than broader market\b", r"\bmarket recap\b"]),
]


def _front_text(document: dict[str, Any]) -> str:
    return " ".join(
        [
            str(document.get("title", "")),
            str(document.get("description", "")),
        ]
    ).lower()


def _full_text(document: dict[str, Any]) -> str:
    return " ".join(
        [
            str(document.get("title", "")),
            str(document.get("description", "")),
            str(document.get("snippet", "")),
        ]
    ).lower()


def _resolve_subtype(event_type: str, front_text: str, full_text: str) -> str | None:
    rules = []
    if event_type == "macro":
        rules = MACRO_SUBTYPE_RULES
    elif event_type == "commentary":
        rules = COMMENTARY_SUBTYPE_RULES

    for subtype, patterns in rules:
        if any(re.search(pattern, front_text) or re.search(pattern, full_text) for pattern in patterns):
            return subtype
    return None


def _story_bucket(event_type: str, event_subtype: str | None) -> str:
    if event_type == "other":
        return "unclear"
    if event_type == "commentary":
        return event_subtype or "commentary"
    if event_type == "macro" and event_subtype == "market_color":
        return "market_color"
    return "event_driven"


def classify_event_type(document: dict[str, Any]) -> dict[str, Any]:
    """Classify a canonical document into a deterministic event taxonomy."""
    front_text = _front_text(document)
    full_text = _full_text(document)

    best_match: dict[str, Any] | None = None
    candidates_by_type: dict[str, dict[str, Any]] = {}
    for event_type, priority, patterns in EVENT_RULES:
        matched_front = [pattern for pattern in patterns if re.search(pattern, front_text)]
        matched_full = [pattern for pattern in patterns if re.search(pattern, full_text)]
        matched = matched_front or matched_full
        if matched:
            confidence = min(0.95, 0.55 + 0.08 * len(set(matched)))
            candidate = {
                "event_type": event_type,
                "event_confidence": float(confidence),
                "event_reasons": matched,
                "_score": (priority, len(matched_front), len(set(matched_full))),
            }
            candidates_by_type[event_type] = candidate
            if not best_match or candidate["_score"] > best_match["_score"]:
                best_match = candidate

    macro_candidate = candidates_by_type.get("macro")
    if macro_candidate and any(re.search(pattern, front_text) for pattern in STRONG_MACRO_PATTERNS):
        best_match = dict(macro_candidate)

    if best_match:
        event_type = str(best_match["event_type"])
        event_subtype = _resolve_subtype(event_type, front_text, full_text)
        story_bucket = _story_bucket(event_type, event_subtype)
        best_match.pop("_score", None)
        best_match["event_subtype"] = event_subtype
        best_match["story_bucket"] = story_bucket
        return best_match

    return {
        "event_type": "other",
        "event_subtype": None,
        "story_bucket": "unclear",
        "event_confidence": 0.25,
        "event_reasons": ["no_rule_match"],
    }
