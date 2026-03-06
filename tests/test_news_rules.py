from __future__ import annotations

from event_engine.nlp.entity_linking import link_document_tickers, load_alias_table
from event_engine.nlp.sentiment import score_polarity
from event_engine.nlp.severity import score_severity
from event_engine.nlp.taxonomy import classify_event_type
from event_engine.quality import assess_event_quality


ALIAS_TABLE = load_alias_table("D:/Playground/popquant_1_month/config/news_entity_aliases.csv")


def test_entity_linking_uses_alias_fallback() -> None:
    document = {
        "title": "Google launches new enterprise AI tools",
        "description": "Alphabet unveiled a product suite for enterprise users.",
        "snippet": "",
        "entity_names": [],
        "symbols": [],
    }
    result = link_document_tickers(document, ALIAS_TABLE)
    assert "GOOGL" in result["tickers"]
    assert result["link_confidence"] >= 0.65


def test_entity_linking_drops_body_only_provider_false_positive() -> None:
    document = {
        "title": "Rumble Reports Fourth Quarter and Full Year 2025 Results",
        "description": "Revenue surpassed $100 million for the first time in company history.",
        "snippet": "Sequential growth accelerated in Q4.",
        "entity_names": ["Apple Inc."],
        "symbols": ["AAPL"],
        "entities": [
            {
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "match_score": 2.93,
            }
        ],
    }
    result = link_document_tickers(document, ALIAS_TABLE)
    assert result["tickers"] == []
    assert result["link_confidence"] == 0.0


def test_entity_linking_keeps_provider_symbol_when_title_is_anchored() -> None:
    document = {
        "title": "Apple cuts guidance after weaker demand in China",
        "description": "Apple lowered its outlook after weak iPhone demand.",
        "snippet": "",
        "entity_names": ["Apple Inc."],
        "symbols": ["AAPL"],
        "entities": [
            {
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "match_score": 8.0,
            }
        ],
    }
    result = link_document_tickers(document, ALIAS_TABLE)
    assert result["tickers"] == ["AAPL"]
    assert result["link_confidence"] == 0.95


def test_entity_linking_upgrades_alias_confidence_when_company_is_in_title() -> None:
    document = {
        "title": "Meta faces trial setback in social media lawsuit",
        "description": "The ruling limits part of Meta's defense in the case.",
        "snippet": "",
        "entities": [],
        "entity_names": [],
        "symbols": [],
    }
    result = link_document_tickers(document, ALIAS_TABLE)
    assert result["tickers"] == ["META"]
    assert result["link_confidence"] == 0.95


def test_entity_linking_uses_coinbase_alias_from_title() -> None:
    document = {
        "title": "Jamie Dimon Fires Back At Coinbase: If You Want To Be A Bank, Be A Bank",
        "description": "The remarks focused on stablecoin balances and bank-like regulation.",
        "snippet": "",
        "entities": [],
        "entity_names": [],
        "symbols": [],
    }
    result = link_document_tickers(document, ALIAS_TABLE)
    assert "COIN" in result["tickers"]
    assert result["link_confidence"] == 0.95


def test_entity_linking_drops_benchmark_alias_when_single_name_is_strong() -> None:
    document = {
        "title": "UBS Maintains Buy Rating on NVIDIA Corporation (NVDA) as Backlog Extends Into 2027",
        "description": "NVIDIA ranks among the best performing S&P 500 stocks in the last 10 years.",
        "snippet": "",
        "entities": [
            {
                "symbol": "NVDA",
                "name": "NVIDIA Corporation",
                "match_score": 60.0,
            }
        ],
        "entity_names": ["NVIDIA Corporation"],
        "symbols": ["NVDA"],
    }
    result = link_document_tickers(document, ALIAS_TABLE)
    assert result["tickers"] == ["NVDA"]


def test_entity_linking_exposes_provider_anchor_diagnostics() -> None:
    document = {
        "title": "Apple cuts guidance after weaker demand in China",
        "description": "Apple lowered its outlook after weak iPhone demand.",
        "snippet": "",
        "entities": [
            {
                "symbol": "AAPL",
                "name": "Apple Inc.",
                "match_score": 8.0,
            }
        ],
        "entity_names": ["Apple Inc."],
        "symbols": ["AAPL", "SPY"],
    }
    result = link_document_tickers(document, ALIAS_TABLE)
    assert result["anchored_provider_symbols"] == ["AAPL"]
    assert "SPY" in result["unanchored_provider_symbols"]


def test_taxonomy_and_sentiment_scoring() -> None:
    document = {
        "title": "JPMorgan faces lawsuit after compliance probe",
        "description": "The bank faces a legal complaint and a regulatory probe.",
        "snippet": "",
        "entities": [],
        "symbols": ["JPM"],
        "entity_names": ["JPMorgan"],
    }
    event = classify_event_type(document)
    polarity = score_polarity(document)
    severity = score_severity(
        document,
        event_type=event["event_type"],
        event_confidence=event["event_confidence"],
        link_confidence=0.95,
        polarity=polarity["polarity"],
    )

    assert event["event_type"] == "legal"
    assert polarity["polarity"] < 0.0
    assert severity["severity"] > 0.6


def test_taxonomy_prefers_earnings_when_earnings_and_guidance_coexist() -> None:
    document = {
        "title": "Microsoft beats earnings estimates and raises outlook for cloud growth",
        "description": "Microsoft reported quarterly earnings above expectations and raised guidance.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "earnings"


def test_taxonomy_captures_live_macro_headline() -> None:
    document = {
        "title": "Tech Sell-Off and Geopolitical Tensions Drive Market Lower; Costco Reports After the Bell",
        "description": "U.S. equity markets faced significant selling pressure as geopolitical tensions in the Middle East sent shockwaves.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "macro"


def test_taxonomy_captures_supply_chain_regulatory_risk() -> None:
    document = {
        "title": "Pentagon Notifies Anthropic It's Deemed Firm Supply-Chain Risk",
        "description": "A senior defense official escalated the dispute over artificial intelligence safeguards.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "legal"


def test_taxonomy_classifies_real_product_headline() -> None:
    document = {
        "title": "Apple unveils the M5 Pro and M5 Max with fusion architecture to power the Next-Gen MacBook Pro",
        "description": "Apple introduced new chips and hardware architecture for the MacBook lineup.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "product"


def test_taxonomy_prefers_macro_for_broad_market_headline_with_earnings_mention() -> None:
    document = {
        "title": "Wall Street Slumps as Iran Conflict Sends Oil Soaring; Target Rallies on Earnings",
        "description": "Broad market selling intensified as oil jumped and geopolitical risk rose.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "macro"


def test_taxonomy_classifies_conference_transcript_as_commentary() -> None:
    document = {
        "title": "Cogent Communications Holdings, Inc. Presents at J.P. Morgan 2026 Global Leveraged Finance Conference Transcript",
        "description": "Conference transcript covering management commentary.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "commentary"


def test_taxonomy_classifies_market_move_recap_as_commentary() -> None:
    document = {
        "title": "Johnson & Johnson (JNJ) Declines More Than Market: Some Information for Investors",
        "description": "The stock closed lower in the latest session.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "commentary"


def test_taxonomy_classifies_stock_picker_headline_as_commentary() -> None:
    document = {
        "title": "Amazon vs. Costco: Which Stock Is a Better Buy?",
        "description": "A comparison of two stocks for investors.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "commentary"


def test_taxonomy_classifies_rankings_headline_as_commentary() -> None:
    document = {
        "title": "10 Most Popular Stocks Among Hedge Funds: December 31, 2025 Rankings",
        "description": "A ranking of popular hedge fund holdings.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "commentary"


def test_taxonomy_classifies_meta_trial_headline_as_legal() -> None:
    document = {
        "title": "Judge blocks Meta from introducing plaintiff's additional trauma claims in social media trial",
        "description": "The trial ruling limits part of Meta's defense in the case.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "legal"


def test_taxonomy_classifies_regulated_like_bank_headline_as_legal() -> None:
    document = {
        "title": "Jamie Dimon Fires Back At Coinbase: If You Want To Be A Bank, Be A Bank",
        "description": "Crypto firms paying interest on stablecoin balances should face the same rules as banks.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "legal"


def test_taxonomy_prefers_upgrade_for_buy_rating_story_even_with_wall_street_snippet() -> None:
    document = {
        "title": "UBS Maintains Buy Rating on NVIDIA Corporation (NVDA) as Backlog Extends Into 2027",
        "description": "NVIDIA ranks among the best performing S&P 500 stocks in the last 10 years.",
        "snippet": "Even Wall Street analysts were caught off guard by the strength of the backlog.",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "upgrade"


def test_taxonomy_classifies_regulatory_policy_story() -> None:
    document = {
        "title": "Chip stocks slide after new U.S. export controls proposal",
        "description": "Washington floated a new policy proposal aimed at tightening export controls.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "regulatory_policy"


def test_taxonomy_classifies_product_issue_story() -> None:
    document = {
        "title": "Cloud outage disrupts Adobe workflow tools across enterprise customers",
        "description": "The service disruption hit key product modules during trading hours.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "product_issue"


def test_taxonomy_classifies_capital_return_story() -> None:
    document = {
        "title": "Oracle expands share repurchase program after strong free cash flow",
        "description": "Management announced a larger buyback authorization.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "capital_return"


def test_taxonomy_exposes_macro_subtype() -> None:
    document = {
        "title": "Wall Street Slumps as Iran Conflict Sends Oil Soaring",
        "description": "Oil prices rose sharply as geopolitical tensions escalated.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "macro"
    assert event["event_subtype"] == "oil_geopolitical"
    assert event["story_bucket"] == "event_driven"


def test_taxonomy_exposes_commentary_story_bucket() -> None:
    document = {
        "title": "Amazon vs. Costco: Which Stock Is a Better Buy?",
        "description": "A comparison for long-term investors.",
        "snippet": "",
    }
    event = classify_event_type(document)
    assert event["event_type"] == "commentary"
    assert event["event_subtype"] == "opinion"
    assert event["story_bucket"] == "opinion"


def test_quality_gate_filters_commentary_content() -> None:
    document = {
        "title": "Is Costco Stock a Long-Term Buy?",
        "description": "A columnist discusses the stock in a long-term investing context.",
        "source": "finance.yahoo.com",
        "url": "https://example.com/costco-buy",
    }
    quality = assess_event_quality(
        document,
        event_type="commentary",
        event_confidence=0.63,
        link_confidence=0.65,
    )
    assert quality["watchlist_eligible"] is False
    assert "generic_content_penalty" in quality["quality_reasons"]


def test_entity_linking_drops_generic_commentary_alias_links() -> None:
    document = {
        "title": "Is Costco Stock a Long-Term Buy?",
        "description": "The article compares the company against the S&P 500 and other stocks.",
        "snippet": "",
        "entities": [],
        "entity_names": [],
        "symbols": [],
    }
    result = link_document_tickers(document, ALIAS_TABLE)
    assert result["tickers"] == []


def test_quality_gate_keeps_macro_event_without_single_name_link() -> None:
    document = {
        "title": "Wall Street Slumps as Geopolitical Tensions Drive Oil Higher",
        "description": "U.S. equity markets faced significant downward pressure as oil prices rose.",
        "source": "example.com",
        "url": "https://example.com/live/macro",
    }
    quality = assess_event_quality(
        document,
        event_type="macro",
        event_confidence=0.95,
        link_confidence=0.0,
    )
    assert quality["watchlist_eligible"] is True
    assert quality["quality_label"] in {"medium", "high"}


def test_quality_gate_filters_unlinked_press_release() -> None:
    document = {
        "title": "Rumble Reports Fourth Quarter and Full Year 2025 Results",
        "description": "Revenue surpassed $100 million for the first time in company history.",
        "source": "globenewswire.com",
        "url": "https://example.com/globenewswire/rumble-results",
    }
    quality = assess_event_quality(
        document,
        event_type="earnings",
        event_confidence=0.63,
        link_confidence=0.0,
    )
    assert quality["watchlist_eligible"] is False
    assert quality["quality_label"] == "low"


def test_quality_gate_hard_filters_low_signal_recap_source() -> None:
    document = {
        "title": "Amazon vs. Costco: Which Stock Is a Better Buy?",
        "description": "A recap article comparing two stocks for investors.",
        "source": "fool.com",
        "url": "https://www.fool.com/investing/example",
    }
    quality = assess_event_quality(
        document,
        event_type="commentary",
        event_confidence=0.85,
        link_confidence=0.90,
        event_subtype="opinion",
        story_bucket="opinion",
    )
    assert quality["watchlist_eligible"] is False
    assert "low_signal_hard_filtered" in quality["quality_reasons"]
