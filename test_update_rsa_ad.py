"""Unit tests for update_rsa_ad — verifies validation and update_mask construction."""
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent))

import google_ads_server


class _PathList:
    def __init__(self):
        self.paths = []

    def extend(self, items):
        self.paths.extend(items)


def _build_fake_client():
    """Build a fake GoogleAdsClient that records the mutate operation."""
    captured = {}

    update_ad = SimpleNamespace(
        resource_name="",
        final_urls=[],
        responsive_search_ad=SimpleNamespace(
            headlines=[],
            descriptions=[],
            path1="",
            path2="",
        ),
    )
    ad_op = SimpleNamespace(update=update_ad, update_mask=_PathList())

    service = MagicMock()
    service.ad_path = lambda cid, ad_id: f"customers/{cid}/ads/{ad_id}"

    def mutate_ads(customer_id, operations):
        captured["customer_id"] = customer_id
        captured["operation"] = operations[0]
        result = SimpleNamespace(results=[SimpleNamespace(resource_name=update_ad.resource_name)])
        return result

    service.mutate_ads = mutate_ads

    def get_type(type_name):
        if type_name == "AdOperation":
            return ad_op
        if type_name == "AdTextAsset":
            return SimpleNamespace(text="")
        raise AssertionError(f"unexpected type requested: {type_name}")

    client = MagicMock()
    client.get_service.return_value = service
    client.get_type.side_effect = get_type
    return client, captured


def _call(client, **kwargs):
    """Invoke update_rsa_ad with explicit Nones for unset params.

    Direct Python calls leave FieldInfo defaults in place; MCP unwraps these at
    runtime. Tests must pass every optional param explicitly.
    """
    base = dict(headlines=None, descriptions=None, final_url=None, path1=None, path2=None)
    base.update(kwargs)
    if client is None:
        return google_ads_server.update_rsa_ad(**base)
    with patch.object(google_ads_server, "get_google_ads_client", return_value=client):
        return google_ads_server.update_rsa_ad(**base)


def test_invalid_final_url_rejected():
    result = _call(None, customer_id="1234567890", ad_id="999", final_url="example.com/no-scheme")
    assert result["success"] is False
    assert "http://" in result["error"]
    print("PASS: invalid final_url rejected")


def test_final_url_only_updates_mask_correctly():
    client, captured = _build_fake_client()
    result = _call(client, customer_id="1234567890", ad_id="999", final_url="https://example.com/new")
    assert result["success"] is True, result
    op = captured["operation"]
    assert op.update_mask.paths == ["final_urls"], op.update_mask.paths
    assert list(op.update.final_urls) == ["https://example.com/new"]
    print("PASS: final_url-only update sets final_urls path only")


def test_headlines_only_still_works():
    client, captured = _build_fake_client()
    result = _call(client, customer_id="1234567890", ad_id="999", headlines=["H1", "H2", "H3"])
    assert result["success"] is True, result
    op = captured["operation"]
    assert op.update_mask.paths == ["responsive_search_ad.headlines"]
    assert [a.text for a in op.update.responsive_search_ad.headlines] == ["H1", "H2", "H3"]
    print("PASS: headlines-only update preserves legacy behavior")


def test_mixed_headlines_and_final_url():
    client, captured = _build_fake_client()
    result = _call(
        client,
        customer_id="1234567890",
        ad_id="999",
        headlines=["H1"],
        final_url="https://example.com/x",
        path1="deals",
        path2="spring",
    )
    assert result["success"] is True, result
    op = captured["operation"]
    assert set(op.update_mask.paths) == {
        "responsive_search_ad.headlines",
        "final_urls",
        "responsive_search_ad.path1",
        "responsive_search_ad.path2",
    }
    assert op.update.responsive_search_ad.path1 == "deals"
    assert op.update.responsive_search_ad.path2 == "spring"
    print("PASS: mixed headlines + final_url + paths update includes all masks")


def test_nothing_provided_raises():
    result = _call(None, customer_id="1234567890", ad_id="999")
    assert result["success"] is False
    assert "At least one" in result["error"]
    print("PASS: no fields provided returns error")


if __name__ == "__main__":
    test_invalid_final_url_rejected()
    test_final_url_only_updates_mask_correctly()
    test_headlines_only_still_works()
    test_mixed_headlines_and_final_url()
    test_nothing_provided_raises()
    print("\nAll update_rsa_ad tests passed.")
