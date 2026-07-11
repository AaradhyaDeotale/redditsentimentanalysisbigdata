"""
test_subkeywords.py
--------------------
Covers the SubkeywordRegistry (src/subkeywords.py) and the
/api/keywords/{keyword}/subkeywords routes.

No Redis runs in the test environment, so connect_redis() returns None and
every SubkeywordRegistry here naturally exercises the in-memory fallback
path - the same convention the rest of the dashboard tests rely on for the
"Redis down" case (see KeywordRegistry, exercised the same way via
test_api.py).

Run from the dashboard/ folder:
    USE_MOCK_DATA=true pytest
"""

import os

os.environ["USE_MOCK_DATA"] = "true"

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from src.main import app  # noqa: E402
from src.subkeywords import SubkeywordRegistry  # noqa: E402


def _registry() -> SubkeywordRegistry:
    reg = SubkeywordRegistry()
    assert reg._redis is None  # confirms this test is exercising the fallback path
    return reg


def test_set_get_roundtrip():
    reg = _registry()
    assert reg.set("apple", ["iphone", "macbook"]) == ["iphone", "macbook"]
    assert reg.get("apple") == ["iphone", "macbook"]


def test_normalization_of_keyword_and_subkeywords():
    reg = _registry()
    reg.set(" Apple ", [" iPhone ", "MacBook"])
    assert reg.get("apple") == ["iphone", "macbook"]


def test_normalization_dedupes_preserving_order():
    reg = _registry()
    assert reg.set("apple", ["iPhone", "iphone", " IPHONE "]) == ["iphone"]


def test_comma_in_subkeyword_rejected():
    reg = _registry()
    with pytest.raises(ValueError):
        reg.set("apple", ["iphone,macbook"])


def test_invalid_parent_keyword_rejected():
    reg = _registry()
    with pytest.raises(ValueError):
        reg.set("bad!keyword", ["iphone"])
    with pytest.raises(ValueError):
        reg.get("bad!keyword")


def test_empty_list_is_valid_and_clears_existing():
    reg = _registry()
    reg.set("apple", ["iphone"])
    assert reg.set("apple", []) == []
    assert reg.get("apple") == []


def test_get_unknown_keyword_returns_empty_list():
    reg = _registry()
    assert reg.get("nokeyword") == []


def test_remove_keyword_clears_entry():
    reg = _registry()
    reg.set("apple", ["iphone"])
    reg.remove_keyword("apple")
    assert reg.get("apple") == []


def test_get_all_returns_every_set_keyword():
    reg = _registry()
    reg.set("apple", ["iphone"])
    reg.set("android", ["pixel"])
    assert reg.get_all() == {"apple": ["iphone"], "android": ["pixel"]}


# -- API-level tests (mirrors how /api/keywords is tested in test_api.py) --


def test_api_get_before_set_returns_empty():
    with TestClient(app) as client:
        res = client.get("/api/keywords/apple/subkeywords")
        assert res.status_code == 200
        assert res.json() == {"subkeywords": []}


def test_api_put_then_get_roundtrip():
    with TestClient(app) as client:
        put_res = client.put(
            "/api/keywords/apple/subkeywords",
            json={"subkeywords": ["iPhone", "MacBook"]},
        )
        assert put_res.status_code == 200
        assert put_res.json() == {"subkeywords": ["iphone", "macbook"]}

        get_res = client.get("/api/keywords/apple/subkeywords")
        assert get_res.status_code == 200
        assert get_res.json() == {"subkeywords": ["iphone", "macbook"]}


def test_api_put_invalid_subkeyword_returns_422():
    with TestClient(app) as client:
        res = client.put(
            "/api/keywords/apple/subkeywords",
            json={"subkeywords": ["iphone,macbook"]},
        )
        assert res.status_code == 422


def test_api_delete_keyword_clears_its_subkeywords():
    with TestClient(app) as client:
        client.post("/api/keywords", json={"keyword": "tesla"})
        client.put("/api/keywords/tesla/subkeywords", json={"subkeywords": ["model3"]})
        assert client.get("/api/keywords/tesla/subkeywords").json() == {
            "subkeywords": ["model3"]
        }

        client.delete("/api/keywords/tesla")

        assert client.get("/api/keywords/tesla/subkeywords").json() == {
            "subkeywords": []
        }
