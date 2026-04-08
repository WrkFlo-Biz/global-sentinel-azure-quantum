def test_apply_quantum_rerank_no_artifacts():
    from src.trade_idea_packager.quantum_rerank import apply_quantum_rerank
    c = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    assert apply_quantum_rerank(c, {}) == c


def test_apply_quantum_rerank_basic():
    from src.trade_idea_packager.quantum_rerank import apply_quantum_rerank
    c = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    artifacts = {"scores": {"b": 0.9, "c": 0.8}}
    out = apply_quantum_rerank(c, artifacts)
    assert out[0]["id"] == "b"
    assert out[1]["id"] == "c"
    assert any(x["id"] == "a" for x in out)


def test_build_package_flag_off(monkeypatch):
    from src.trade_idea_packager.packager import build_package
    import os
    monkeypatch.delenv("QUANTUM_RERANK_ENABLED", raising=False)
    c = [{"id": "a"}, {"id": "b"}]
    p = build_package(c, {"scores": {"b": 1.0}})
    assert p["meta"]["rerank_applied"] is False


def test_build_package_flag_on(monkeypatch):
    from src.trade_idea_packager.packager import build_package
    monkeypatch.setenv("QUANTUM_RERANK_ENABLED", "true")
    c = [{"id": "a"}, {"id": "b"}]
    p = build_package(c, {"scores": {"b": 1.0}})
    assert p["meta"]["rerank_applied"] is True
    assert p["candidates"][0]["id"] == "b"
