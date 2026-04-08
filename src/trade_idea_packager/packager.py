"""
Patch: integrate quantum rerank into build_package with feature-flag safe path.
"""
import os
from typing import List, Dict, Any
from .quantum_rerank import apply_quantum_rerank


def build_package(candidates: List[Dict[str, Any]], artifacts: Dict[str, Any] = None) -> Dict[str, Any]:
    """Build a package from candidates. If QUANTUM_RERANK_ENABLED=true, apply artifact reranking."""
    enabled = os.getenv("QUANTUM_RERANK_ENABLED", "false").lower() == "true"
    if enabled and artifacts:
        try:
            candidates = apply_quantum_rerank(candidates, artifacts)
        except Exception as e:
            # safe fallback: log and continue with original candidates
            print("quantum_rerank failed:", e)
    # assemble package (simplified)
    return {"candidates": candidates, "meta": {"rerank_applied": enabled and bool(artifacts)}}
