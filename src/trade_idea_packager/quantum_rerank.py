"""
Helper module: apply quantum artifacts to candidate lists as an offline reranker.
"""
from typing import List, Dict, Any


def apply_quantum_rerank(candidates: List[Dict[str, Any]], artifacts: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return a new candidate list re-ordered by artifact scores.

    Simple safe behavior:
    - If artifacts is falsy or empty, return candidates unmodified.
    - Artifacts expected to contain a mapping candidate_id -> score (higher = better).
    - Candidates without scores keep relative order but are placed after scored candidates.
    """
    if not artifacts:
        return candidates

    scores = artifacts.get("scores", {}) if isinstance(artifacts, dict) else {}
    scored = []
    unscored = []
    for c in candidates:
        cid = c.get("id")
        s = scores.get(cid)
        if s is not None:
            scored.append((s, c))
        else:
            unscored.append(c)

    scored.sort(key=lambda x: x[0], reverse=True)
    return [c for _, c in scored] + unscored
