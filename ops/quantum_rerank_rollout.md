Quantum Rerank Rollout & Canary Checklist

Goal: Safely roll the quantum artifact reranker into staging and production with monitoring and fast rollback.

Preparation
- [ ] Confirm feature flag/ENV: QUANTUM_RERANK_ENABLED (default: false)
- [ ] Create branch: feature/quantum-rerank
- [ ] Add unit tests + integration smoke described in tests/test_quantum_rerank.py
- [ ] Add CI smoke workflow: .github/workflows/quantum-rerank-smoke.yml
- [ ] Add logs/metrics: counters (quantum_rerank.run_count), timing (quantum_rerank.ms), errors (quantum_rerank.errors)
- [ ] Add note in runbook and on-call contacts

Staging Canary (0 -> 10 -> 20% rollout)
- [ ] Deploy to staging behind flag (QUANTUM_RERANK_ENABLED=true) and run a canned integration for 24h
- [ ] Canary: promote to 10% of traffic in staging-like environment for 24h
- [ ] If metrics look normal, increase to 20% for 24h
- [ ] Promote to full staging for 48h and observe

Key Metrics to Monitor
- Ranking delta: fraction of packages where top-3 candidates change vs baseline
- Conversion/usage signal: clicks/accepts on suggested ideas (where applicable)
- Latency: 95th percentile packaging latency delta (target < +50ms)
- Error rate: exceptions raised during rerank (target 0)
- Resource usage: memory/CPU for packaging job

Alerting & Rollback Criteria (auto/manual)
- Auto-rollback if error rate > 1% over 5m
- Auto-rollback if 95p latency increase > 200ms sustained for 10m
- Manual rollback if ranking delta causes >10% drop in conversion/usage over 24h

Runbook: Quick rollback
1) Set QUANTUM_RERANK_ENABLED=false in deployment config (or disable feature flag)
2) Re-de