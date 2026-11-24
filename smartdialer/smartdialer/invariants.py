"""Global invariants, checked continuously by the simulator and by the tests."""
from __future__ import annotations

from .states import ACTIVE_CALL_STATES

_ACTIVE = ",".join(f"'{s.value}'" for s in ACTIVE_CALL_STATES)


def check(c) -> list[str]:
    errs = []
    q = lambda sql, *a: c.execute(sql, a).fetchall()
    for r in q(f"""SELECT agent_id, COUNT(*) n FROM calls WHERE agent_id IS NOT NULL AND state IN ({_ACTIVE})
                   GROUP BY agent_id HAVING n > 1"""):
        errs.append(f"agent {r[0]} has {r[1]} active calls")
    for r in q(f"""SELECT borrower_id, COUNT(*) n FROM calls WHERE state IN ({_ACTIVE})
                   GROUP BY borrower_id HAVING n > 1"""):
        errs.append(f"borrower {r[0]} has {r[1]} active calls")
    for r in q("""SELECT a.id, a.state, a.call_id, k.state, k.agent_id FROM agents a LEFT JOIN calls k ON k.id=a.call_id
                  WHERE a.state IN ('RESERVED','DIALING','CONNECTED')"""):
        if r[3] is None or r[3] not in [s.value for s in ACTIVE_CALL_STATES] or r[4] != r[0]:
            errs.append(f"agent {r[0]} {r[1]} points at call {r[2]} in state {r[3]} owned by {r[4]}")
    for r in q(f"""SELECT k.id, k.agent_id, a.state, a.call_id FROM calls k JOIN agents a ON a.id=k.agent_id
                   WHERE k.state IN ({_ACTIVE})"""):
        if r[3] != r[0]:
            errs.append(f"call {r[0]} bound to agent {r[1]} who is {r[2]} on {r[3]}")
    for r in q(f"""SELECT id, agent_id FROM calls WHERE state='CONNECTED' AND agent_id IS NULL"""):
        errs.append(f"call {r[0]} CONNECTED without agent")
    for r in q(f"""SELECT b.id FROM borrowers b LEFT JOIN calls k ON k.id=b.call_id
                   WHERE b.state='IN_CALL' AND (k.id IS NULL OR k.state NOT IN ({_ACTIVE}))"""):
        errs.append(f"borrower {r[0]} IN_CALL without an active call")
    n_agents = c.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
    bound = c.execute(f"SELECT COUNT(*) FROM calls WHERE agent_id IS NOT NULL AND state IN ({_ACTIVE})").fetchone()[0]
    if bound > n_agents:
        errs.append(f"{bound} agent-bound calls for {n_agents} agents")
    return errs
