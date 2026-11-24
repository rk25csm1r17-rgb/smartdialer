from smartdialer.states import (AGENT_TRANSITIONS, CALL_RANK, TERMINAL_CALL_STATES, AgentState as A,
                                CallState as C, call_transition_allowed)


def test_call_states_only_move_forward():
    assert call_transition_allowed(C.RESERVED, C.RINGING)          # skipping is fine
    assert call_transition_allowed(C.RINGING, C.ANSWERED)
    assert not call_transition_allowed(C.ANSWERED, C.RINGING)      # late RINGING ignored
    assert not call_transition_allowed(C.ANSWERED, C.ANSWERED)     # duplicate ignored
    assert not call_transition_allowed(C.CONNECTED, C.ANSWERED)


def test_terminal_states_are_absorbing():
    for t in TERMINAL_CALL_STATES:
        for s in C:
            assert not call_transition_allowed(t, s)


def test_every_non_terminal_call_state_can_terminate():
    for s in C:
        if s not in TERMINAL_CALL_STATES:
            assert any(call_transition_allowed(s, t) for t in TERMINAL_CALL_STATES)


def test_agent_machine_is_complete_and_connected_must_wrap_up():
    assert set(AGENT_TRANSITIONS) == set(A)
    assert AGENT_TRANSITIONS[A.CONNECTED] == {A.WRAP_UP}
    assert A.RESERVED in AGENT_TRANSITIONS[A.AVAILABLE]
    assert A.RESERVED not in AGENT_TRANSITIONS[A.RESERVED]
