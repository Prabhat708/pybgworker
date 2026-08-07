import json
from hypothesis import given, strategies as st
from pybgworker import task
from pybgworker.sqlite_queue import SQLiteQueue
from pybgworker.utils import get_conn

# Simple task
@task(name="tests.fuzz.task")
def fuzz_task(*args, **kwargs):
    return True

# Fuzzing valid JSON arguments
@given(
    args=st.lists(st.one_of(st.integers(), st.floats(allow_nan=False, allow_infinity=False), st.text(), st.booleans(), st.none())),
    kwargs=st.dictionaries(st.text(), st.one_of(st.integers(), st.floats(allow_nan=False, allow_infinity=False), st.text(), st.booleans(), st.none())).filter(lambda k: not any(x in k for x in ["priority", "countdown", "eta", "idempotency_key"])),
    priority=st.integers(min_value=-100, max_value=100),
    countdown=st.one_of(st.none(), st.floats(min_value=-1000, max_value=1000, allow_nan=False, allow_infinity=False)),
    idempotency_key=st.one_of(st.none(), st.text(min_size=1, max_size=100))
)
def test_fuzz_task_enqueue(args, kwargs, priority, countdown, idempotency_key):
    """Fuzz the task enqueue mechanism with random valid inputs to ensure no crashes."""
    res = fuzz_task.delay(
        *args,
        priority=priority,
        countdown=countdown,
        idempotency_key=idempotency_key,
        **kwargs
    )
    assert res.task_id is not None
    assert isinstance(res.task_id, str)

@given(
    st.lists(st.tuples(
        st.lists(st.integers()),
        st.dictionaries(st.text(), st.integers())
    ), min_size=1, max_size=50)
)
def test_fuzz_delay_many(arg_pairs):
    """Fuzz batch enqueue."""
    results = fuzz_task.delay_many(arg_pairs)
    assert len(results) == len(arg_pairs)
    for res in results:
        assert isinstance(res.task_id, str)
