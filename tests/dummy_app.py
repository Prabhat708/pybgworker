from pybgworker import task

@task(name="dummy.task")
def dummy_task():
    return True
