import pytest
from time import sleep
from timeflux.helpers.background import Task

class DummyWorker():
    def echo(self, message='hello', delay=0, fail=False):
        sleep(delay)
        if fail: raise Exception('failed')
        self.message = message
        return(self.message)

def test_default(working_path):
    task = Task(DummyWorker(), 'echo').start()
    while not task.done:
        status = task.status()
    assert status['result'] == 'hello'
    assert status['instance'].message == 'hello'

def test_args(working_path):
    task = Task(DummyWorker(), 'echo', 'foobar').start()
    while not task.done:
        status = task.status()
    assert status['result'] == 'foobar'

def test_kwargs(working_path):
    task = Task(DummyWorker(), 'echo', message='foobar').start()
    while not task.done:
        status = task.status()
    assert status['result'] == 'foobar'

def test_exception(working_path):
    task = Task(DummyWorker(), 'echo', fail=True).start()
    while not task.done:
        status = task.status()
    assert status['success'] == False
    assert status['exception'].args[0] == 'failed'

def test_stop_running(working_path):
    task = Task(DummyWorker(), 'echo', delay=5).start()
    sleep(.5)
    assert task.done == False
    task.stop()
    assert task.done == True

def test_stop_not_running(working_path):
    task = Task(DummyWorker(), 'echo').start()
    while not task.done:
        status = task.status()
    task.stop()
    assert task.done == True
