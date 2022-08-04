import pytest

from task import *

class TestTask(object):
    def setup(self):
        self.tasks = {'unassigned': Task(0, 'foo', Status.UNASSIGNED),
                      'queued': Task(1, 'bar', Status.QUEUED),
                      'assigned': Task(2, 'baz', Status.ASSIGNED),
                      'completed': Task(3, 'qux', Status.COMPLETED)}

    @pytest.mark.parametrize('name', ['unassigned', 'queued', 'assigned',
                                      'completed'])
    def test_initialization(self, name):
        task = self.tasks[name]
        assert task.number == task.status  # hack from specific setup here
        if name in ['unassigned', 'queued']:
            assert task.status.is_available()
        else:
            assert not task.status.is_available()

        if name in ['completed']:
            assert not task.status.is_not_completed()
        else:
            assert task.status.is_not_completed()

    @pytest.mark.parametrize('name', ['unassigned', 'queued', 'assigned',
                                      'completed'])
    def test_serialization_cycle(self, name):
        task = self.tasks[name]
        ser = task.serialize()
        deser = Task.deserialize(ser)
        assert task.status == deser.status
        assert task.number == deser.number
        assert task.function_id == deser.function_id
        reser = deser.serialize()
        assert ser == reser
