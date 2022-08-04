from enum import IntEnum

class Status(IntEnum):
    UNASSIGNED = 0
    QUEUED = 1
    ASSIGNED = 2
    COMPLETED = 3

    def is_available(self):
        return self in [Status.UNASSIGNED, Status.QUEUED]

    def is_not_completed(self):
        return self != Status.COMPLETED


class Task(object):
    """Task object.

    The metadata attribute exists as a cache to store information related to
    this task. All operations should assume that the metadata might be None,
    and all operations that set metadata must set non-conflicting metadata.
    (Namely, a :class:`.Task` should be functionally immutable.)

    Parameters
    ----------
    number : int
        task ID number
    function_id : str
        task function identifier
    status : int or :class:`.Status`
        status identifier
    """
    def __init__(self, number, function_id, status):
        self.number = number
        self.function_id = function_id
        self.status = Status(status)
        self.metadata = None

    def serialize(self):
        return {'number': self.number,
                'function_id': self.function_id,
                'status': self.status}

    @classmethod
    def deserialize(cls, dct):
        return cls(**dct)

    def __repr__(self):
        return (f"Task(number={self.number}, function_id={self.function_id}, "
                + f"status='{self.status}')")


