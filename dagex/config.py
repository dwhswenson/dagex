import importlib
from dataclasses import dataclass

def parse_python_object(obj_str):
    return obj_str

def import_name(name):
    splitted = name.split('.')
    module = ".".join(splitted[:-1])
    mod = importlib.import_module(module)
    obj = getattr(mod, splitted[-1])
    return obj

def _from_dict_param(cls, key, dct):
    dct = dct.copy()
    dct[key] = cls(**dct[key])
    return dct

class Config(object):
    def update(self, dct):
        for key, value in dct.items():
            setattr(self, key, value)

    def to_dict(self):
        sig = inspect.signature(self.__init__)
        names = list(sig.parameters.keys())
        mapping = {name: getattr(self, name) for name in names}
        return {name: obj.to_dict() if isinstance(obj, Config) else obj
                for name, obj in mapping.items()}

    @classmethod
    def from_dict(cls, dct):
        return cls(**dct)


@dataclass
class Submit(Config):
    command: str = ""
    script: str = ""


@dataclass
class LockFile(Config):
    timeout: float
    delay: float

class CommandConfig(Config):
    def __init__(self, lockfile):
        self.lockfile = lockfile

    def make_lock(self, task_filename):
        timeout, delay = self.lockfile.timeout, self.lockfile.delay
        return lockfile.LockFile(task_filename, timeout, delay)

    def update(self, dct):
        dct = dct.copy()
        lockfile = dct.pop('lockfile', {})
        self.lockfile.update(lockfile)
        super().update(dct)

    @classmethod
    def from_dict(self, dct):
        dct = _from_dict_param(LockFile, 'lockfile', dct)
        return super().from_dict(dct)


class Manager(CommandConfig):
    def __init__(self, submit, max_queued=0, extra_queued=0, lockfile=None):
        if lockfile is None:
            lockfile = LockFile(timeout=60.0, delay=1.0)
        super().__init__(lockfile)
        self.submit = submit
        self.max_queued = max_queued
        self.extra_queued = extra_queued

    def update(self, dct):
        dct = dct.copy()
        submit = dct.pop('submit', {})
        self.submit.update(submit)
        super().update(dct)

    @classmethod
    def from_dict(cls, dct):
        dct = _from_dict_param(Submit, 'submit', dct)
        return super().from_dict(dct)


class Runner(CommandConfig):
    def __init__(self, consume_fast_tasks=True, task_selector=None, lockfile=None):
        if lockfile is None:
            lockfile = LockFile(timeout=300.0, delay=1.0)
        super().__init__(lockfile)
        self.consume_fast_tasks = consume_fast_tasks
        self.task_selector = parse_python_object(task_selector)

    def update(self, dct):
        dct = dct.copy()
        task_selector = dct.pop('task_selector', None)
        if task_selector:
            self.task_selector = parse_python_object(task_selector)
        super().update(dct)

class TaskController(Config):
    # items in the list of classes/func_ids
    def __init__(self, name, fast=True, submit=None):
        self.name = name
        self.fast = fast
        self.submit = submit

    def update(self, dct):
        dct = dct.copy()
        submit = dct.pop('submit', {})
        if self.submit:
            self.submit.update(submit)
        elif submit:
            self.submit = submit
        # otherwise stays as is
        super().update(dct)


class TaskConfig(Config):
    def __init__(self, classes, func_ids):
        self.classes = classes
        self.func_ids = func_ids

        self._is_fast = {}
        self._submit = {}
        self.fast_classes = set([])

        self._update_caches()

    def _update_caches(self):
        is_fast = {func_id.name: func_id.fast for func_id in self.func_ids}
        submit = {func_id.name: func_id.submit for func_id in self.func_ids}
        fast_classes = {import_name(cls.name) for cls in self.classes
                        if cls.fast}
        self._is_fast.update(is_fast)
        self._submit.update(submit)
        self.fast_classes.update(fast_classes)

    def _update_list(self, current, update_dct):
        existing = {c.name: c for c in current}
        updates = {d['name']: d for d in update_dct}
        new = []
        for name, dct in updates.items():
            if name in existing:
                existing[name].update(dct)
            else:
                new.append(TaskController.from_dict(dct))
        return list(existing.values()) + new

    def update(self, dct):
        self.classes = self._update_list(self.classes, dct['classes'])
        self.func_ids = self._update_list(self.func_ids), dct['func_ids']
        self._update_caches()

    def to_dict(self):
        dct = super().to_dict()
        dct['classes'] = [c.to_dict() for c in dct['classes']]
        dct['func_ids'] = [c.to_dict() for c in dct['func_ids']]
        return dct

    @classmethod
    def from_dict(cls, dct):
        dct = dct.copy()
        dct['classes'] = [TaskController.from_dict(d) for d in dct['classes']]
        dct['func_ids'] = [TaskController.from_dict(d) for d in dct['func_ids']]
        return super().from_dict(dct)

    def is_fast(instance, func_id):
        is_fast = self._is_fast.get(func_id, None)
        if is_fast is None:
            is_fast = isinstance(instance, self.fast_classes)
            self._is_fast[func_id] = is_fast
        return is_fast

    def get_submit(self, func_id, default):
        submit = self._submit.get(func_id, None)
        if submit is None:
            self._submit[func_id] = default
            submit = default
        return default
