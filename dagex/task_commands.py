import signal
import functools

from storage import Storage
from lockfile import LockFile

from task import Status

class Command(object):
    def __init__(self, task_file, config, context):
        self.task_file = task_file
        self.config = config
        self.context = context
        lock = self.config.lockfile
        self.lock = LockFile(self.task_file, lock.timeout, lock.delay)

    def find_aborted_tasks(self):
        pass

    def build_dag(self):
        self.lock.acquire()
        storage = Storage(self.task_file)
        incomplete_tasks = [t for t in storage.load_all()
                            if t.status.is_not_completed()]
        dag = self.context.tasks_to_dag(incomplete_tasks)
        self.lock.release()
        return dag


class TaskRunner(Command):
    abnormal_exit_signals = [signal.SIGTERM]
    def __init__(self, task_file, config, context):
        super().__init__(task_file, config, context)
        self.state = self.context.get_initial_state()
        self.n_long_run = 0

    def on_abnormal_exit(self, sig_num, frame, task):
        exit()
        pass

    def get_next_task(self):
        # 1. search for aborted tasks; if there is one, run it
        aborted_tasks = self.find_aborted_tasks()
        if aborted_tasks:
            return aborted_tasks[0]

        # 2. find the next task in the graph
        dag = self.build_dag()
        available = [t for t in dag.zero_count()
                     if t.status.is_available()]
        if available:
            # TODO: smarter way of selecting which task to run
            return available[0]

        # 3. if no tasks to run, return None
        return None

    def update_task_status(self, task, status):
        storage = Storage(self.task_file)
        task.status = status
        storage.update_task(task)

    def get_task_parts(self, task):
        args, kwargs = self.context.get_args_kwargs(task, state)
        func = self.context.get_task_function(task)
        return func, args, kwargs

    def assign_task(self, task):
        # ensure that we write out abort file if we abort
        abnormal_exit = functools.partial(self.on_abnormal_exit, task=task)
        for sig in self.abnormal_exit_signals:
            signal.signal(sig, abnormal_exit)

        # not allowed to assign a second long task to us
        self.n_long_run += int(self.context.is_long(task))
        can_assign = self.n_long_run < 2

        if can_assign:
            self.update_task_status(task, Status.ASSIGNED)
        else:
            task = None

        return task

    def finish_task(self, task, results):
        self.context.report_results(results)
        self.update_task_status(task, Status.COMPLETED)

    def run_one_task(self):
        self.lock.acquire()
        task = self.get_next_task()

        # try to assign the task to us; task becomes None if can't assign
        if task is not None:
            func, args, kwargs = self.get_task_parts(task)
            task = self.assign_task(task)

        self.lock.release()

        # run task and report results
        if task is not None:
            results = func(*args, **kwargs)
            self.lock.acquire()
            self.finish_task(task, results)
            self.lock.release()

        return task

    def run(self):
        last_task = self.run_one_task()
        while last_task is not None and self.config.consume_fast_tasks:
            last_task = self.run_one_task()


class TaskManager(Command):
    def submit_job(self):
        pass

    def run(self):
        dag_incomplete = self.build_dag()
        n_available_tasks = len(self.available_tasks(dag))
        n_running_tasks = len([
            t for t in dag.nodes
            if t.status == 'assigned' and self.context.is_long(t)
        ])
        n_to_submit = min(self.config.max_queued - n_running_tasks,
                          n_available_tasks)
        for _ in range(n_to_submit):
            self.submit_job()
