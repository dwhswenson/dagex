from dag import DAG, Edge

class Context(object):
    def __init__(self, state):
        self.state = state

    def tasks_to_dag(self, task_list):
        last_output = {}
        edges = []
        for task in task_list:
            input_tasks = [last_output.get(ens)
                           for ens in self.get_input_slots(task)]
            edges.extend(Edge(inp_task, task) for inp_task in input_tasks
                         if inp_task is not None)
            outputs = {ens: task for ens in self.get_output_slots(task)}
            last_output.update(outputs)

        dag = DAG.from_edges(edges)
        return dag

    def get_task_metadata(self, task):
        if task.metadata is None:
            task.metadata = self._get_task_metadata(task)
        return task.metadata

    def context_params(self):
        pass

    @classmethod
    def from_context_params(cls, params):
        return cls()

    def _get_task_metadata(self, task):
        pass  # not required to be implemented, but improves efficiency

    def get_input_slots(self, task):
        raise NotImplementedError()

    def get_output_slots(self, task):
        raise NotImplementedError()

    def is_fast(self, task):
        raise NotImplementedError()

    def get_task_function(self, task):
        raise NotImplementedError()

    def get_task_args_kwargs(self, task):
        raise NotImplementedError()

    def report_results(self, results):
        raise NotImplementedError()


class MockMetadata(object):
    def __init__(self, func_id, input_slots, output_slots, func):
        self.func_id = func_id
        self.input_slots = input_slots
        self.output_slots = output_slots
        self.func = func


class MockContext(Context):
    def __init__(self, state, task_functions):
        super().__init__(state)
        self.task_functions = task_functions

    def _get_task_metadata(self, task):
        return self.task_functions[task.function_id]

    def get_input_slots(self, task):
        metadata = self.get_task_metadata(task)
        return metadata.input_slots

    def get_output_slots(self, task):
        metadata = self.get_task_metadata(task)
        return metadata.output_slots

    def is_fast(self, task):
        return len(self.get_output_slots(task)) > 1

    def get_task_function(self, task):
        metadata = self.get_task_metadata(task)
        return metadata.func

    # def get_task_args_kwargs(self, func):
        # pass

    # def report_results(self, results):
        # pass
