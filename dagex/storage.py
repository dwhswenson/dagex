import sqlalchemy as sql
from task import Task

"""
The storage database here is very simple. There's just one table:

Tasks:
| number (primary key) | mover UUID | status |
"""

class Storage(object):
    def __init__(self, connection_uri):
        self.connection_uri = connection_uri
        self.engine = sql.create_engine(self.connection_uri)
        self.metadata = sql.MetaData(bind=self.engine)

        self.metadata.reflect(self.engine)
        if 'tasks' not in self.metadata.tables:
            self._initiate_new_table()

        self.table = self.metadata.tables['tasks']

    @classmethod
    def new_from_function_ids(cls, connection_uri, function_ids):
        tasks = [Task(num, func_id, 'unassaigned')
                 for num, func_id in enumerate(function_ids)]
        storage = cls(connection_uri)
        storage.insert(tasks)
        return storage

    def _initiate_new_table(self):
        table = sql.Table('tasks', self.metadata,
                          sql.Column('number', sql.Integer,
                                     primary_key=True),
                          sql.Column('function_id', sql.String),
                          sql.Column('status', sql.Integer))
        self.metadata.create_all(self.engine)


    def insert_tasks(self, task_list):
        serialized = [task.serialize() for task in task_list]
        with self.engine.connect() as conn:
            conn.execute(self.table.insert(), serialized)

    def update_task(self, task):
        stmt = sql.update(self.table).\
                where(self.table.c.number == task.number).\
                values(status=task.status)
        with self.engine.connect() as conn:
            conn.execute(stmt)

    def _load_all_rows(self):
        # used for debugging/testing as well as load_all, below
        with self.engine.connect() as conn:
            results = list(conn.execute(self.table.select()))
        return results

    def load_all(self):
        rows = self._load_all_rows()
        task_list = [Task.deserialize(task_data)
                     for task_data in rows]
        return task_list
