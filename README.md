# Parallelization of OPS

The goal is to create a couple OPS commands to enable simple parallelization.
There are three "main" functions that will have to play together:

* `build_graph`: Plan the simulation; build the task graph.
* `launch_tasks`: Find the number of free tasks available and launch tasks to
  do them.
* `run_task`: Run the next task. This will actually determine the next task by
  checking the graph when it is run.

The idea is that `build_graph` is run once by the user; `launch_tasks` is run periodically (either with cron or by running at the end of a script); and `run_task` is run from the remote job.

Most likely, `build_graph` and `lauch_tasks` will have the same command name in the CLI, but different calling conventions (like `build_graph` may have an extra argument required).

Some of the things I'll need to implement:

* Creating a DAG from our tasks -- we know the input and output ensembles, but
  those are "half edges"
* Identifying which task to run
* Marking tasks as unassigned / assigned / completed
* Compressing the fast task subgraphs
