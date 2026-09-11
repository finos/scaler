Monitoring
==========

Scaler comes with a number of features that can be used to monitor and profile tasks, and customize behavior.

Scaler Top (Monitoring)
-----------------------

Top is a monitoring tool that allows you to see the status of the Scaler.
The scheduler prints an address to the logs on startup that can be used to connect to it with the `scaler_top` CLI command:

.. code:: bash

    scaler_top ipc:///tmp/0.0.0.0_8516_monitor

Which will show an interface similar to the standard Linux `top` command:

.. code:: console

   scheduler        | task_manager        |     scheduler_sent         | scheduler_received
         cpu   0.0% |   unassigned      0 |      HeartbeatEcho 283,701 |          Heartbeat 283,701
         rss 130.1m |      running      0 |     ObjectResponse     233 |      ObjectRequest     215
                    |      success 53,704 |           TaskEcho  53,780 |               Task  53,764
                    |       failed     14 |               Task  54,660 |         TaskResult  53,794
                    |     canceled     48 |         TaskResult  53,766 |         TaskCancel      60
                    |    not_found     14 |      ObjectRequest     366 |    BalanceResponse      15
                                          |         TaskCancel      62 |          GraphTask       6
                                          |     BalanceRequest      15 |
                                          |    GraphTaskResult       6 |
   -------------------------------------------------------------------------------------------------
   Shortcuts: worker[n] agt_cpu[C] agt_rss[M] cpu[c] rss[m] free[f] sent[w] queued[d] lag[l]

   Total 7 worker(s)
                      worker agt_cpu agt_rss [cpu]    rss free sent queued   lag ITL |    client_manager
   2732890|sd-1e7d-dfba|d26+    0.5%  111.8m  0.5% 113.3m 1000    0      0 0.7ms 100 |
   2732885|sd-1e7d-dfba|56b+    0.0%  111.0m  0.5% 111.2m 1000    0      0 0.7ms 100 | func_to_num_tasks
   2732888|sd-1e7d-dfba|108+    0.0%  111.7m  0.5% 111.0m 1000    0      0 0.6ms 100 |
   2732891|sd-1e7d-dfba|149+    0.0%  113.0m  0.0% 112.2m 1000    0      0 0.9ms 100 |
   2732889|sd-1e7d-dfba|211+    0.5%  111.7m  0.0% 111.2m 1000    0      0   1ms 100 |
   2732887|sd-1e7d-dfba|e48+    0.5%  112.6m  0.0% 111.0m 1000    0      0 0.9ms 100 |
   2732886|sd-1e7d-dfba|345+    0.0%  111.5m  0.0% 112.8m 1000    0      0 0.8ms 100 |


* `scheduler` section shows the scheduler's resource usage
* `task_manager` section shows the status of tasks
* `scheduler_sent` section counts the number of each type of message sent by the scheduler
* `scheduler_received` section counts the number of each type of message received by the scheduler
* `worker` section shows worker details, you can use shortcuts to sort by columns, and the * in the column header shows which column is being used for sorting

  * `agt_cpu/agt_rss` means cpu/memory usage of the worker agent
  * `cpu/rss` means cpu/memory usage of the worker
  * `free` means number of free task slots for the worker
  * `sent` means how many tasks scheduler sent to the worker
  * `queued` means how many tasks worker received and enqueued
  * `lag` means the latency between scheduler and the worker
  * `ITL` means is debug information

    * `I` means processor initialized
    * `T` means have a task or not
    * `L` means task lock


Additional client-facing feature guides have been consolidated into :doc:`scaler_client`.


Scaler Web GUI
--------------

Scaler also provides a browser-based monitoring dashboard through ``scaler_gui``.
It subscribes to the scheduler monitor stream and serves a real-time web UI over HTTP.

Start the GUI by pointing it at the scheduler monitor address:

.. code:: bash

    scaler_gui tcp://127.0.0.1:6380 --gui-address 127.0.0.1:50001

Open ``http://127.0.0.1:50001`` in your browser.

What the Web GUI shows:

* **Live**: scheduler metrics, worker manager summary, and worker-level metrics (CPU/PSS/free/sent/queued/lag/ITL).
* **Task List**: one row per task, with its client, worker, duration, peak memory and capabilities. Click a column header to sort the whole retained list by it.
* **Task Log**: one row per state change in the order it happened, so a task that is rebalanced or retried leaves its whole trail. Click a row to see one task's trail alone.
* **Worker Task Stream**: a timeline by worker with capability colors and status overlays (failed and canceled patterns).
* **Memory and CPU**: rolling chart of what the fleet holds, read on the left axis, and the CPU it uses, read on a right axis that scales to the busiest moment in the window.
* **Workers**: one row per worker under a row for its manager, which sums the same columns: the worker's memory against its limit and its CPU, the task each processor holds and for how long, and the queue waiting behind it in the order it runs. Click a queued task for its trail.
* **Machines**: one row per host, with its workers, CPU, memory and host-wide network counters.
* **Clients**: one row per connected client, with its host, tasks in flight, finished and failed counts, CPU, memory and latency. Finished counts every task that reached a terminal state, cancelled ones included.
* **Objects**: the biggest objects the scheduler tracks, with their size, the client that created them, and the tasks holding them, a page at a time.

The Live tab also carries an Object Storage card: how many objects the storage server holds, how many distinct payloads are behind them, the bytes they occupy, and how many requests are waiting for an object that does not exist yet.
A waiting count that does not fall is a fetch nobody can answer, because a client blocks in ``get_object`` until the object is created.

.. note::
   Worker memory is reported as PSS (proportional set size) on Linux, so the shared copy-on-write pages a
   worker and its forked processors map are not double-counted; where PSS is unavailable (macOS/Windows) it
   falls back to RSS.

Interactive behavior:

* Pushes updates over a server-sent event stream, which the page reconnects when it drops.
* Sends a full current snapshot on connect, then incremental updates in short batches.
* Supports runtime settings for stream window length (5/10/30 minutes) and memory axis scale (linear/log).
* Keeps each browser tab's view, meaning its tab, settings, sorting, pages and task filter, across a reload or a dropped stream.
