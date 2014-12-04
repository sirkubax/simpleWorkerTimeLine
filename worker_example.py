#!/usr/bin/env python
"""
Assumption
Element in ordered set in redis will allways be  "element_name"|"time_stamp" - based on this fact we will asume that it is unique
add(r_server, worker_set_name, int(time.time()),"element_name|%s" % int(time.time()))

This implementation will store Your tasks as a todo list:
Work list: [('taskB|1417721875', 1417721875.0), ('taskA|1417721890', 1417721890.0), ('taskB|1417721891', 1417721891.0), ('taskB|1417725349', 1417725349.0)]

A task is run when current time is grater than task timestamp.  For me it's OK, but:
It is single thread processing "line" so Start of the task at given timestamp (in case of 2 or more task in the same start_time) is not guaranteed.
You can implement Thread if You like.

"""
import redisWork as workQueue
import time

worker_set_name = 'w1'

def taskA(r_server):
    print "This is task A"
    new_time = int(time.time() + 3)
    """ 
    Add new event of the same kind, Start: 3 second from now
    I would like to have only one event of this kind at any time, so check if the future timeline is empty
    """
    if True in ['taskA' not in x[0] for x in workQueue.get_all(r_server)] or len(workQueue.get_all(r_server)) == 0:
        workQueue.add(r_server, worker_set_name, new_time, "taskA|%s" % new_time)

def taskB(r_server):
    print "This is task B"
    new_time = int(time.time() + 10)
    if True in ['taskB' not in x[0] for x in workQueue.get_all(r_server)] or len(workQueue.get_all(r_server)) == 0:
        workQueue.add(r_server, worker_set_name, new_time, "taskB|%s" % new_time)


if __name__ == "__main__":
  r_server = workQueue.init()
  new_time = int(time.time())
  """ You can clean old tasks:"""
  #workQueue.remove_all(r_server)
  workQueue.add(r_server, worker_set_name, new_time + 0, "taskA|%s" % str(new_time + 0))
  workQueue.add(r_server, worker_set_name, new_time + 1, "taskB|%s" % str(new_time + 1))
  workQueue.print_all(r_server)
  print "-------------"
  print int(time.time())
  print "-------------"

  while 1:
    current = workQueue.Event(workQueue.pop(r_server, False))
    time_now = int(time.time())
    if time_now > current.time and current.time != 0:
      print "Pop: %s,%s" % workQueue.pop(r_server, True)
      if current.funct == "taskA":
        taskA(r_server)
      elif current.funct == "taskB":
        taskB(r_server)
    else:
      #print "Sleep: " + current._print_string()
      time.sleep(1)
