#!/usr/bin/env python
"""
You need redis
apt-get install redis-server

Assumption
Element in ordered set in redis will allways be  "element_name"|"time_stamp" - based on this fact we will asume that it is unique 
add(r_server, worker_set_name, int(time.time()),"el_%s" % int(time.time()))

This implementation will store Your tasks as a todo list:
Work list: [('taskB|1417721875', 1417721875.0), ('taskA|1417721890', 1417721890.0), ('taskB|1417721891', 1417721891.0), ('taskB|1417725349', 1417725349.0)]


"""

import redis
import time

worker_set_name = 'w1'


class Event():
  def __init__(self, tupple):
    self.msg, self.time = tupple
    if self.msg != 0:
      self.funct = self.msg.split("|")[0]
    else:
      self.funct = 'none'

  def _print(self):
    print "%s, %s, %s" % (self.time, self.msg, self.funct)
  
  def _print_string(self):
    return "%s, %s, %s" % (self.time, self.msg, self.funct)

def init():
  r_server = redis.Redis('localhost')
  return r_server

def pop(r_server, remove=True):
  return z_pop(r_server, remove)

def z_pop(r_server, remove=True):
  x = r_server.zrange(worker_set_name, 0, 0, withscores=True)
  if len(x) > 0:
    element, score = x[0]
    if remove: 
      r_server.zrem(worker_set_name, element) 
  else:
    element, score = (0,0)
  return element,score

def add(r_server, worker_set_name, score, element):
  z_add(r_server, worker_set_name, score, element)

def z_add(r_server, worker_set_name, score, element):
  r_server.zadd(worker_set_name, element, score)

def rem(r_server, worker_set_name, element):
  z_rem(worker_set_name, element)

def z_rem(r_server, worker_set_name, element):
  r_server.zrem(worker_set_name, element)

def remove_all(r_server):
  loop = True
  while loop:
    element, score = z_pop(r_server, True)
    print "Element %s, score %s" % (element, score)
    if element == 0:
      loop = False 

def get_all(r_server, remove=False):
  zlist = r_server.zrange(worker_set_name, 0, -1, withscores=True)
  return zlist

def print_all(r_server, remove=False):
  zlist = r_server.zrange(worker_set_name, 0, -1, withscores=True)
  print "Work list: %s" % zlist

def self_check(r_server):
  new_time = int(time.time() + 10)
  if True in ['self_check' not in x[0] for x in get_all(r_server)] or len(get_all(r_server)) == 0:
    add(r_server, worker_set_name, new_time, "self_check|%s" % new_time)

def run(r_server):
  while True:
    current = Event(pop(r_server, False))
    time_now = int(time.time())    
    if time_now > current.time and current.time != 0:
      print "Pop: %s,%s" % pop(r_server, True)
      #current._print()
      if current.funct == "self_check":
        self_check(r_server)
    else:
      #print "Sleep: " + current._print_string()
      time.sleep(1)
   

if __name__ == '__main__':
  r_server = init()
  remove_all(r_server)
  add(r_server, worker_set_name, int(time.time()),"self_check|%s" % int(time.time()))
  add(r_server, worker_set_name, int(time.time()+ 12 ),"self_check|%s" % int(time.time()+ 12))
  add(r_server, worker_set_name, int(time.time()+ 22 ),"self_check|%s" % int(time.time()+ 22))

  print_all(r_server)
  print "-------------"
  print int(time.time())
  print "-------------"
  run(r_server)

   
