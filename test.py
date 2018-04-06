#!/usr/bin/env python3

from quiche import dep

@dep.task((), "base")
def base():
  return 7

@dep.task(("base",), "plus_one")
def plus_one(base):
  return base + 1

@dep.task(("plus_one",), "times_two")
def times_two(val):
  return val*2

ts, val = dep.create("times_two")
print("(7 + 1) * 2 is", val)
