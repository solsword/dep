#!/usr/bin/env python3
"""
unit.py

Quiche unit tests (pretty minimal so far).
"""

try:
  from . import dep
except:
  print("Warning: using installed dep module instead of bundled version.")
  from quiche import dep

@dep.task([], ["value"], "base", ["ephemeral"])
def base(value=1):
  return value

@dep.task(["base"], ["times"], "product", ["ephemeral"])
def mult(base, times=1):
  return base * times

def test1():
  p1 = dep.create("product", {"value": 3, "times": 5})[1]
  p2 = dep.create("product", {"value": 5, "times": 6})[1]

  assert p1 == 15, "3×5 failed"
  assert p2 == 30, "5×6 failed"

  return True

ALL_TESTS = [
  test1,
]

def main():
  failed = 0
  for t in ALL_TESTS:
    success = True
    try:
     success = t()
    except:
      success = False

    if not success:
      print("Test '{}' failed.".format(t.__name__))
      failed += 1

  nt = len(ALL_TESTS)
  print("--{}/{} test(s) passed--".format(nt - failed, nt))

if __name__ == "__main__":
  main()
