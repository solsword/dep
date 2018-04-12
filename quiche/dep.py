"""
dep.py

Python make-like object building/caching system. Example usage:


  @dep.task(["dependency-1", "dependency-2"], ["param_1"], "product-name")
  def function_that_produces_product(dep1, dep2, param_1=None):
    ...
    return product

Results are cached on-disk in CACHE_FILE, which can be changed using
set_cache_file.
"""

import os
import time
import pickle
import regex as re # for same-name group overwriting
import collections
import traceback

from . import cache

TARGET_ALIASES = {}
KNOWN_TARGETS = {}
TARGET_GENERATORS = collections.OrderedDict()

CACHED_VALUES = {}

CACHE_FILE = ".quiche"

DC1 = '\u0010'
DC2 = '\u0011'
DC3 = '\u0012'
DC4 = '\u0013'

def set_cache_file(fn):
  """
  Sets the cache file. Doesn't delete the old one (do that manually).
  """
  CACHE_FILE = fn

def add_alias(alias, target):
  """
  Adds an alias that simply bridges between targets. Note that aliases take
  priority over actual targets, so be careful not to shadow stuff. Alias
  chaining does work, but can also create infinite loops.
  """
  global TARGET_ALIASES
  TARGET_ALIASES[alias] = target

def add_object(obj, target, flags=()):
  """
  Adds a task that simply returns the given object. As a precaution, when
  add_object is called the target object is immediately (re-)cached, so that
  any old objects cached under the same target will be overwritten. If you want
  to avoid this behavior, don't call add_object repeatedly.
  """
  global KNOWN_TARGETS
  def offer():
    nonlocal obj
    return obj
  cache_value(target, (), {}, obj, flags)
  KNOWN_TARGETS[target] = ((), (), offer, flags)

def add_gather(inputs, output, flags=()):
  """
  Adds a task which simply gathers all of its dependencies into a list.
  """
  global KNOWN_TARGETS
  def gather(*inputs):
    return inputs
  KNOWN_TARGETS[output] = (inputs, (), gather, flags)

def task(inputs, params, output, flags=()):
  """
  A decorator for defining a task. Registers the decorated function as the
  mechanism for producing the declared target using the given inputs (given as
  *args) and parameters (given as **kwargs). Parameters not given by the user
  when creating an object will default to None. An example:

  ```
from quiche import dep

@dep.task([], ["value"], "base")
def base(value=1):
  return value

@dep.task(["base"], ["times"], "product")
def mult(base, times=1):
  return base * times

p1 = dep.create("product", {"value": 3, "times": 5})[1]
p2 = dep.create("product", {"value": 5, "times": 6})[1]

print("{} == {}".format(p1, 15))
print("{} == {}".format(p2, 30))
  ```
  """
  if not isinstance(inputs, (list, tuple)):
    raise ValueError(
      "task inputs must be a list or tuple. Did you forget a comma?"
    )
  if not isinstance(inputs, (list, tuple)):
    raise ValueError(
      "task params must be a list or tuple. Did you forget a comma?"
    )
  if not isinstance(output, str):
    raise ValueError("task output must be a string.")
  def decorate(function):
    global KNOWN_TARGETS
    KNOWN_TARGETS[output] = (inputs, params, function, flags)
    return function
  return decorate

def template_task(inputs, params, output, flags=()):
  """
  A decorator similar to task, but it generates targets by replacing named
  formatting groups within input/param/output strings with appropriate matches.
  Note that the formatting groups which are used to specify inputs or params
  must be named.

  The function will be called with an re.match object as its first argument.
  """
  if not isinstance(inputs, (list, tuple)):
    raise ValueError(
      "template_task inputs must be a list or tuple. Did you forget a comma?"
    )
  if not isinstance(params, (list, tuple)):
    raise ValueError(
      "template_task params must be a list or tuple. Did you forget a comma?"
    )
  if not isinstance(output, str):
    raise ValueError("template_task output must be a string.")

  def decorate(function):
    global TARGET_GENERATORS

    slots = re.findall(r"(?<!{){[^{}]*}", output)

    plainslots = slots.count("{}")
    keyslots = set( sl[1:-1] for sl in slots if sl != "{}" )

    if plainslots + len(keyslots) > 16:
      raise ValueError("Too many slots (>16)!\n{}".format(output))

    # Encode indices using control character pairs
    digits = [DC1, DC2, DC3, DC4]
    plainrep = [ digits[i//4] + digits[i%4] for i in range(plainslots) ]
    keyrep = {}
    i = plainslots
    for k in keyslots:
      keyrep[k] = digits[i//4] + digits[i%4]
      i += 1

    keygroups = { k:  r"(?<" + k + r">.+)" for k in keyslots }

    tre = re.escape(output.format(*plainrep, **keyrep))
    for pr in plainrep:
      tre = tre.replace(pr, r"(.+)")
    for k in keyrep:
      tre = tre.replace(keyrep[k], keygroups[k])

    def gen_target(name_match, stuff):
      inputs, params, function, flags = stuff

      gd = name_match.groupdict()
      try:
        inputs = [ inp.format(**gd) for inp in inputs ]
      except IndexError:
        raise ValueError("Task template inputs may not include unnamed groups!")
      try:
        params = [ param.format(**gd) for param in params ]
      except IndexError:
        raise ValueError("Task template params may not include unnamed groups!")
      def wrapped(*args, **kwargs):
        nonlocal function, name_match
        return function(name_match, *args, **kwargs)
      wrapped.__name__ = function.__name__
      return inputs, params, wrapped, flags

    TARGET_GENERATORS[tre] = (gen_target, (inputs, params, function, flags))
    return function
  return decorate

def iter_task(inputs, params, output, flags=()):
  """
  A decorator similar to task, but it generates targets by replacing {iter} and
  {next} within input/param/output strings with subsequent natural numbers.
  """
  if not isinstance(inputs, (list, tuple)):
    raise ValueError(
      "iter_task inputs must be a list or tuple. Did you forget a comma?"
    )
  if not isinstance(output, str):
    raise ValueError("iter_task output must be a string.")
  def decorate(function):
    global TARGET_GENERATORS

    tre = re.escape(output.format(iter=DC1, next=DC2))
    tre = tre.replace(DC1, r"(?P<iter>[0-9]+)")
    tre = tre.replace(DC2, r"(?P<next>[0-9]+)")

    def gen_target(name_match, stuff):
      inputs, params, function, flags = stuff

      try:
        ival = int(name_match.group("iter"))
      except IndexError:
        ival = None
      try:
        nval = int(name_match.group("next"))
      except IndexError:
        nval = None

      if ival == None and nval != None:
        if nval <= 0:
          ival = "start"
        else:
          ival = nval - 1
      elif ival != None and nval == None:
        nval = ival + 1
      elif ival == None or nval == None:
        ival = "start"
        nval = 0

      inputs = [ inp.format(iter=ival, next=nval) for inp in inputs ]
      params = [ param.format(iter=ival, next=nval) for param in params ]
      def wrapped(*args, **kwargs):
        nonlocal function, nval
        return function(nval, *args, **kwargs)
      wrapped.__name__ = function.__name__
      return inputs, params, wrapped, flags

    TARGET_GENERATORS[tre] = (gen_target, (inputs, params, function, flags))
    return function
  return decorate

class NotAvailable:
  pass

def params__bytes(pnames, params):
  """
  Returns a unique byte string for the values of each of the given parameter
  names within the given parameters dictionary.
  """
  obj = tuple((pn, params.get(pn, None)) for pn in pnames)
  try:
    return pickle.dumps(obj)
  except:
    raise ValueError("Parameter object is not picklable:\n{}".format(obj))

def bytes__params(pbytes):
  """
  Converts a parameters byte string back into a parameters dictionary. Only the
  selected names will be present, of course.
  """
  obj = pickle.loads(pbytes)
  return { x[0]: x[1] for x in obj }

def mix_target(target, relevant, params):
  """
  Creates a full target name out of a base target name, a list of relevant
  parameters, and a parameter values dictionary.
  """
  return target + ':' + params__bytes(relevant, params).decode(
    "utf-8",
    errors="replace"
  )

def get_cache_time(target, pnames=(), params={}):
  """
  Gets the cache time of the given target. Returns None if the target isn't
  cached anywhere.
  """
  full_target = mix_target(target, pnames, params)
  if full_target in CACHED_VALUES:
    return CACHED_VALUES[full_target][0]
  else:
    return cache.check_time(CACHE_FILE, full_target)

def get_cached(target, pnames=(), params={}):
  """
  Fetches a cached object for the given target (which uses the named parameters
  from the given parameters dictionary), or returns a special NotAvailable
  result. Returns a (timestamp, value) pair, with None as the time if the
  object isn't available.
  """
  full_target = mix_target(target, pnames, params)
  if full_target in CACHED_VALUES:
    # in memory
    return CACHED_VALUES[full_target]
  else:
    try:
      # on disk
      return cache.load_any(CACHE_FILE, full_target)
    except:
      # must create
      return None, NotAvailable

def cache_value(target, pnames, params, value, flags):
  """
  Adds a value to the cache, also storing it to disk. Returns the timestamp of
  the newly-cached value. Flags affect caching as follows:

    "ephemeral": Don't cache on disk (only in memory).
    "volatile": Don't cache in memory (only on disk).

  Combining "ephemeral" and "volatile" will not work (in that case, just use a
  normal function to return the value, rather than a dependency).
  """
  full_target = mix_target(target, pnames, params)
  if "ephemeral" not in flags: # Else don't save on disk
    cache.save_any(CACHE_FILE, value, full_target)
  ts = time.time()
  if "volatile" not in flags: # Else don't save in memory
    CACHED_VALUES[full_target] = (ts, value)
  else:
    try:
      del CACHED_VALUES[full_target]
    except:
      pass
  return ts

def find_target(target):
  """
  Retrieves information (inputs, parameters, processing function, and flags)
  for the given target. Generates a target when necessary and possible.
  """
  while target in TARGET_ALIASES:
    target = TARGET_ALIASES[target]
  if target in KNOWN_TARGETS:
    # known target: return it
    return KNOWN_TARGETS[target]
  else:
    # try to find a generator that can handle it?
    for tre in TARGET_GENERATORS:
      m = re.match(tre, target)
      if m:
        gen, stuff = TARGET_GENERATORS[tre]
        try:
          return gen(m, stuff)
        except:
          pass

  # Not a known target and no generator matches:
  raise ValueError("Unknown target '{}'.".format(target))

def indent(report):
  """
  Adds one layer of indentation to the given report.
  """
  return "  " + "\n  ".join(report.split("\n")[:-1]) + "\n"

def find_target_report(target, show_tracebacks=False):
  """
  Searches for the given target, storing and returning a log of targets
  attempted. Useful for debugging missing targets.

  Setting show_tracebacks to True enables reporting tracebacks for rule
  generation failures.
  """
  report = ""
  while target in TARGET_ALIASES:
    report += "alias '{}' → '{}'\n".format(target, TARGET_ALIASES[target])
    target = TARGET_ALIASES[target]
  if target in KNOWN_TARGETS:
    report += "found known target '{}'\n".format(target)
    # known target: return our report
    return report
  else:
    report += "unknown target '{}'; searching rule templates\n".format(target)
    # try to find a generator that can handle it?
    for tre in TARGET_GENERATORS:
      m = re.match(tre, target)
      if m:
        report += "matched expression '{}'\n".format(tre)
        gen, stuff = TARGET_GENERATORS[tre]
        try:
          success = gen(m, stuff)
          report += "generated rule with dependencies:\n  {}\n".format(
            ",\n  ".join("'{}'".format(d) for d in success[0])
          )
          return report
        except Exception as e:
          tb = traceback.format_exc()
          report += "rule generation failed for: '{}'\n".format(tre)
          report += indent(tb)
      else:
        report += "didn't match expression '{}'\n".format(tre)

  report += "no matching rules for '{}'\n".format(target)
  return report

def recursive_target_report(target, above=None):
  """
  Creates and returns a report detailing all of the recursive dependencies of
  the given target. Detects and reports circular dependencies.
  """
  report = ""
  above = above or set()
  above = set(above)
  above.add(target)
  try:
    deps, fcn, flags = find_target(target)
    if deps:
      report += "'{}' depends on:\n".format(target)
      for d in deps:
        if d in above:
          report += "  '{}', which is a circular dependency!\n".format(d)
        else:
          subreport = recursive_target_report(d, above)
          report += indent(subreport)
    else:
      report += "'{}'\n".format(target)
  except ValueError:
    report += "'{}' (could not be resolved)\n".format(target)

  return report

def gather_relevant_parameters(target):
  """
  Recursively gathers a sorted list of parameters relevant to the given target.
  This includes parameters specified as inputs to that target and to any
  (recursive) required sub-target(s).
  """
  inputs, relevant, function, flags = find_target(target)
  rv = list(relevant)
  for inp in inputs:
    for param in gather_relevant_parameters(inp):
      where = len(rv)
      for i, p in enumerate(rv):
        if param < p:
          where = i
          break
        elif param == p:
          where = -1 # don't insert (already present)
          break

      if where >= 0:
        rv.insert(where, param)

  return rv

def check_up_to_date(target, params={}, knockout=()):
  """
  Returns a timestamp for the given target after checking that all of its
  (recursive) perquisites are up-to-date. If missing and/or out-of-date values
  are found, new values are generated.

  If given, targets in the knockout list (or set) will be considered stale even
  if timestamps indicate that they're up-to-date.
  """
  inputs, pnames, function, flags = find_target(target)

  all_relevant = gather_relevant_parameters(target)

  times = [ check_up_to_date(inp, params, knockout) for inp in inputs ]
  subparams = [ gather_relevant_parameters(inp) for inp in inputs ]

  if target in knockout:
    myts = None # explicit rebuild
  else:
    myts = get_cache_time(target, all_relevant, params)
  if myts is None or any(ts > myts for ts in times):
    # Compute and cache a new value:
    ivalues = []
    for relevant, inp in zip(subparams, inputs):
      ts, val = get_cached(inp, relevant, params)
      if val is NotAvailable:
        raise ValueError("Couldn't create dependency '{}'.".format(inp))
      ivalues.append(val)
    pvalues = { pn: params[pn] for pn in pnames if pn in params }
    value = function(*ivalues, **pvalues)
    return cache_value(target, all_relevant, params, value, flags)
  else:
    # Just return time cached:
    return myts

def create(target, params={}, knockout=()):
  """
  Creates the desired target, using cached values when appropriate. Passes
  parameters and the knockout list to check_up_to_date. Returns a (timestamp,
  value) pair indicating when the returned value was constructed. Raises a
  ValueError if the target is invalid or can't be created.

  The given parameters (values should be small where possible; use targets to
  embody bigger objects) are available to tasks, and the same target will be
  cached differently for different params if it depends on them (even
  recursively).
  """
  # Update dependencies as necessary (recursively)
  check_up_to_date(target, params, knockout)

  # Grab newly-cached value:
  ts, val = get_cached(target, gather_relevant_parameters(target), params)

  # Double-check that we got a value:
  if val is NotAvailable:
    raise ValueError("Failed to create target '{}'.".format(target))

  return (ts, val)

def create_brave(target, params={}, knockout=()):
  """
  Creates the desired target, using the cache without question if a cached
  value is available. Only use this when you're fine with an out-of-date cached
  value.
  """

  # Reach for a cached value *without* checking freshness:
  ts, val = get_cached(target, gather_relevant_parameters(target), params)

  if val is NotAvailable: # Fine, we'll do a full dependency check
    ts, val = create(target, params, knockout)

  # If that failed, we're out of luck
  if val is NotAvailable:
    raise ValueError("Failed to create target '{}'.".format(target))

  return (ts, val)
