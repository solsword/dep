"""
cache.py

Code for saving/loading models and objects. If keras is imported, will
load/save Keras models.
"""

import os
import sys
import time
import shelve
import tempfile

def now():
  """
  Get the current timestamp.
  """
  return time.time()

def save_model(cache_file, model, model_name):
  """
  Saves the given model to disk for retrieval using load_model.
  """
  with tempfile.TemporaryDirectory() as tmpdir:
    fn = os.path.join(tmpdir, "model.h5")
    model.save(fn)
    with open(fn, 'rb') as fin:
      modelbytes = fin.read()

  with shelve.open(cache_file) as shelf:
    shelf["model:" + model_name] = (now(), modelbytes)
    shelf.sync()

def load_model(cache_file, model_name):
  """
  Loads the given model from the cache. Raises a ValueError if the target
  doesn't exist. Returns a (timestamp, value) pair.
  """
  import keras
  with shelve.open(cache_file) as shelf:
    mk = "model:" + model_name
    if mk in shelf:
      ts, modelbytes = shelf[mk]
      with tempfile.TemporaryDirectory() as tmpdir:
        fn = os.path.join(tmpdir, "model.h5")
        with open(fn, 'wb') as fout:
          fout.write(modelbytes)

        loaded = keras.models.load_model(fn)

      return (ts, loaded)
    else:
      raise ValueError(
        "Model '{}' isn't stored in cache '{}'.".format(
          model_name,
          cache_file
        )
      )

def save_object(cache_file, obj, name):
  """
  Uses pickle to save the given object to a file.
  """
  ok = "obj:" + name
  with shelve.open(cache_file) as shelf:
    try:
      shelf[ok] = (now(), obj)
    except:
      raise ValueError("Failed to pickle result for: '{}'".format(name))

def load_object(cache_file, name):
  """
  Uses pickle to load the given object from a file. If the file doesn't exist,
  raises a ValueError. Returns a (timestamp, value) pair.
  """
  ok = "obj:" + name
  with shelve.open(cache_file) as shelf:
    if ok in shelf:
      return shelf[ok]
    else:
      raise ValueError(
        "Object '{}' isn't stored in cache '{}'.".format(
          name,
          cache_file
        )
      )

def save_any(cache_file, obj, name):
  """
  Selects save_object or save_model automatically.
  """
  if "keras" in sys.modules:
    keras = sys.modules["keras"]
    if isinstance(obj, (keras.models.Sequential, keras.models.Model)):
      save_model(cache_file, obj, name)
    else:
      save_object(cache_file, obj, name)
  else:
    save_object(cache_file, obj, name)

def load_any(cache_file, name):
  """
  Attempts load_object and falls back to load_model.
  """
  try:
    return load_object(cache_file, name)
  except Exception as e:
    if "keras" in sys.modules:
      return load_model(cache_file, name)
    else:
      raise e

def check_time(cache_file, name):
  """
  Returns just the modification time for the given object (tries pickle first
  and then h5). Returns None if the file does not exist.
  """
  with shelve.open(cache_file) as shelf:
    key = "obj:" + name
    mkey = "model:" + name
    if key in shelf:
      return shelf[key][0]
    elif mkey in shelf:
      return shelf[mkey][0]
    else:
      return None
