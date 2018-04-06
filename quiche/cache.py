"""
cache.py

Code for saving/loading models and objects. If keras is imported, will
load/save Keras models.
"""

import os
import sys
import time
import pickle
import base64
import string

SLUG_CHARS = (
  string.ascii_lowercase
+ string.ascii_uppercase
+ string.digits
)

FILENAME_ENCODING = "utf-8"

def slug(name):
  result = ""
  running = False
  for c in name:
    if c in SLUG_CHARS:
      if running:
        result += '-'
      result += c
      running = False
    else:
      running = True
  if running:
    result += '-'

  return result

def safe_filename(name):
  """
  Encodes the given target name safely as a filesystem name.
  """
  return slug(name) + '_' + base64.urlsafe_b64encode(
    bytes(name, encoding=FILENAME_ENCODING)
  ).decode(encoding=FILENAME_ENCODING)

def file_basename(cache_dir, target_name):
  """
  Converts a target name to the name of the file it'll be stored under.
  """
  return os.path.join(cache_dir, safe_filename(target_name))

def save_model(cache_dir, model, model_name):
  """
  Saves the given model to disk for retrieval using load_model.
  """
  model.save(file_basename(cache_dir, model_name) + ".h5")

def load_model(cache_dir, model_name):
  """
  Loads the given model from disk. Raises a ValueError if the target doesn't
  exist. Returns a (timestamp, value) pair.
  """
  import keras
  fn = file_basename(cache_dir, model_name) + ".h5"
  if os.path.exists(fn):
    ts = os.path.getmtime(fn)
    return (ts, keras.models.load_model(fn))
  else:
    raise ValueError(
      "Model '{}' isn't stored in directory '{}'.".format(
        model_name,
        cache_dir
      )
    )


def save_object(cache_dir, obj, name):
  """
  Uses pickle to save the given object to a file.
  """
  fn = file_basename(cache_dir, name) + ".pkl"
  with open(fn, 'wb') as fout:
    pickle.dump(obj, fout)

def load_object(cache_dir, name):
  """
  Uses pickle to load the given object from a file. If the file doesn't exist,
  raises a ValueError. Returns a (timestamp, value) pair.
  """
  fn = file_basename(cache_dir, name) + ".pkl"
  if os.path.exists(fn):
    ts = os.path.getmtime(fn)
    with open(fn, 'rb') as fin:
      return (ts, pickle.load(fin))
  else:
    raise ValueError(
      "Object '{}' isn't stored in directory '{}'.".format(
        name,
        cache_dir
      )
    )

def save_any(cache_dir, obj, name):
  """
  Selects save_object or save_model automatically.
  """
  if "keras" in sys.modules:
    keras = sys.modules["keras"]
    if isinstance(obj, (keras.models.Sequential, keras.models.Model)):
      save_model(cache_dir, obj, name)
    else:
      save_object(cache_dir, obj, name)
  else:
    save_object(cache_dir, obj, name)

def load_any(cache_dir, name):
  """
  Attempts load_object and falls back to load_model.
  """
  try:
    return load_object(cache_dir, name)
  except Exception as e:
    if "keras" in sys.modules:
      return load_model(cache_dir, name)
    else:
      raise e

def check_time(cache_dir, name):
  """
  Returns just the modification time for the given object (tries pickle first
  and then h5). Returns None if the file does not exist.
  """
  bfn = file_basename(cache_dir, name)
  fn = bfn + ".pkl"
  if os.path.exists(fn):
    return os.path.getmtime(fn)
  else:
    fn = bfn + ".h5"
    if os.path.exists(fn):
      return os.path.getmtime(fn)
    else:
      return None
