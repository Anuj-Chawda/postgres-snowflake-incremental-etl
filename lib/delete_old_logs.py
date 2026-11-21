import logging
import os
from datetime import datetime, timedelta
import time

def delete_old_log_files(log_dir, days_to_keep=5):
  """Deletes log files older than the specified number of days."""
  try:
    for filename in os.listdir(log_dir):
      filepath = os.path.join(log_dir, filename)
      if os.path.isfile(filepath):
        file_mtime = os.path.getmtime(filepath)
        file_date = datetime.fromtimestamp(file_mtime)
        today = datetime.now()
        delta = today - file_date
        if delta.days > days_to_keep:
          os.remove(filepath)
          logging.info(f"Deleted old log file: {filepath}")
  except FileNotFoundError:
    logging.warning(f"Log directory not found: {log_dir}")
  except Exception as e:
    logging.error(f"Error deleting old log files: {e}")