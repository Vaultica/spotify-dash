"""`appengine_config` gets loaded when starting a new application instance."""
import sys
import os.path
import tempfile

tempfile.SpooledTemporaryFile = tempfile.TemporaryFile

# add `lib` subdirectory to `sys.path`, so our `main` module can load
# third-party libraries.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'lib'))

