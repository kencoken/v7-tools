import tempfile
import shutil
from contextlib import contextmanager


@contextmanager
def mktmpdir(**kwargs) -> str:
    """
    Create a temporary directory and return it's path
    Destroys all resources in the directory once the context is exited
    :return: the temporary directory path
    """
    tmpdir = tempfile.mkdtemp(**kwargs)
    yield tmpdir
    shutil.rmtree(tmpdir)