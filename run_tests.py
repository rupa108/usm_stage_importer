import unittest
import sys

from tests.lib.test_support import install_fake_vm_builtin
# 1. Inject VM into builtins before loading any test modules
install_fake_vm_builtin()

if __name__ == '__main__':
    # 2. Discover and run your tests normally
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir='tests') # Point this to your tests directory
    
    runner = unittest.TextTestRunner(verbosity=3, buffer=True)
    result = runner.run(suite)
    
    sys.exit(not result.wasSuccessful())