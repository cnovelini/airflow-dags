from os.path import abspath, join, dirname
import sys
import tests.assets.fixtures
import tests.assets.mocks
from tests.assets.walk_packages import get_package_paths_in_module

sys.path.insert(0, abspath(join(dirname(dirname(__file__)), "plugins")))


pytest_plugins = [*get_package_paths_in_module(tests.assets.fixtures)]
