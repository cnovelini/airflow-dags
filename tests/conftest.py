import tests.assets.fixtures
import tests.assets.mocks
from tests.assets.walk_packages import get_package_paths_in_module

pytest_plugins = [*get_package_paths_in_module(tests.assets.fixtures)]
