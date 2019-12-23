"""Create shared mocks for our tests.

Author: T.J. Gaffney (gaffneytj@google.com)

Copyright 2019 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from copy import deepcopy

from table import *

TEST_PREFIX = "test_prefix"
MOCK_CM_DIR = "MOCK_CM_DIR"
MOCK_DS_DIR = "MOCK_DS_DIR"
MOCK_CELLS_DIR = "MOCK_CELLS_DIR"


class MockColumnManager(ColumnManager):
    """A version of ColumnManager with mocked file operations.

    Attributes:
        fake_files: A dictionary whose keys are file names, and whose values are
            the files stored at those files.
    """

    def __init__(self, prefix: Text, fake_files: Optional[Dict] = None):
        self.load_log = list()
        self.save_log = list()
        self.fake_files = dict()
        if fake_files is not None:
            self.fake_files = fake_files

        super().__init__(prefix)

    def _walk_files(self):
        for k, _ in self.fake_files.items():
            if k.split("::")[0] == MOCK_CM_DIR:
                yield ("", [k.split("::")[1]])

    def _load_file(self, path: Text) -> Any:
        full_path = "{}::{}".format(MOCK_CM_DIR, path)
        assert (full_path in self.fake_files)

        self.load_log.append(path)
        return self.fake_files[full_path]

    def _save_file(self, object: Any, path: Text) -> None:
        full_path = "{}::{}".format(MOCK_CM_DIR, path)
        self.save_log.append(path)
        self.fake_files[full_path] = deepcopy(object)


class MockDateSet(DateSet):
    """A version of DateSet with mocked file operations.

    Attributes:
        fake_files: A dictionary whose keys are file names, and whose values are
            the files stored at those files.
    """

    def __init__(self, prefix: Text, fake_files: Optional[Dict] = None):
        self.load_log = list()
        self.save_log = list()
        self.fake_files = dict()
        if fake_files is not None:
            self.fake_files = fake_files

        super().__init__(prefix)

    def _walk_files(self):
        for k, _ in self.fake_files.items():
            if k.split("::")[0] == MOCK_DS_DIR:
                yield ("", [k.split("::")[1]])

    def _load_file(self, path: Text) -> Any:
        full_path = "{}::{}".format(MOCK_DS_DIR, path)
        assert (full_path in self.fake_files)

        self.load_log.append(path)
        return self.fake_files[full_path]

    def _save_file(self, object: Any, path: Text) -> None:
        full_path = "{}::{}".format(MOCK_DS_DIR, path)
        self.save_log.append((path, deepcopy(object)))
        self.fake_files[full_path] = deepcopy(object)


class MockCellMaster(CellMaster):
    """A version of CellMaster with mocked file operations.

    Attributes:
        fake_files: A dictionary whose keys are file names, and whose values are
            the files stored at those files.
    """
    def __init__(self, prefix: Optional[Text],
                 cache_size: int = 5, fake_files: Optional[Dict] = None):
        self.load_log = list()
        self.save_log = list()
        self.fake_files = dict()
        if fake_files is not None:
            self.fake_files = fake_files

        super().__init__(prefix, cache_size)

    def _load_file(self, path: Text) -> Any:
        full_path = "{}::{}".format(MOCK_CELLS_DIR, path)

        self.load_log.append(path)
        if full_path in self.fake_files:
            return self.fake_files[full_path]
        return dict()

    def _save_file(self, object: Any, path: Text) -> None:
        full_path = "{}::{}".format(MOCK_CELLS_DIR, path)

        self.save_log.append((path, deepcopy(object)))
        self.fake_files[full_path] = deepcopy(object)


class MockTable(Table):
    def __init__(self, prefix: Text, fake_files: Optional[Dict] = None):
        super().__init__(prefix)

        # Mock out the core components
        self.cells = MockCellMaster(self.prefix, fake_files=fake_files)
        self.cm = MockColumnManager(self.prefix, fake_files=fake_files)
        self.ds = MockDateSet(self.prefix, fake_files=fake_files)
