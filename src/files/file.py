import re

import os
from os.path import abspath


class File:
    _FILENAME_REGEX = None

    def __init__(self, path, file_type) -> None:
        self.path = abspath(path)
        self.is_parsed = False
        self.file_type = file_type

    @property
    def filename(self):
        return os.path.basename(self.path)

    @property
    def tablename(self):
        m = re.search(self._FILENAME_REGEX, self.filename)
        table_name = "{}".format(m.group("filename")).replace("_", "").lower()
        return table_name
