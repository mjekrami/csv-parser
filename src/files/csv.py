import re
from .file import File


class CSVFile(File):
    _FILENAME_REGEX = r"(?P<filename>.*)_\d+.csv"

    def __init__(self, path, seperator=";", file_type="csv") -> None:
        super().__init__(path, file_type)
        self.seperator = seperator