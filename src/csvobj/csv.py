import re
import pathlib


class CSVFile():
    _FILENAME_REGEX = r"(?P<filename>.*)_\d+.csv"

    def __init__(self, file, seperator=";") -> None:
        self.path = file
        self.is_parsed = False
        self.seperator = seperator

    @property
    def tablename(self):
        filename = pathlib.Path(self.path).name
        m = re.search(self._FILENAME_REGEX, filename)
        table_name = "{}".format(m.group("filename")).replace("_", "").lower()
        return table_name
