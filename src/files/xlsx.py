import re
import pathlib
from .file import File


class XLSXFile(File):
    _FILENAME_REGEX = r"(?P<filename>.*)_\d+.xlsx"

    def __init__(self, path, file_type) -> None:
        super().__init__(path, file_type="xlsx")