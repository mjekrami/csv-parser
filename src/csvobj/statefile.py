import json
from .file import File
from pathlib import Path

class StateFile:
    def __init__(self, path) -> None:
        self.path = path
        self.state_data = self._load_state()

    def _load_state(self):
        try:
            with open(self.path, "r") as sf:
                return json.load(sf)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_state(self):
        with open(self.path, "w") as sf:
            json.dump(self.state_data, sf)

    def check_file_is_parsed(self, file):
        return self.state_data.get(file.path, False)

    def set_file_read(self, file: File, is_read: bool):
        file_path = file.path
        self.state_data[file_path] = is_read
        self.save_state()
