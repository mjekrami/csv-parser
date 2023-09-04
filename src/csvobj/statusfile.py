import json
import os.path
from .csv import CSVFile 
class StatusFile:
    def __init__(self, path) -> None:
        self.path = path
        self.status_data = self._load_status()

    def _load_status(self):
        try:
            with open(self.path, "r") as sf:
                return json.load(sf)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def save_status(self):
        with open(self.path, "w") as sf:
            json.dump(self.status_data, sf)

    def check_csv_is_parsed(self, csv):
        csv_path = csv.path
        return self.status_data.get(csv_path, False)

    def set_csv_read(self, csv:CSVFile,is_read:bool):
        csv_path = csv.path
        self.status_data[csv_path] = is_read
        self.save_status()
