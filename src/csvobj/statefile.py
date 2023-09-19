import json
from .csv import CSVFile


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

    def check_csv_is_parsed(self, csv):
        csv_path = csv.path
        return self.state_data.get(csv_path, False)

    def set_csv_read(self, csv: CSVFile, is_read: bool):
        csv_path = csv.path
        self.state_data[csv_path] = is_read
        self.save_state()
