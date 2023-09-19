import polars as pl
from logger.log import setup_logger
import logging
from csvobj.statefile import StateFile
from csvobj.csv import CSVFile
import glob

logger = setup_logger("processor.log", logging.DEBUG)


class CSVProcessor:
    def __init__(self, sf: StateFile) -> None:
        self.sf = sf

    def parse(self, csv: CSVFile):
        if not self.sf.check_csv_is_parsed(csv):
            try:
                logger.info(f"parsing {csv.path}")
                df = pl.read_csv_batched(csv.path, separator=csv.seperator)
                self.sf.set_csv_read(csv, True)
                logger.info(f"{csv.path}: True")
                return df
            except Exception as e:
                raise (e)


class Scanner:
    def __init__(self, State_file: StateFile, scan_path) -> None:
        self.scan_path = glob.glob(scan_path)
        self.sf = State_file

    def scan(self):
        res = []
        for file in self.scan_path:
            csv = CSVFile(file)
            res.append(csv)
        return res
