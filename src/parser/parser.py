import polars as pl
from logger.log import setup_logger
import logging
from csvobj.statusfile import StatusFile
from csvobj.csv import CSVFile
import glob

logger = setup_logger("processor.log", logging.DEBUG)


class CSVProcessor:
    def __init__(self, sf: StatusFile) -> None:
        self.sf = sf

    def parse(self, csv: CSVFile):
        if not self.sf.check_csv_is_parsed(csv):
            try:
                logger.info(f"parsing {csv.path}")
                # df = pl.read_csv(csv.path, separator=csv.seperator,
                #                  n_threads=8, use_pyarrow=True,
                #                  low_memory=True, batch_size=2048)
                df = pl.read_csv_batched(csv.path, separator=csv.seperator)
                self.sf.set_csv_read(csv, True)
                logger.info(f"{csv.path}: True")
                return df
            except Exception as e:
                raise (e)


CDR_PATH = "./csv/*"
CSV_FILES = glob.glob(CDR_PATH)


class Scanner:
    def __init__(self, status_file: StatusFile, scan_path) -> None:
        self.scan_path = glob.glob(scan_path)
        self.sf = status_file

    def scan(self):
        res = []
        for file in self.scan_path:
            csv = CSVFile(file)
            res.append(csv)
        return res
