from abc import abstractmethod

from watchdog.observers import Observer

from logger.log import setup_logger
from files.statefile import StateFile
from files.csv import CSVFile
from files.file import File
from files.xlsx import XLSXFile

import polars as pl
import logging
import glob
import os
import csv

logger = setup_logger("processor.log", logging.DEBUG)


class AbastractParser:
    _DATE_COLUMNS = ["Date", "Day"]

    def __init__(self, sf: StateFile) -> None:
        self.sf = sf

    @abstractmethod
    def parse(self, file: File):
        pass

    def parse_dates(self, df: pl.DataFrame):
        for date in self._DATE_COLUMNS:
            if date in df.columns:
                date_col = df.columns[0]
                df = df.with_columns(
                    pl.col(date_col).str.strptime(pl.Date, format="%Y.%m.%d")
                )
                return df
        raise Exception("Could not parse dates")


class CSVParser(AbastractParser):
    def parse(self, csv: CSVFile):
        if not self.sf.check_file_is_parsed(csv):
            try:
                logger.info(f"parsing {csv.path}")
                df = pl.read_csv_batched(csv.path, separator=csv.seperator)
                self.sf.set_file_read(csv, True)
                logger.info(f"{csv.path}: True")
                return df
            except Exception as e:
                raise (e)


class XLSXParser(AbastractParser):
    def parse(self, xlsx: XLSXFile):
        if not self.sf.check_file_is_parsed(xlsx):
            try:
                logger.info(f"parsing {xlsx.path}")
                df = pl.read_excel(xlsx.path)
                self.sf.set_file_read(xlsx, True)
                logger.info(f"{xlsx.path}: True")
                return df
            except Exception as e:
                raise (e)


class Parser(AbastractParser):
    def parse(self, file: File):
        if file.file_type == "xlsx":
            p = XLSXParser(self.sf)
            return p.parse(file)
        elif file.file_type == "csv":
            p = CSVParser(self.sf)
            return p.parse(file)
        else:
            raise Exception(f"could not parse filetype {file.file_type}")


class Scanner:
    def __init__(self, scan_path) -> None:
        self.scan_path = glob.glob(scan_path)

    def scan(self):
        res = []
        for file in self.scan_path:
            file_extension = os.path.splitext(file)[-1].lower()
            file_path = os.path.realpath(file)

            if file_extension == ".csv":
                separator = self._get_csv_separator(file)
                csv_file = CSVFile(file, separator)
                res.append(csv_file)

            elif file_extension == ".xlsx":
                xlsx = XLSXFile(file, file_path)
                res.append(xlsx)

            else:
                raise Exception("file type is not supported: {file}")
        return res

    def _get_csv_separator(self, file):
        with open(file, "r", newline="") as csvfile:
            first_line = csvfile.readline()
            dialect = csv.Sniffer().sniff(first_line)
            csvfile.close()
        return dialect.delimiter
