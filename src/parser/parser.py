from abc import abstractmethod

from logger.log import setup_logger
from csvobj.statefile import StateFile

from csvobj.csv import CSVFile
from csvobj.file import File
from csvobj.xlsx import XLSXFile

import polars as pl
import logging
import glob
import os

logger = setup_logger("processor.log", logging.DEBUG)


class AbastractParser:
    def __init__(self, sf: StateFile) -> None:
        self.sf = sf
    
    @abstractmethod
    def parse(self, file: File):
        pass
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

    def parse(self, file:File):
        if file.file_type == "xlsx":
            p = XLSXParser(self.sf)
            return p.parse(file)
        elif file.file_type == "csv":
            p = CSVParser(self.sf)
            return p.parse(file)
        else:
            raise Exception(f"could not parse filetype {file.file_type}")



class Scanner:
    def __init__(self, State_file: StateFile, scan_path) -> None:
        self.scan_path = glob.glob(scan_path)
        self.sf = State_file

    def scan(self):
        res = []
        for file in self.scan_path:
            file_extension = os.path.splitext(file)[-1].lower()
            file_path = os.path.realpath(file)
            if file_extension == ".csv":
                csv = CSVFile(file,";")
                res.append(csv)
            elif file_extension == ".xlsx":
                xlsx = XLSXFile(file, file_path)
                res.append(xlsx)
            else:
                raise Exception("file type is not supported: {file}")
        return res
