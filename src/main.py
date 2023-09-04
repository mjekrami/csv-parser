import argparse

from csvobj.csv import CSVFile
from csvobj.statusfile import StatusFile

from parser.parser import CSVProcessor, Scanner

from logger.log import setup_logger
from logging import INFO


CONNECTION_STRING = "<>"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--log-path", dest="log_path",
                        help="Path to the logging file",
                        default="/dev/stdout")

    parser.add_argument("--status-path", dest="status_path",
                        required=True, help="Path to the status file")
    parser.add_argument("--scan-path", dest="scan_path",
                        required=True, help="Path to scan CSV files")
    args = parser.parse_args()
    logger = setup_logger(args.log_path, INFO)
    sf = StatusFile(args.status_path)
    parser = CSVProcessor(sf)
    scanner = Scanner(sf, args.scan_path)
    files: [CSVFile | None] = scanner.scan()
    for file in files:
        reader = parser.parse(file)
        if reader is not None:
            try:
                batches = reader.next_batches(100)
                tablename = file.tablename()
                while batches:
                    for batch in batches:
                        batch.write_database(tablename, engine="sqlalchemy",
                                             connection_uri=CONNECTION_STRING,
                                             if_exists="replace")
                    batches = reader.next_batches(100)

                logger.info(
                    f"Successfully wriiten {file.path} to {tablename}")
            except Exception as e:
                sf.set_csv_read(file, False)
                logger.error(e)