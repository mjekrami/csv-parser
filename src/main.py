import argparse

from files.file import File
from files.statefile import StateFile

from parser.parser import Parser, Scanner

from logger.log import setup_logger
from logging import INFO


CONNECTION_STRING = "mysql+pymysql://root:mamali75@localhost:3306/cdr"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--log-path", dest="log_path",
                        help="Path to the logging file",
                        default="/dev/stdout")

    parser.add_argument("--state-path", dest="state_path",
                        required=True, help="Path to the State file")
    parser.add_argument("--scan-path", dest="scan_path",
                        required=True, help="Path to scan CSV files")
    args = parser.parse_args()
    logger = setup_logger(args.log_path, INFO)
    sf = StateFile(args.state_path)
    parser = Parser(sf)
    scanner = Scanner(args.scan_path if args.scan_path else "./*")
    files: [File | None] = scanner.scan()
    for file in files:
        reader = parser.parse(file)
        if reader is not None:
            try:
                batches = reader.next_batches(100)
                tablename = file.tablename
                while batches:
                    for batch in batches:
                        batch.write_database(tablename, engine="sqlalchemy",
                                             connection=CONNECTION_STRING,
                                             if_exists="append")
                    batches = reader.next_batches(100)
                logger.info(
                    f"Successfully wriiten {file.path} to {tablename}")
            except Exception as e:
                sf.set_file_read(file, False)
                raise(e)
