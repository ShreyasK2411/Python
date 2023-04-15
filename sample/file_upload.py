import argparse
from Upload import UploadToSql

if __name__=="__main__":
    # setting the arguments for the utility
    parser = argparse.ArgumentParser()
    parser.add_argument("-d","--source_dir",
                        type=str,
                        help="Source directory from which we need to read all files to upload to mysql")

    parser.add_argument("-c","--config_file",
                        type=str,
                        help="Path of a JSON file which contains all details of MySQL to which to connect to")

    parser.add_argument("-t","--dest_table",
                        type=str,
                        help="Name of table to upload this data to")

    # storing the arguments data in a variable
    args = parser.parse_args()
    upload = UploadToSql(args.source_dir,args.dest_table,args.config_file)
    try:
        upload.read()
        upload.configure()
        upload.connect_to_mysql()
        upload.insert()
    finally:
        upload.close()