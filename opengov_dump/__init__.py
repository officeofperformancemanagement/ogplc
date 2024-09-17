#to-do: sanitize, remove/lower time sleeps
import argparse
import csv
import datetime
import json
import os
import sys
import time

import pyodbc

# avoid _csv.Error: field larger than field limit (131072)
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    # OverflowError: Python int too large to convert to C long
    csv.field_size_limit(2147483647)  # maximum value of a long

def dump(
    command: str,
    server: str,
    database: str,
    username: str,
    password: str,
    output: str,
    driver: str,
    record_types: list = [],
    max_rows: int = 1_000_000_000
):
    print("[opengov-dump] starting dump")
    print("maxrow:", [max_rows])

    if command != "plc":
        raise Exception('[opengov-dump] missing plc, should read "opengov-dump plc ..."')

    if not output:
        raise Exception('[opengov-dump] missing output')

    if not (output and os.path.isabs(output) and os.path.isdir(output)):
        raise Exception('[opengov-dump] output must be an absolute path to a pre-existing directory')

    if max_rows is None:
        max_rows = 1_000_000_000

    if not database:
        raise Exception('[opengov-dump] missing database, should read "opengov-dump plc --database="..."')

    if not server:
        raise Exception('[opengov-dump] missing server, should read "opengov-dump plc --server="..."')

    if not username:
        raise Exception('[opengov-dump] missing username, should read "opengov-dump plc --username="..."')

    if not password:
        raise Exception('[opengov-dump] missing password, should read "opengov-dump plc --password="..."')

    if not driver:
        available_drivers = pyodbc.drivers()

        if not available_drivers:
            print("no available SQL Server drivers")

        if "SQL Server" in available_drivers:
            driver = "SQL Server"
        elif "ODBC Driver 11 for SQL Server" in available_drivers:
            driver = "ODBC Driver 11 for SQL Server"
        else:
            # select first driver available
            driver = available_drivers[0]
        print(f'[opengov-dump] automatically chose driver "{driver}"')

    params = {
        "DRIVER": "{" + driver + "}",
        "SERVER": server,
        "DATABASE": database,
        "UID": username,
        "PWD": password
    }

    connection_string = ";".join(["=".join(item) for item in params.items()])

    # print("connection_string:", connection_string)

    cnxn = pyodbc.connect(connection_string)

    print("[opengov-dump] connected to database")

    cursor = cnxn.cursor()
    print("[opengov-dump] created database cursor")

    # get all record types
    cursor.execute("SELECT DISTINCT recordType FROM apiRecords;")

    all_record_types = list([row[0] for row in cursor.fetchall()])
    print("[opengov-dump] got all record types:", all_record_types)

    trimmed_record_types = [it.strip() for it in all_record_types]
    print("[opengov-dump] trimmed all record types just in case there are some extra spaces")

    # user didn't select specific record types to dump, so select all of them!
    if not record_types:
        record_types = all_record_types
        print("[opengov-dump] dumping all record types")

    
    for record_type in record_types:
        print(f'dumping all records for type "{record_type}"')
        time.sleep(2)

        if record_type not in all_record_types and record_type not in trimmed_record_types:
            raise Exception(f"invalid record type: {record_type}")

        slug = record_type.lower().replace(" ", "_").replace("-", "_").replace("(", "_").replace(")", "_")

        dataset_folder = os.path.join(output, slug)

        if not os.path.isdir(dataset_folder):
            os.mkdir(dataset_folder)
            print("[opengov-dump] created {dataset_folder}")

        # write metadata file
        metadata_path = os.path.join(dataset_folder, "metadata.json")
        print("metadata_path:", metadata_path)

        metadata = {
            "Record Type": record_type,
            "Updated": str(datetime.datetime.now()),
            "Slug": slug
        }
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, ensure_ascii=True, indent=4)
        print(f"saved metadata to: {metadata_path}")

        outfile = os.path.join(dataset_folder, "dataset.csv")
        print(f'[opengov-dump] exporting to "{outfile}"')

        for it in all_record_types:
            if it != record_type and it.strip() == record_type.strip():
                record_type = it
                print(f'set record_type to "{record_type}"')

        cursor.execute(f"SELECT DISTINCT formSectionLabel, formFieldLabel FROM apiFormData LEFT JOIN apiRecords ON apiRecords.recordId = apiFormData.recordID WHERE apiRecords.recordType = '{record_type}';")

        all_form_questions = list([(row[0].strip(), row[1].strip()) for row in cursor.fetchall()])
        print("all_form_questions:", all_form_questions)

        cursor.execute(f"SELECT apiRecords.*, apiFormData.* FROM apiRecords LEFT JOIN apiFormData ON apiRecords.recordID = apiFormData.recordID WHERE apiRecords.recordType = '{record_type}';")
        print('cursor.description:', cursor.description)

        column_names = [column[0] for column in cursor.description]
        print("column_names:", column_names)

        # column names for the output csv 
        fieldnames = column_names[:column_names.index("formFieldID") - 1] + [section + ": " + label for section, label in all_form_questions]
        print("fieldnames:", fieldnames)

        time.sleep(2)
        with open(outfile, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            print(f"wrote header to {outfile}")

        written = 0
        outrow = None
        for row in cursor:
            dbrow = dict(zip(column_names, row))

            # trim values
            dbrow = dict([(k.strip(), v.strip() if isinstance(v, str) else v) for k, v in dbrow.items()])

            # print(dbrow)
            formFieldID = dbrow.pop("formFieldID")
            formSectionLabel = dbrow.pop("formSectionLabel").strip()
            formFieldLabel = dbrow.pop("formFieldLabel").strip()
            formFieldEntry = dbrow.pop("formFieldEntry")

            if isinstance(formFieldEntry, str):
                formFieldEntry = formFieldEntry.strip()

            # first row only
            if outrow is None: outrow = dbrow

            if dbrow['recordID'] != outrow['recordID']:
                time.sleep(2)
                with open(outfile, "a", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writerow(outrow)
                    written += 1
                outrow = dbrow

            outrow[formSectionLabel + ": " + formFieldLabel] = formFieldEntry

            if written >= max_rows:
                break

        time.sleep(2)
        with open(outfile, "a") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writerow(outrow)
        print(f'finished writing "{outfile}"')

    cnxn.close()

    print("[opengov-dump] finished dump")

def main():
    parser = argparse.ArgumentParser(
        prog="opengov-dump",
        description="Dump OpenGov instance into a folder of CSV files",
    )

    parser.add_argument("command", help='command, currently only "plc" is supported')

    parser.add_argument(
        "--server",
        type=str,
        help='domain of database server',
    )

    parser.add_argument(
        "--database",
        type=str,
        help='name of database',
    )

    parser.add_argument(
        "--username",
        type=str,
        help='username for connecting to database',
    )

    parser.add_argument(
        "--password",
        type=str,
        help='password for connecting to database',
    )

    parser.add_argument(
        "--output",
        type=str,
        help='absolute path to output folder',
    )

    parser.add_argument(
        "--driver",
        type=str,
        help='optional database driver. defaults to first available ODBC driver already installed on system',
    )

    parser.add_argument(
        "--record-types",
        type=str,
        help='optional comma separated list of record types to export. default is everything',
    )

    parser.add_argument(
        "--max-rows",
        type=int,
        help='optional maximum number of rows',
    )

    args = parser.parse_args()

    if args.record_types:
        args.record_types = [it.strip() for it in args.record_types.split(",")]

    dump(**vars(args))

if __name__ == "__main__":
    main()
