# Tyler Data & Insights prevents datasets of larger than 500 columns
# We need to find an effective way of identifying date columns by other than regex

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


def escape_string(str):
    return "".join(
        [
            c
            for c in str
            if c in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ -()/_"
        ]
    )


def is_safe(str):
    return str == escape_string(str)


def slugify(text):
    return escape_string(
        text.lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace("(", "_")
        .replace(")", "_")
        .replace("/", "_")
        .replace("___", "_")
        .replace("__", "_")
        .strip("_")
    )


def remove_new_lines_from_string(text):
    return text.replace("\n", "").replace("\r", "").strip()


def dump(
    server: str,
    database: str,
    username: str,
    password: str,
    output: str,
    driver: str,
    record_types: list = [],
    skip_record_types: list = [],
    max_rows: int = 1_000_000_000,
    wait: int = 0,
    remove_new_lines=True,
    delimiter=",",
    debug_level=0,
):
    if debug_level is None:
        debug_level = 0

    if debug_level >= 1:
        print("[ogplc] starting dump")

    if not output:
        raise Exception("[ogplc] missing output")

    if not (output and os.path.isabs(output) and os.path.isdir(output)):
        raise Exception(
            "[ogplc] output must be an absolute path to a pre-existing directory"
        )

    if max_rows is None:
        max_rows = 1_000_000_000

    if remove_new_lines is None:
        remove_new_lines = True

    if not database:
        raise Exception(
            '[ogplc] missing database, should read "ogplc dump --database="..."'
        )

    if not server:
        raise Exception(
            '[ogplc] missing server, should read "ogplc dump --server="..."'
        )

    if not username:
        raise Exception(
            '[ogplc] missing username, should read "ogplc dump --username="..."'
        )

    if not password:
        raise Exception(
            '[ogplc] missing password, should read "ogplc dump --password="..."'
        )

    if not driver:
        available_drivers = pyodbc.drivers()

        if not available_drivers:
            if debug_level >= 2:
                print("no available SQL Server drivers")

        if "SQL Server" in available_drivers:
            driver = "SQL Server"
        elif "ODBC Driver 11 for SQL Server" in available_drivers:
            driver = "ODBC Driver 11 for SQL Server"
        else:
            # select first driver available
            driver = available_drivers[0]
        if debug_level >= 2:
            print(f'[ogplc] automatically chose driver "{driver}"')

    params = {
        "DRIVER": "{" + driver + "}",
        "SERVER": server,
        "DATABASE": database,
        "UID": username,
        "PWD": password,
    }

    connection_string = ";".join(["=".join(item) for item in params.items()])
    if debug_level >= 10:
        print("[ogplc] connection_string:", connection_string)

    cnxn = pyodbc.connect(connection_string)
    if debug_level >= 2:
        print("[ogplc] connected to database")

    cursor = cnxn.cursor()
    if debug_level >= 2:
        print("[ogplc] created database cursor")

    # get all record types
    cursor.execute("SELECT DISTINCT recordType FROM apiRecords;")

    all_record_types = list([row[0] for row in cursor.fetchall()])
    if debug_level >= 2:
        print("[ogplc] got all record types:", all_record_types)

    trimmed_record_types = [it.strip() for it in all_record_types]
    if debug_level >= 2:
        print(
            "[ogplc] trimmed all record types just in case there are some extra spaces"
        )

    if not delimiter:
        delimiter = ","
    if debug_level >= 2:
        print("[ogplc] delimiter is", delimiter)

    # user didn't select specific record types to dump, so select all of them!
    if not record_types:
        record_types = all_record_types
        if debug_level >= 2:
            print("[ogplc] dumping all record types")

    if not skip_record_types:
        skip_record_types = []

    for record_type in record_types:
        if record_type in skip_record_types:
            if debug_level >= 2:
                print(f'[ogplc] skipping "{record_type}"')
            continue

        if debug_level >= 2:
            print(f'[ogplc] dumping all records for type "{record_type}"')

        # skip if unsafe characters in string like apostrophes
        # prevents SQL injection
        if not is_safe(record_type):
            if debug_level >= 2:
                print(f"[ogplc] skipping unsafe string: {escape_string(record_type)}")
            continue

        if (
            record_type not in all_record_types
            and record_type not in trimmed_record_types
        ):
            raise Exception(f"[ogplc] invalid record type: {record_type}")

        slug = slugify(record_type)

        dataset_folder = os.path.join(output, slug)

        if not os.path.isdir(dataset_folder):
            os.mkdir(dataset_folder)
            if debug_level >= 1:
                print("[ogplc] created {dataset_folder}")

        # write metadata file
        metadata_path = os.path.join(dataset_folder, "metadata.json")
        if debug_level >= 1:
            print("[ogplc] metadata_path:", metadata_path)

        # maybe add row count? data types?
        # write .csvt?
        metadata = {
            "Record Type": record_type,
            "Updated": str(datetime.datetime.now()),
            "Slug": slug,
        }
        with open(metadata_path, "w", newline="", encoding="utf-8") as f:
            json.dump(metadata, f, ensure_ascii=True, indent=4)
        if debug_level >= 1:
            print(f"[ogplc] saved metadata to: {metadata_path}")

        outfile = os.path.join(dataset_folder, "dataset.csv")
        if debug_level >= 1:
            print(f'[ogplc] exporting to "{outfile}"')

        for it in all_record_types:
            if it != record_type and it.strip() == record_type.strip():
                record_type = it
                if debug_level >= 1:
                    print(f'[ogplc] set record_type to "{record_type}"')

        time.sleep(wait)
        statement = f"SELECT DISTINCT formSectionLabel, formFieldLabel FROM apiFormData LEFT JOIN apiRecords ON apiRecords.recordId = apiFormData.recordID WHERE apiRecords.recordType = '{record_type}';"
        if debug_level >= 1:
            print(f"[ogplc] executing: {statement}")
        cursor.execute(statement)

        all_form_questions = list(
            [(row[0].strip(), row[1].strip()) for row in cursor.fetchall()]
        )
        if debug_level >= 1:
            print("[ogplc] all_form_questions:", all_form_questions)

        time.sleep(wait)
        statement = f"SELECT DISTINCT step FROM apiApprovals LEFT JOIN apiRecords ON apiRecords.recordId = apiApprovals.recordid WHERE apiRecords.recordType = '{record_type}';"
        # statement = f"SELECT apiApprovals.recordid, COUNT(*) as c FROM apiApprovals LEFT JOIN apiRecords ON apiRecords.recordId = apiApprovals.recordid WHERE apiRecords.recordType = 'Commercial Building Permit' GROUP BY apiApprovals.recordid ORDER BY c DESC;"
        if debug_level >= 1:
            print(f"[ogplc] executing: {statement}")
        cursor.execute(statement)
        # max approval steps seems to be 50 even included uncompleted
        all_approval_steps = [row[0].strip().lower() for row in cursor.fetchall()]
        if debug_level >= 1:
            print("[ogplc] all_approval_steps:", all_approval_steps)

        # time.sleep(wait)
        # statement = f"SELECT TOP (1) COUNT(*) as _count_ FROM apiApprovals LEFT JOIN apiRecords ON apiRecords.recordId = apiApprovals.recordid WHERE apiRecords.recordType = '{record_type}' GROUP BY apiApprovals.recordid ORDER BY _count_ DESC;"
        # print(f"[ogplc] executing: {statement}")
        # cursor.execute(statement)
        # max_number_of_steps = list(cursor.fetchall())[0][0]
        # print("max_number_of_steps:", max_number_of_steps)

        time.sleep(wait)
        # statement = f"SELECT apiRecords.*, apiFormData.formFieldID, apiFormData.formSectionLabel, apiFormData.formFieldLabel, apiFormData.formFieldEntry, apiApprovals.* FROM apiRecords LEFT JOIN apiFormData ON apiRecords.recordID = apiFormData.recordID LEFT JOIN apiApprovals ON apiRecords.recordID = apiApprovals.recordID WHERE apiRecords.recordType = '{record_type}';"

        statement = f"SELECT apiRecords.*, apiFormData.formSectionLabel, apiFormData.formFieldLabel, apiFormData.formFieldEntry, apiApprovals.step, apiApprovals.completionDate FROM apiRecords LEFT JOIN apiFormData ON apiRecords.recordID = apiFormData.recordID LEFT JOIN apiApprovals ON apiRecords.recordID = apiApprovals.recordID WHERE apiRecords.recordType = '{record_type}';"
        if debug_level >= 1:
            print(f"[ogplc] executing: {statement}")
        cursor.execute(statement)
        if debug_level >= 1:
            print("[ogplc] cursor.description:", cursor.description)

        column_names = [column[0] for column in cursor.description]
        if debug_level >= 1:
            print("[ogplc] column_names:", column_names)

        # column names for the output csv
        fieldnames = column_names[: column_names.index("formSectionLabel")] + [
            section + ": " + label for section, label in all_form_questions
        ]
        if debug_level >= 1:
            print("[ogplc] fieldnames:", fieldnames)
        if debug_level >= 1:
            print("[ogplc] number of fieldnames:", len(fieldnames))

        for step in all_approval_steps:
            fieldnames.append(f"{step}: completion date")

        with open(outfile, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter)
            writer.writeheader()
            if debug_level >= 1:
                print(f"[ogplc] wrote header to {outfile}")

        written = 0
        outrow = None
        for row in cursor:
            # print("\ncolumn_names:", column_names)
            # print("\nrow:", row)
            dbrow = dict(zip(column_names, row))
            # print("\ndbrow:", dbrow)
            if dbrow["recordID"] is None:
                raise Exception("uhoh")

            # trim values
            dbrow = dict(
                [
                    (
                        k.strip().replace("\t", ""),
                        v.strip().replace("\t", "") if isinstance(v, str) else v,
                    )
                    for k, v in dbrow.items()
                ]
            )

            # Tyler Data & Insights doesn't support dates with millisecond precision
            # so we truncate precision to seconds
            for key in ["dateCreated", "dateSubmitted", "dateCompleted"]:
                if (
                    str(dbrow[key].__class__) == "<class 'datetime.datetime'>"
                    and dbrow[key].microsecond
                ):
                    dbrow[key] = dbrow[key].replace(microsecond=0)

            if remove_new_lines:
                dbrow = dict(
                    [
                        (
                            remove_new_lines_from_string(k),
                            remove_new_lines_from_string(v)
                            if isinstance(v, str)
                            else v,
                        )
                        for k, v in dbrow.items()
                    ]
                )

            # print(dbrow)
            formSectionLabel = dbrow.pop("formSectionLabel")
            formFieldLabel = dbrow.pop("formFieldLabel")
            formFieldEntry = dbrow.pop("formFieldEntry")

            if isinstance(formSectionLabel, str):
                formSectionLabel = formSectionLabel.strip()

            if isinstance(formFieldLabel, str):
                formFieldLabel = formFieldLabel.strip()

            if isinstance(formFieldEntry, str):
                formFieldEntry = formFieldEntry.strip()

            completionDate = dbrow.pop("completionDate")
            step = dbrow.pop("step")

            if isinstance(step, str):
                # step names have dynamic casing, so need to standardize
                step = step.strip().lower()

            # first row only
            if outrow is None:
                outrow = dbrow

            # print('dbrow["recordID"]:', dbrow["recordID"])
            # print('outrow["recordID"]:', outrow["recordID"])
            if dbrow["recordID"] != outrow["recordID"]:
                # remove anything from the outrow that isn't in the fieldnames
                # this could indicate a question that was removed or reworded
                keys = list(outrow.keys())
                for key in keys:
                    if key not in fieldnames:
                        if debug_level >= 1:
                            print(f'[ogplc] skipping obsolete column: "{key}"')
                        if debug_level >= 1:
                            print("fieldnames:", fieldnames)
                        del outrow[key]
                        return

                with open(outfile, "a", newline="", encoding="utf-8") as f:
                    try:
                        writer = csv.DictWriter(
                            f, fieldnames=fieldnames, delimiter=delimiter
                        )
                        writer.writerow(outrow)
                        written += 1

                        if written % 100 == 0:
                            if debug_level >= 1:
                                print(f"[ogplc] wrote {written} rows to {outfile}")

                    except Exception as e:
                        print("fieldnames:", fieldnames)
                        print("outrow:", outrow)
                        raise e
                outrow = dbrow

            if formSectionLabel and formFieldLabel:
                outrow[formSectionLabel + ": " + formFieldLabel] = formFieldEntry

            if step:
                outrow[f"{step}: completion date"] = completionDate

            if written >= max_rows:
                break

        with open(outfile, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=delimiter)
            writer.writerow(outrow)
        if debug_level >= 1:
            print(f'[ogplc] finished writing "{outfile}"')

    cnxn.close()

    if debug_level >= 1:
        print("[ogplc] finished dump")


def main():
    parser = argparse.ArgumentParser(
        prog="ogplc",
        description="High-Level Database Client for OpenGov Permitting & Licensing",
    )

    parser.add_argument("command", help='command, currently only "dump" is supported')

    parser.add_argument(
        "--server",
        type=str,
        help="domain of database server",
    )

    parser.add_argument(
        "--database",
        type=str,
        help="name of database",
    )

    parser.add_argument(
        "--username",
        type=str,
        help="username for connecting to database",
    )

    parser.add_argument(
        "--password",
        type=str,
        help="password for connecting to database",
    )

    parser.add_argument(
        "--output",
        type=str,
        help="absolute path to output folder",
    )

    parser.add_argument(
        "--driver",
        type=str,
        help="optional database driver. defaults to first available ODBC driver already installed on system",
    )

    parser.add_argument(
        "--record-types",
        type=str,
        help="optional comma separated list of record types to export. default is everything",
    )

    parser.add_argument(
        "--skip-record-types",
        type=str,
        help="optional comma separated list of record types to skip. default is nothing",
    )

    parser.add_argument(
        "--max-rows",
        type=int,
        help="optional maximum number of rows",
    )

    parser.add_argument(
        "--wait",
        type=int,
        help="optional wait between database requests",
    )

    parser.add_argument(
        "--remove-new-lines",
        type=int,
        help="optional remove new line characters from output",
    )

    parser.add_argument(
        "--delimiter",
        type=str,
        help="optional delimiter, default is comma",
    )

    parser.add_argument(
        "--debug-level",
        type=int,
        help="optional debug level, default is zero",
    )

    args = parser.parse_args()

    if args.record_types:
        args.record_types = [it.strip() for it in args.record_types.split(",")]

    if args.skip_record_types:
        args.skip_record_types = [
            it.strip() for it in args.skip_record_types.split(",")
        ]

    if args.delimiter == "\\t":
        args.delimiter = "\t"

    if args.command != "dump":
        raise Exception('[ogplc] missing dump, should read "ogplc dump ..."')
    del args.command

    dump(**vars(args))


if __name__ == "__main__":
    main()
