# ogplc
High-Level Database Client for OpenGov Permitting & Licensing

## install
```sh
pip install ogplc
```

## cli usage
```sh
# dump to a datasets folder
ogplc dump --username="myusername" --password="mypassword" --server="myserver" --database="mydatabase" --output="$PWD/datasets"
```

## python usage
```py
import ogplc

ogplc.dump(
    server = "server_host",
    database = "database_name",
    username = "username",
    password = "password",

    # folder where to save output files
    output = "/home/users/documents/datasets",

    # optional parameters below
    driver = "SQL Server",

    # only include these record types in results
    record_types = ["Skyscraper Permit"],

    # exclude these record types from results
    skip_record_types = ["Orchard Permit"],

    # maximum number of rows in output dataset for each record type
    max_rows = 1000,

    # number of seconds to wait between database queries
    wait = 0,

    # remove new line from databse results
    # this can help improve readibility
    remove_new_lines = True,

    # delimiter to pass to csv writer
    delimiter = ",",

    # logging level
    debug_level = 1
)
```
