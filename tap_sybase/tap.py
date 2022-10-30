"""sybase tap class."""

from typing import List

from singer_sdk import SQLTap, SQLStream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_sybase.client import sybaseStream


class Tapsybase(SQLTap):
    """sybase tap class."""
    name = "tap-sybase"
    default_stream_class = sybaseStream

    config_jsonschema = th.PropertiesList(
        th.Property(
            "user",
            th.StringType,
            required=True,
            description="The user name used to connect to the Sybase database"
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The password for the connecting user to the Sybase database"
        ),
        th.Property(
            "host",
            th.StringType,
            required=True,
            description="The host name or IP Address running the Sybase Database"
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=2638,
            required=False,
            description="The port that the database is running on, defaults to port 2638"
        ),
        th.Property(
            "filter_dbs",
            th.StringType,
            required=False,
            description="To filter the discovery to a particular schema within a database. "
            "This is useful if you have a large number of schemas and wish to speed up the "
            "discovery."
        ),
        th.Property(
            "use_date_datatype",
            th.BooleanType,
            default=False,
            required=False,
            description="To emit a date as a date without a time component or time without an UTC "
            "offset. This is helpful to avoid time conversions or to just work with a date "
            "datetype in the target database. If this boolean config item is not set, the default "
            "behaviour is `false` i.e. emit date datatypes as a datetime. It is recommended to set "
            "this on if you have time datetypes and are having issues uploading into into a target "
            "database."
        ),
        th.Property(
            "tds_version",
            th.StringType,
            default=None,
            required=False,
            description="Set the version of TDS to use when communicating with Sybase Server (the "
            "default is None). This is used by pymssql with connecting and fetching data from "
            "Sybase databases. See the pymssql documentation and FreeTDS documentation for more "
            " details."
        ),
        th.Property(
            "characterset",
            th.StringType,
            default="utf8",
            required=False,
            description="The characterset for the database / source system. The default is `utf8`, "
            "however older databases might use a charactersets like cp1252 for the encoding. "
            "If you have errors with a UnicodeDecodeError: 'utf-8' codec can't decode byte .... "
            "then a solution is examine the characterset of the source database / system and make "
            "an appropriate substitution for utf8 like cp1252."
        ),
        th.Property(
            "cursor_array_size",
            th.IntegerType,
            default=1,
            required=False,
            description="To make use of fetchmany(x) instead of fetchone(), use cursor_array_size "
            "with an integer value indicating the number of rows to pull. This can help in some "
            "architectures by pulling more rows into memory. The default if omitted is 1, the tap "
            "will still use fetchmany, but with an argument of 1, under the assumption that  "
            "like cp1252."
        ),
        th.Property(
            "use_singer_decimal",
            th.BooleanType,
            default=False,
            required=False,
            description="o emit all numeric values as strings and treat floats as string data types for the target, "
            "set use_singer_decimal to true. The resulting SCHEMA message will contain an attribute in "
            "additionalProperties containing the scale and precision of the discovered property"
        ),
    ).to_dict()


if __name__ == "__main__":
    Tapsybase.cli()
