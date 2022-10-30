"""SQL client handling.

This includes sybaseStream and sybaseConnector.
"""
from typing import Iterable, Optional, Dict, Any, List

import urllib.parse
import sqlalchemy

from singer_sdk import SQLConnector, SQLStream


class sybaseConnector(SQLConnector):
    """Connects to the sybase SQL source."""

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """SQLAlchemy URL for connecting to the Sybase source."""

        return (
            f"mssql+pymssql://{config['user']}:"
            f"{urllib.parse.quote_plus(config['password'])}@"
            f"{config['host']}:{config['port']}/"
            f"{config['database']}"
        )

    def create_sqlalchemy_engine(self) -> sqlalchemy.engine.Engine:
        """Return a new SQLAlchemy engine using the provided config.
        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.
        Returns:
            A newly created SQLAlchemy engine object.
        """

        return sqlalchemy.create_engine(
            self.sqlalchemy_url,
            echo=False,
            connect_args=
                {"charset": self.config['characterset'],
                 "tds_version": self.config.get('tds_version',None),
                 "conn_properties": ''
                },
        )

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Return information about the primary key constraint on
        table_name`.
        Given a :class:`_engine.Connection`, a string
        `table_name`, and an optional string `schema`, return primary
        key information as a dictionary with these keys:
        constrained_columns
          a list of column names that make up the primary key
        name
          optional name of the primary key constraint.
        """
        sql = """
            SELECT
                CONST.CONSTNAME AS PK_NAME,
                KEY.COLNAME AS COLUMN_NAME
            FROM
            SYSCAT.TABLES TAB
            INNER JOIN SYSCAT.TABCONST CONST
                ON CONST.TABSCHEMA = TAB.TABSCHEMA
                AND CONST.TABNAME = TAB.TABNAME AND CONST.TYPE = 'P'
            INNER JOIN SYSCAT.KEYCOLUSE KEY
                ON CONST.TABSCHEMA = KEY.TABSCHEMA
                AND CONST.TABNAME = KEY.TABNAME
                AND CONST.CONSTNAME = KEY.CONSTNAME
            WHERE TAB.TYPE = 'T'
                AND TAB.TABSCHEMA NOT LIKE 'SYS%'
        """

        result = connection.execute(sql).fetchall()
        output = {"constrained_columns": []}
        for constraint in result:
            output["name"] = constraint[0]
            output["constrained_columns"].append(constraint[1])

        return output

    def get_columns(self, connection, table_name, schema=None, **kw):
        """Return information about columns in `table_name`.
        Given a :class:`_engine.Connection`, a string
        `table_name`, and an optional string `schema`, return column
        information as a list of dictionaries with these keys:
        name
            the column's name
        type
            [sqlalchemy.types#TypeEngine]
        nullable
            boolean
        default
            the column's default value
        autoincrement
            boolean
        sequence
            a dictionary of the form
                {'name' : str, 'start' :int, 'increment': int, 'minvalue': int,
                'maxvalue': int, 'nominvalue': bool, 'nomaxvalue': bool,
                'cycle': bool, 'cache': int, 'order': bool}
        Additional column attributes may be present.
        """

        sql = """
            SELECT
                s."NAME" AS name,
                s."COLTYPE" AS "type",
                s."NULLS" AS nullable,
                CAST(s."DEFAULT" AS VARCHAR) AS "default",
                c."INCREMENT" = 1 AS autoincrement,
                c."COLNAME" AS sequence_name,
                c."START" AS sequence_start,
                c."MINVALUE"  AS sequence_minvalue,
                c."MAXVALUE" AS sequence_maxvalue,
                c.COLNAME IS NOT NULL AND c."MINVALUE" IS NULL
                    AS sequence_nominvalue,
                c.COLNAME IS NOT NULL AND c."MAXVALUE" IS NULL
                    AS sequence_nomaxvalue,
                c.CYCLE = 'Y' AS sequence_cycle,
                c.CACHE AS sequence_cache,
                c.ORDER = 'Y' AS sequence_order
            FROM SYSIBM.SYSCOLUMNS s
            LEFT JOIN SYSCAT.TABLES t
            ON s.TBNAME = t.TABNAME
            LEFT JOIN SYSCAT.COLIDENTATTRIBUTES c
            ON s.NAME = c.COLNAME
            WHERE s.TBNAME = '{}'
            AND t.TABSCHEMA = '{}'
        """.format(
            table_name.upper(),
            schema.upper(),
        )
        result = connection.execute(sql).fetchall()
        columns = []
        for row in result:
            column = {"sequence": {}}
            for k, v in row._mapping.items():
                if "sequence_" not in k:
                    column[k] = str(v)
                else:
                    k = k.split("_")[1]
                    column["sequence"][k] = str(v)
            columns.append(column)

        return columns

    def get_table(self, full_table_name: str) -> sqlalchemy.Table:
        """Return a table object.
        Args:
            full_table_name: Fully qualified table name.
        Returns:
            A table object with column list.
        """
        columns = self.get_table_columns(full_table_name).values()
        _, schema_name, table_name = self.parse_full_table_name(
            full_table_name
        )
        meta = sqlalchemy.MetaData()
        return sqlalchemy.schema.Table(
            table_name,
            meta,
            *list(columns),
            schema=schema_name,
        )

    def get_table_columns(
        self, full_table_name: str
    ) -> Dict[str, sqlalchemy.Column]:
        """Return a list of table columns.
        Args:
            full_table_name: Fully qualified table name.
        Returns:
            An ordered list of column objects.
        """
        _, schema_name, table_name = self.parse_full_table_name(
            full_table_name
        )

        columns = self.get_columns(self.connection, table_name, schema_name)

        result: Dict[str, sqlalchemy.Column] = {}
        for col_meta in columns:
            result[col_meta["name"]] = sqlalchemy.Column(
                col_meta["name"],
                self.to_sql_type(col_meta["type"]),
                nullable=col_meta.get("nullable", False),
            )

        return result

    def discover_catalog_entries(self) -> List[dict]:
        """Return a list of catalog entries from discovery.
        Returns:
            The discovered catalog entries as a list.
        """
        result: List[dict] = []
        engine = self.create_sqlalchemy_engine()
        inspected = sqlalchemy.inspect(engine)
        for schema_name in inspected.get_schema_names():
            # Get list of tables and views
            table_names = inspected.get_table_names(schema=schema_name)
            try:
                view_names = inspected.get_view_names(schema=schema_name)
            except NotImplementedError:
                # Some DB providers do not understand 'views'
                self._warn_no_view_detection()
                view_names = []
            object_names = [(t, False) for t in table_names] + [
                (v, True) for v in view_names
            ]

            # Iterate through each table and view
            for table_name, is_view in object_names:
                # Initialize unique stream name
                unique_stream_id = self.get_fully_qualified_name(
                    db_name=None,
                    schema_name=schema_name,
                    table_name=table_name,
                    delimiter="-",
                )

                # Detect key properties
                possible_primary_keys: List[List[str]] = []
                pk_def = self.get_pk_constraint(
                    self.connection,
                    table_name,
                    schema=schema_name,
                )
                if pk_def and "constrained_columns" in pk_def:
                    possible_primary_keys.append(pk_def["constrained_columns"])
                # TO-DO: to be implemented
                # for index_def in inspected.get_indexes(
                #     table_name, schema=schema_name
                # ):
                #     if index_def.get("unique", False):
                #         possible_primary_keys.append(index_def["column_names"])
                key_properties = next(iter(possible_primary_keys), None)

                # Initialize available replication methods
                addl_replication_methods: List[str] = ["INCREMENTAL"]
                # By default an empty list.
                # Notes regarding replication methods:
                # - 'INCREMENTAL' replication must be enabled by the user by
                #   specifying a replication_key value.
                # - 'LOG_BASED' replication must be enabled by the developer,
                #   according to source-specific implementation capabilities.

                replication_method = next(
                    reversed(["FULL_TABLE"] + addl_replication_methods)
                )

                # Create the catalog entry object
                schema = self.schema(table_name, schema_name)

                catalog_entry = CatalogEntry(
                    tap_stream_id=unique_stream_id,
                    stream=unique_stream_id,
                    table=table_name,
                    key_properties=key_properties,
                    schema=singer.Schema.from_dict(schema),
                    is_view=is_view,
                    replication_method=replication_method,
                    metadata=MetadataMapping.get_standard_metadata(
                        schema_name=schema_name,
                        schema=schema,
                        replication_method=replication_method,
                        key_properties=key_properties,
                        valid_replication_keys=None,  # Must be defined by user
                    ),
                    database=None,  # Expects single-database context
                    row_count=None,
                    stream_alias=None,
                    replication_key=None,  # Must be defined by user
                )
                result.append(catalog_entry.to_dict())

        return result

    def schema(self, table_name, schema_name) -> dict:
        """Dynamically detect the json schema for the stream.
        This is evaluated prior to any records being retrieved.
        """
        properties: List[Property] = []
        for column_def in self.get_columns(
            self.connection,
            table_name,
            schema=schema_name,
        ):
            column_name = column_def["name"]
            column_type = self.get_jsonschema_type(str(column_def["type"]))
            properties.append(Property(column_name, column_type))

        return PropertiesList(*properties).to_dict()

    @staticmethod
    def to_jsonschema_type(sql_type: sqlalchemy.types.TypeEngine) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_jsonschema_type(sql_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_sql_type(jsonschema_type)


class sybaseStream(SQLStream):
    """Stream class for sybase streams."""

    connector_class = sybaseConnector

    def get_records(self, partition: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Optionally, add custom logic instead of calling the super().
        # This is helpful if the source database provides batch-optimized record
        # retrieval.
        # If no overrides or optimizations are needed, you may delete this method.
        yield from super().get_records(partition)
