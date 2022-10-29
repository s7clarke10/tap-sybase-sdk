"""SQL client handling.

This includes sybaseStream and sybaseConnector.
"""
from typing import Iterable, Optional, Dict, Any

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
