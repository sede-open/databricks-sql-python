import re
from sqlalchemy.sql import compiler, sqltypes, ColumnElement


class DatabricksIdentifierPreparer(compiler.IdentifierPreparer):
    # SparkSQL identifier specification:
    # ref: https://spark.apache.org/docs/latest/sql-ref-identifier.html

    legal_characters = re.compile(r"^[A-Z0-9_]+$", re.I)

    def __init__(self, dialect):
        super().__init__(dialect, initial_quote="`")


class DatabricksDDLCompiler(compiler.DDLCompiler):
    def post_create_table(self, table):
        return " USING DELTA"

    def visit_set_column_comment(self, create, **kw):
        """
        Example syntax for adding column comment:
        "ALTER TABLE schema.table_name CHANGE COLUMN COLUMN_NAME COMMENT 'Comment to be added to column';"

        """
        return """ALTER TABLE {0} CHANGE COLUMN {1} COMMENT {2}""".format(
            'testing.alembic_test1',
            self._format_table_from_column(
                 create, use_schema=True
            ),
            self.preparer.format_column(
                create.element, use_table=False
            ),
            self.sql_compiler.render_literal_value(
                create.element.comment, sqltypes.String()
            ),
        )

    def visit_drop_column_comment(self, drop, **kw):
        """
        Example syntax for dropping column comment:
        "ALTER TABLE schema.table_name CHANGE COLUMN COLUMN_NAME COMMENT '';"

        Note: There is no syntactical 'DROP' statement in this case, the comment must be replaced with an empty string
        """
        return "ALTER TABLE {0} CHANGE COLUMN {1} COMMENT '';".format(
            self._format_table_from_column(
                 drop, use_schema=True
            ),
            self.preparer.format_column(
                drop.element, use_table=False
            )
        )

    def _format_table_from_column(self, column_object, use_schema=False):
        """
        Prepare a quoted table name from the column object (including schema if specified)
        """
        schema_table_column = self.preparer.format_column(
                column_object.element, use_table=True, use_schema=True
            )

        name = schema_table_column.split(".")[1]

        if use_schema:
            name = schema_table_column.split(".")[0] + '.' + name

        return name
