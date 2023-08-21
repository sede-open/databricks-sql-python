import re
from alembic.ddl.base import ColumnComment, ColumnType
from sqlalchemy import util, exc
from sqlalchemy.sql import compiler, sqltypes, ColumnElement
from sqlalchemy.sql.schema import Column as DefaultColumn
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.type_api import TypeEngine


# class DatabricksColumn(DefaultColumn):
#     """Represents a column in a databricks table."""
#     def __init__(self, *args, liquid_cluster=False, **kwargs):
#         self.liquid_cluster = liquid_cluster
#         super().__init__(*args, **kwargs)


class DatabricksIdentifierPreparer(compiler.IdentifierPreparer):
    # SparkSQL identifier specification:
    # ref: https://spark.apache.org/docs/latest/sql-ref-identifier.html

    legal_characters = re.compile(r"^[A-Z0-9_]+$", re.I)

    def __init__(self, dialect):
        super().__init__(dialect, initial_quote="`")


class DatabricksDDLCompiler(compiler.DDLCompiler):
    def post_create_table(self, table):
        return " USING DELTA"

    # def get_column_specification(self, column: DatabricksColumn, **kwargs):
    #     colspec = (
    #         self.preparer.format_column(column)
    #         + " "
    #         + self.dialect.type_compiler.process(
    #             column.type, type_expression=column
    #         )
    #     )
    #
    #     default = self.get_column_default_string(column)
    #     if default is not None:
    #         colspec += " DEFAULT " + default
    #
    #     if column.computed is not None:
    #         colspec += " " + self.process(column.computed)
    #
    #     if (
    #         column.identity is not None
    #         and self.dialect.supports_identity_columns
    #     ):
    #         colspec += " " + self.process(column.identity)
    #
    #     if not column.nullable and (
    #         not column.identity or not self.dialect.supports_identity_columns
    #     ):
    #         colspec += " NOT NULL"
    #     return colspec

    def visit_set_column_comment(self, create, **kw):
        """
        Example syntax for adding column comment:
        "ALTER TABLE schema.table_name CHANGE COLUMN COLUMN_NAME COMMENT 'Comment to be added to column';"

        """
        return """ALTER TABLE {0} CHANGE COLUMN {1} COMMENT {2}""".format(
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

    def visit_create_table(self, create, **kw):
        table = create.element
        preparer = self.preparer

        text = "\nCREATE "
        if table._prefixes:
            text += " ".join(table._prefixes) + " "

        # Default to 'IF NOT EXISTS'
        text += "TABLE IF NOT EXISTS "

        text += preparer.format_table(table) + " "

        create_table_suffix = self.create_table_suffix(table)
        if create_table_suffix:
            text += create_table_suffix + " "

        text += "("

        separator = "\n"

        # if only one primary key, specify it along with the column
        first_pk = False
        liquid_clustering = False
        liquid_cluster_columns = []
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(
                    create_column, first_pk=column.primary_key and not first_pk
                )
                if processed is not None:
                    text += separator
                    separator = ", \n"
                    text += "\t" + processed
                if column.primary_key:
                    first_pk = True
            except exc.CompileError as ce:
                util.raise_(
                    exc.CompileError(
                        util.u(f"(in table '{table.description}', column '{column.name}'): {ce.args[0]}")
                    ),
                    from_=ce,
                )

            # Check and apply liquid clustering
            if 'databricks' in column.dialect_options:
                try:
                    cluster_on = column.dialect_options['databricks'].__getitem__('cluster_key')
                    if cluster_on:
                        liquid_clustering = True
                        liquid_cluster_columns.append(column.name)
                except KeyError:
                    pass

        const = self.create_table_constraints(
            table,
            _include_foreign_key_constraints=create.include_foreign_key_constraints,  # noqa
        )
        if const:
            text += separator + "\t" + const

        text += f"\n){self.post_create_table(table)}\n"

        if liquid_clustering:
            text += f"{self.liquid_cluster_on_table(liquid_cluster_columns)}\n\n"

        return text

    def liquid_cluster_on_table(self, liquid_cluster_columns):
        columns = liquid_cluster_columns

        return """CLUSTER BY ({cols})""".format(cols=', '.join(columns))

    def visit_drop_table(self, drop, **kw):
        text = "\nDROP TABLE IF EXISTS "

        return text + self.preparer.format_table(drop.element)

    # def visit_create_column(self, create, first_pk=False, **kw):
    #     column = create.element

    #     if column.system:
    #         return None

    #     text = self.get_column_specification(column, first_pk=first_pk)
    #     const = " ".join(
    #         self.process(constraint) for constraint in column.constraints
    #     )
    #     if const:
    #         text += " " + const

    #     # Code to deal with NOT NULL being unsupported in ADD COLUMNS clause
    #     if "NOT NULL" in text:
    #         text.replace("NOT NULL", "")
    #         text += """;
    #         ALTER TABLE {0} ALTER COLUMN {1} SET NOT NULL;
    #         """.format(
    #             self._format_table_from_column(
    #                 create, use_schema=True
    #             ),
    #             self.preparer.format_column(
    #                 create.element, use_table=False
    #             )
    #         )
    #     return text


@compiles(ColumnComment, "databricks")
def visit_column_comment(
    element: ColumnComment, compiler: DatabricksDDLCompiler, **kw) -> str:
    ddl = "ALTER TABLE `{schema}`.{table_name} ALTER COLUMN {column_name} COMMENT {comment}"
    comment = (
        compiler.sql_compiler.render_literal_value(
            element.comment, sqltypes.String()
        )
        if element.comment is not None
        else "NULL"
    )

    return ddl.format(
        schema=element.schema,
        table_name=element.table_name,
        column_name=element.column_name,
        comment=comment,
    )


# @compiles(ColumnType, "databricks")
# def visit_column_type(element: ColumnType, compiler: DatabricksDDLCompiler, **kw) -> str:
#
#
#     return "%s %s %s" % (
#         alter_table(compiler, element.table_name, element.schema),
#         alter_column(compiler, element.column_name),
#         "TYPE %s" % format_type(compiler, element.type_),
#     )
#
#
# def format_type(compiler: DatabricksDDLCompiler, type_: TypeEngine) -> str:
#     return compiler.dialect.type_compiler.process(type_)
