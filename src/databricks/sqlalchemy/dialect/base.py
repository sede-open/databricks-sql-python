import re
from alembic.ddl.base import ColumnComment, ColumnType
from sqlalchemy import util, exc
from sqlalchemy.sql import compiler, sqltypes, ColumnElement
from sqlalchemy.sql.schema import Column as DefaultColumn
from sqlalchemy.sql.schema import ColumnDefault, Sequence, DefaultClause, FetchedValue
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.type_api import TypeEngine
from sqlalchemy.sql.base import SchemaEventTarget
from sqlalchemy.sql.elements import quoted_name


class Column(DefaultColumn):
    """Represents a column in a databricks table."""

    __visit_name__ = "column"

    inherit_cache = True

    def __init__(self, *args, **kwargs):
        name = kwargs.pop("name", None)
        type_ = kwargs.pop("type_", None)
        args = list(args)
        if args:
            if isinstance(args[0], util.string_types):
                if name is not None:
                    raise exc.ArgumentError(
                        "May not pass name positionally and as a keyword."
                    )
                name = args.pop(0)
        if args:
            coltype = args[0]

            if hasattr(coltype, "_sqla_type"):
                if type_ is not None:
                    raise exc.ArgumentError(
                        "May not pass type_ positionally and as a keyword."
                    )
                type_ = args.pop(0)

        if name is not None:
            name = quoted_name(name, kwargs.pop("quote", None))
        elif "quote" in kwargs:
            raise exc.ArgumentError(
                "Explicit 'name' is required when " "sending 'quote' argument"
            )

        super(Column, self).__init__(name, type_)
        self.key = kwargs.pop("key", name)
        self.primary_key = primary_key = kwargs.pop("primary_key", False)

        self._user_defined_nullable = udn = kwargs.pop(
            "nullable", NULL_UNSPECIFIED
        )

        if udn is not NULL_UNSPECIFIED:
            self.nullable = udn
        else:
            self.nullable = not primary_key

        self.default = kwargs.pop("default", None)
        self.server_default = kwargs.pop("server_default", None)
        self.server_onupdate = kwargs.pop("server_onupdate", None)

        # these default to None because .index and .unique is *not*
        # an informational flag about Column - there can still be an
        # Index or UniqueConstraint referring to this Column.
        self.index = kwargs.pop("index", None)
        self.unique = kwargs.pop("unique", None)

        self.system = kwargs.pop("system", False)
        self.doc = kwargs.pop("doc", None)
        self.onupdate = kwargs.pop("onupdate", None)
        self.autoincrement = kwargs.pop("autoincrement", "auto")
        self.constraints = set()
        self.foreign_keys = set()
        self.comment = kwargs.pop("comment", None)
        self.computed = None
        self.identity = None
        self.liquid_cluster = kwargs.pop("liquid_cluster", None)

        # check if this Column is proxying another column
        if "_proxies" in kwargs:
            self._proxies = kwargs.pop("_proxies")
        # otherwise, add DDL-related events
        elif isinstance(self.type, SchemaEventTarget):
            self.type._set_parent_with_dispatch(self)

        if self.default is not None:
            if isinstance(self.default, (ColumnDefault, Sequence)):
                args.append(self.default)
            else:
                if getattr(self.type, "_warn_on_bytestring", False):
                    if isinstance(self.default, util.binary_type):
                        util.warn(
                            "Unicode column '%s' has non-unicode "
                            "default value %r specified."
                            % (self.key, self.default)
                        )
                args.append(ColumnDefault(self.default))

        if self.server_default is not None:
            if isinstance(self.server_default, FetchedValue):
                args.append(self.server_default._as_for_update(False))
            else:
                args.append(DefaultClause(self.server_default))

        if self.onupdate is not None:
            if isinstance(self.onupdate, (ColumnDefault, Sequence)):
                args.append(self.onupdate)
            else:
                args.append(ColumnDefault(self.onupdate, for_update=True))

        if self.server_onupdate is not None:
            if isinstance(self.server_onupdate, FetchedValue):
                args.append(self.server_onupdate._as_for_update(True))
            else:
                args.append(
                    DefaultClause(self.server_onupdate, for_update=True)
                )
        self._init_items(*args)

        util.set_creation_order(self)

        if "info" in kwargs:
            self.info = kwargs.pop("info")

        self._extra_kwargs(**kwargs)

    foreign_keys = None
    """A collection of all :class:`_schema.ForeignKey` marker objects
       associated with this :class:`_schema.Column`.

       Each object is a member of a :class:`_schema.Table`-wide
       :class:`_schema.ForeignKeyConstraint`.

       .. seealso::

           :attr:`_schema.Table.foreign_keys`

    """

    index = None
    """The value of the :paramref:`_schema.Column.index` parameter.

       Does not indicate if this :class:`_schema.Column` is actually indexed
       or not; use :attr:`_schema.Table.indexes`.

       .. seealso::

           :attr:`_schema.Table.indexes`
    """

    unique = None
    """The value of the :paramref:`_schema.Column.unique` parameter.

       Does not indicate if this :class:`_schema.Column` is actually subject to
       a unique constraint or not; use :attr:`_schema.Table.indexes` and
       :attr:`_schema.Table.constraints`.

       .. seealso::

           :attr:`_schema.Table.indexes`

           :attr:`_schema.Table.constraints`.

    """

class DatabricksIdentifierPreparer(compiler.IdentifierPreparer):
    # SparkSQL identifier specification:
    # ref: https://spark.apache.org/docs/latest/sql-ref-identifier.html

    legal_characters = re.compile(r"^[A-Z0-9_]+$", re.I)

    def __init__(self, dialect):
        super().__init__(dialect, initial_quote="`")


class DatabricksDDLCompiler(compiler.DDLCompiler):
    def post_create_table(self, table):
        return " USING DELTA"

    def get_column_specification(self, column: Column, **kwargs):    # TODO: replace column with column: Column ?
        colspec = (
            self.preparer.format_column(column)
            + " "
            + self.dialect.type_compiler.process(
                column.type, type_expression=column
            )
        )

        # TODO: debugging line
        print(colspec)

        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default

        if column.computed is not None:
            colspec += " " + self.process(column.computed)

        if (
            column.identity is not None
            and self.dialect.supports_identity_columns
        ):
            colspec += " " + self.process(column.identity)

        if not column.nullable and (
            not column.identity or not self.dialect.supports_identity_columns
        ):
            colspec += " NOT NULL"
        return colspec

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

            #print(type(create_column))
            # print(create_column)
            # print(type(column))
            #print(column)
            # print(dir(column))
            print(column.__dict__)

            # Check column.kwargs

            # if column.liquid_cluster is not None:
            #     liquid_cluster = column.liquid_cluster
            #     if liquid_cluster:
            #         liquid_clustering = True
            #         liquid_cluster_columns.append(column.name)

        const = self.create_table_constraints(
            table,
            _include_foreign_key_constraints=create.include_foreign_key_constraints,  # noqa
        )
        if const:
            text += separator + "\t" + const

        text += f"\n){self.post_create_table(table)}\n\n"

        if liquid_clustering:
            text += f"\n{self.liquid_cluster_on_table(liquid_cluster_columns)}\n\n"

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
