from sqlalchemy.sql import compiler


class DatabricksTypeCompiler(compiler.GenericTypeCompiler):
    """Originally forked from pyhive"""

    def visit_INTEGER(self, type_):
        return "INT"

    def visit_NUMERIC(self, type_):
        if type_.precision is None:
            return "DECIMAL"
        elif type_.scale is None:
            return "DECIMAL({precision})".format(precision=type_.precision)
        else:
            return "DECIMAL({precision}, {scale})".format(precision=type_.precision, scale=type_.scale)

    def visit_CHAR(self, type_):
        return "STRING"

    def visit_VARCHAR(self, type_):
        return "STRING"

    def visit_NCHAR(self, type_):
        return "STRING"

    def visit_TEXT(self, type_):
        return "STRING"

    def visit_CLOB(self, type_):
        return "STRING"

    def visit_BLOB(self, type_):
        return "BINARY"

    def visit_TIME(self, type_):
        return "TIMESTAMP"

    def visit_DATE(self, type_):
        return "DATE"

    def visit_DATETIME(self, type_):
        return "TIMESTAMP"

    def visit_JSON(self, type_):
        return "STRUCT"

    def visit_ARRAY(self, type_):
        element_type = type_.item_type
        print(element_type)
        if element_type == "VARCHAR":
            print("String type if statement hit")
            return "ARRAY<STRING>"
        else:
            return "ARRAY<{type}>".format(type=type_.item_type)
