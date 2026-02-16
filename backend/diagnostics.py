from enum import Enum

class Stage(str, Enum):
    FILE = "file"
    ENCODING = "encoding"
    HEADER = "header"
    SCHEMA = "schema"
    ROW = "row"
    COPY = "copy"
    DB = "db"
    SEARCH = "search"
    DELETE = "delete"


class DiagnosticError(Exception):
    def __init__(
        self,
        stage: Stage,
        message: str,
        *,
        row: int | None = None,
        column: str | None = None,
        hint: str | None = None,
    ):
        self.stage = stage
        self.message = message
        self.row = row
        self.column = column
        self.hint = hint
