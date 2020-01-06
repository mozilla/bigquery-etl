"""Tokenize SQL so that it can be formatted."""

from dataclasses import dataclass, field
import re

# These words get their own line followed by increased indent
TOP_LEVEL_KEYWORDS = [
    # DDL
    "ALTER TABLE IF EXISTS",
    "ALTER TABLE",
    "CREATE OR REPLACE TABLE",
    "CREATE OR REPLACE VIEW",
    "CREATE TABLE IF NOT EXISTS",
    "CREATE VIEW IF NOT EXISTS",
    "CREATE TEMP TABLE",
    "CREATE TABLE",
    "CREATE VIEW",
    "DROP TABLE",
    "DROP VIEW",
    "CLUSTER BY",
    "OPTIONS",
    # DML
    "DELETE FROM",
    "DELETE",
    "INSERT INTO",
    "INSERT",
    "MERGE INTO",
    "MERGE",
    "UPDATE",
    # SQL
    "CROSS JOIN",
    "EXCEPT DISTINCT",
    "INTERSECT DISTINCT",
    "FROM",
    "FULL JOIN",
    "FULL OUTER JOIN",
    "GROUP BY",
    "HAVING",
    "INNER JOIN",
    "INTERSECT",
    "JOIN",
    "LEFT JOIN",
    "LEFT OUTER JOIN",
    "LIMIT",
    "ORDER BY",
    "OUTER JOIN",
    "PARTITION BY",
    "RIGHT JOIN",
    "RIGHT OUTER JOIN",
    "ROLLUP",
    "ROWS BETWEEN",
    "ROWS",
    "SELECT AS STRUCT",
    "SELECT AS VALUE",
    "SELECT",
    "SET",
    "UNION ALL",
    "UNION DISTINCT",
    "UNION",
    "USING",
    "VALUES",
    "WHERE",
    "WINDOW",
    "WITH",
]
# These words start a new line at the current indent
NEWLINE_KEYWORDS = [
    # UDF
    "CREATE",
    "RETURNS",
    "LANGUAGE",
    # Conditional
    "AND",
    "BETWEEN",
    "ELSE",
    "END",
    "OR",
    "WHEN",
    "XOR",
]
# These words, and all get capitalized
RESERVED_KEYWORDS = [
    "ALL",
    "AND",
    "ANY",
    "ARRAY",
    "AS",
    "ASC",
    "ASSERT_ROWS_MODIFIED",
    "AT",
    "BETWEEN",
    "BY",
    "CASE",
    "CAST",
    "COLLATE",
    "CONTAINS",
    "CREATE",
    "CROSS",
    "CUBE",
    "CURRENT",
    "DEFAULT",
    "DEFINE",
    "DESC",
    "DISTINCT",
    "ELSE",
    "END",
    "ENUM",
    "ESCAPE",
    "EXCEPT",
    "EXCLUDE",
    "EXISTS",
    "EXTRACT",
    "FALSE",
    "FETCH",
    "FOLLOWING",
    "FOR",
    "FROM",
    "FULL",
    "GROUP",
    "GROUPING",
    "GROUPS",
    "HASH",
    "HAVING",
    "IFNULL",
    "IF",
    "IGNORE",
    "IN",
    "INNER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS",
    "JOIN",
    "LATERAL",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LOOKUP",
    "MERGE",
    "NATURAL",
    "NEW",
    "NO",
    "NOT",
    "NULL",
    "NULLS",
    "OF",
    "ON",
    "OR",
    "ORDER",
    "OUTER",
    "OVER",
    "PARTITION",
    "PRECEDING",
    "PROTO",
    "RANGE",
    "RECURSIVE",
    "REPLACE",
    "RESPECT",
    "RIGHT",
    "ROLLUP",
    "ROWS",
    "SELECT",
    "SET",
    "SOME",
    "STRUCT",
    "TABLESAMPLE",
    "THEN",
    "TO",
    "TREAT",
    "TRUE",
    "UNBOUNDED",
    "UNION",
    "UNNEST",
    "USING",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
    "WITHIN",
]
QUOTE = "\"\"\"|'''|\"|'"
# strings contain any character, with backslash always followed by one more character
STRING_CONTENT = r"\\.|[^\\]"


def _keyword_pattern(words):
    """
    Compile a regex that will match the given list of keywords.

    Match one or more whitespace characters between words patterns, and only
    match whole words.
    """
    return re.compile(
        "(?:"
        + "|".join(pattern.replace(" ", r"\s+") for pattern in words)
        + ")(?![A-Za-z_0-9])",
        re.IGNORECASE,
    )


@dataclass
class Token:
    """Abstract token class."""

    value: str
    pattern: re.Pattern = field(init=False, repr=False)

    def __post_init__(self):
        """Enable post-init for child classes."""
        pass


class Comment(Token):
    """Comment abstract class.

    Comments must match preceding whitespace, but not match multiple preceding
    lines or trailing whitespace.
    """

    _format_off = re.compile(r"\bformat\s*:?\s*off\b")
    _format_on = re.compile(r"\bformat\s*:?\s*on\b")

    def __post_init__(self):
        self.format_off = self._format_off.search(self.value) is not None
        self.format_on = self._format_on.search(self.value) is not None


class LineComment(Comment):
    """Comment that spans to the end of the current line, sans trailing whitespace."""

    pattern = re.compile(r"\n?[\t ]*(?:#|--)(?:[^\s]|[^\n][^\s])*")


class BlockComment(Comment):
    """Comment that may span multiple lines."""

    pattern = re.compile(r"(:?\n[\t ]*)?/\*.*?\*/", re.DOTALL)


class Whitespace(Token):
    """One or more whitespace characters on a single line."""

    pattern = re.compile(r"\s((?!\n)\s)*")


class ReservedKeyword(Token):
    """Token that gets capitalized and separates words with a single space."""

    pattern = _keyword_pattern(RESERVED_KEYWORDS)


class SpaceBeforeBracketKeyword(ReservedKeyword):
    """Keyword that should be separated by a space from a following opening bracket."""

    pattern = _keyword_pattern(["AS", "IN", "EXCEPT", "REPLACE", "NOT"])


class NewlineKeyword(SpaceBeforeBracketKeyword):
    """Keyword that should start a new line."""

    pattern = _keyword_pattern(NEWLINE_KEYWORDS)


class TopLevelKeyword(NewlineKeyword):
    """Keyword that should get its own line followed by increased indent."""

    pattern = _keyword_pattern(TOP_LEVEL_KEYWORDS)


class AngleBracketKeyword(ReservedKeyword):
    """Keyword indicating that if the next token is '<' it is a bracket."""

    pattern = _keyword_pattern(["ARRAY", "STRUCT"])


class Identifier(Token):
    """Token that identifies a column, parameter, table, or other database object."""

    pattern = re.compile(fr"@?[A-Za-z_][A-Za-z_0-9]*|`(?:{STRING_CONTENT})+?`")


class Literal(Token):
    """A constant value of a built-in data type."""

    pattern = re.compile(
        # String literal
        fr"(?:r?b|b?r)?({QUOTE})(?:{STRING_CONTENT})*?\1"
        # Hexadecimal integer literal
        "|0[xX][0-9a-fA-F]+"
        # Decimal integer or float literal
        r"|\d+\.?\d*(?:[Ee][+-]?)?\d*"
    )


class OpeningBracket(Token):
    """Opening bracket or parenthesis.

    May be followed by no whitespace or a new line and increased indent.
    """

    pattern = re.compile(r"[([]")


class ClosingBracket(Token):
    """Closing bracket or parenthesis.

    May be preceded by a new line and decreased indent, or no whitespace.
    """

    pattern = re.compile(r"[\])]")


class MaybeOpeningAngleBracket(Token):
    """Token that needs context to determine whether it is an operator or bracket."""

    pattern = re.compile(r"<")


class MaybeClosingAngleBracket(Token):
    """Token that needs context to determine whether it is an operator or bracket."""

    pattern = re.compile(r">")


class ExpressionSeparator(Token):
    """Token that separates expressions.

    Should not be preceded by whitespace and may be followed by a space or a
    new line.
    """

    pattern = re.compile(r",")


class StatementSeparator(Token):
    """Token that separates statements.

    Should not be preceded by whitespace and should be followed by a new line
    and reset indent.
    """

    pattern = re.compile(r";")


class Operator(Token):
    """Operator."""

    pattern = re.compile(r"<<|>>|>=|<=|<>|!=|.")


class FieldAccessOperator(Operator):
    """Operator for field access.

    May use whitespace different from other operators.

    May be followed by an identifier that would otherwise be a reserved
    keyword.
    """

    pattern = re.compile(r"\.")


BIGQUERY_TOKEN_PRIORITY = [
    LineComment,
    BlockComment,
    Whitespace,
    TopLevelKeyword,
    NewlineKeyword,
    AngleBracketKeyword,
    SpaceBeforeBracketKeyword,
    ReservedKeyword,
    Literal,
    Identifier,
    OpeningBracket,
    ClosingBracket,
    MaybeOpeningAngleBracket,
    MaybeClosingAngleBracket,
    FieldAccessOperator,
    ExpressionSeparator,
    StatementSeparator,
    Operator,
]


def tokenize(query, token_priority=BIGQUERY_TOKEN_PRIORITY):
    """Split query into a series of tokens."""
    open_angle_brackets = 0
    angle_bracket_is_operator = True
    reserved_keyword_is_identifier = False
    while query:
        for token_type in token_priority:
            match = token_type.pattern.match(query)
            if not match:
                continue
            token = token_type(match.group())
            # handle stateful matches
            if isinstance(token, MaybeOpeningAngleBracket):
                if angle_bracket_is_operator:
                    continue  # prevent matching operator as opening bracket
                token = OpeningBracket(token.value)
                open_angle_brackets += 1
            elif isinstance(token, MaybeClosingAngleBracket):
                if angle_bracket_is_operator:
                    continue  # prevent matching operator as closing bracket
                token = ClosingBracket(token.value)
                open_angle_brackets -= 1
            elif reserved_keyword_is_identifier and isinstance(token, ReservedKeyword):
                continue  # prevent matching identifier as keyword
            yield token
            length = len(token.value)
            query = query[length:]
            # update stateful conditions for next token
            if not isinstance(token, (Comment, Whitespace)):
                # angle brackets are operators unless already in angle bracket
                # block or preceded by an AngleBracketKeyword
                angle_bracket_is_operator = not (
                    open_angle_brackets > 0 or isinstance(token, AngleBracketKeyword)
                )
                # field access operator may be followed by an identifier that
                # would otherwise be a reserved keyword.
                reserved_keyword_is_identifier = isinstance(token, FieldAccessOperator)
            break
        else:
            raise ValueError(f"Could not determine next token in {query!r}")
