"""Tokenize SQL so that it can be formatted."""

import re
import sys
from dataclasses import dataclass, field
from typing import Iterator

# These words get their own line followed by increased indent
TOP_LEVEL_KEYWORDS = [
    # DDL
    "ALTER TABLE IF EXISTS",
    "ALTER TABLE",
    "CLUSTER BY",
    "CREATE OR REPLACE TABLE",
    "CREATE OR REPLACE VIEW",
    "CREATE TABLE IF NOT EXISTS",
    "CREATE VIEW IF NOT EXISTS",
    "CREATE TEMP TABLE",
    "CREATE TABLE",
    "CREATE VIEW",
    "DROP TABLE",
    "DROP VIEW",
    "OPTIONS",
    # DML
    "DELETE FROM",
    "DELETE",
    "INSERT INTO",
    "INSERT",
    "MERGE INTO",
    "MERGE",
    "UPDATE",
    # scripting
    "BEGIN TRANSACTION",
    "BREAK",
    "COMMIT TRANSACTION",
    "COMMIT",
    "CONTINUE",
    "ITERATE",
    "LEAVE",
    "ROLLBACK TRANSACTION",
    "ROLLBACK",
    # SQL
    "AS",  # only when not identified as an AliasSeparator
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
    "QUALIFY",
    "RANGE BETWEEN",
    "RANGE",
    "RIGHT JOIN",
    "RIGHT OUTER JOIN",
    "ROLLUP",
    "ROWS BETWEEN",
    "ROWS",
    "SELECT AS STRUCT",
    "SELECT AS VALUE",
    "SELECT DISTINCT AS STRUCT",
    "SELECT DISTINCT AS VALUE",
    "SELECT DISTINCT",
    "SELECT",
    "UNION ALL",
    "UNION DISTINCT",
    "UNION",
    "VALUES",
    "WHEN MATCHED",
    "WHEN NOT MATCHED BY SOURCE",
    "WHEN NOT MATCHED BY TARGET",
    "WHEN NOT MATCHED",
    "WHERE",
    "WITH(?! OFFSET)",
    "WINDOW",
]
# These words start a new line at the current indent
NEWLINE_KEYWORDS = [
    "ON",
    "USING",
    "WITH OFFSET",
    # UDF
    "CREATE OR REPLACE",
    "CREATE",
    "RETURNS",
    "LANGUAGE",
    # Conditional
    "AND",
    "BETWEEN",
    "ELSE",
    "ELSEIF",
    "END",
    "OR",
    "WHEN",
    "XOR",
]
# These words get capitalized
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
    "DECLARE",
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
    "IF",
    "IGNORE",
    "IN",
    "INNER",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "IS DISTINCT FROM",
    "IS NOT DISTINCT FROM",
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
    "RAISE USING MESSAGE",
    "RAISE",
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
# These built-in function names get capitalized
BUILTIN_FUNCTIONS = [
    "ABS",
    "ACOS",
    "ACOSH",
    "AEAD.DECRYPT_BYTES",
    "AEAD.DECRYPT_STRING",
    "AEAD.ENCRYPT",
    "ANY_VALUE",
    "APPROX_COUNT_DISTINCT",
    "APPROX_QUANTILES",
    "APPROX_TOP_COUNT",
    "APPROX_TOP_SUM",
    "ARRAY",
    "ARRAY_AGG",
    "ARRAY_CONCAT",
    "ARRAY_CONCAT_AGG",
    "ARRAY_LENGTH",
    "ARRAY_REVERSE",
    "ARRAY_TO_STRING",
    "ASCII",
    "ASIN",
    "ASINH",
    "ATAN",
    "ATAN2",
    "ATANH",
    "AVG",
    "BIT_AND",
    "BIT_COUNT",
    "BIT_OR",
    "BIT_XOR",
    "BOOL",
    "BYTE_LENGTH",
    "CAST",
    "CBRT",
    "CEIL",
    "CEILING",
    "CHAR_LENGTH",
    "CHARACTER_LENGTH",
    "CHR",
    "COALESCE",
    "CODE_POINTS_TO_BYTES",
    "CODE_POINTS_TO_STRING",
    "COLLATE",
    "CONCAT",
    "CONTAINS_SUBSTR",
    "CORR",
    "COS",
    "COSH",
    "COT",
    "COTH",
    "COUNT",
    "COUNTIF",
    "COVAR_POP",
    "COVAR_SAMP",
    "CSC",
    "CSCH",
    "CUME_DIST",
    "CURRENT_DATE",
    "CURRENT_DATETIME",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "DATE",
    "DATE_ADD",
    "DATE_DIFF",
    "DATE_FROM_UNIX_DATE",
    "DATE_SUB",
    "DATE_TRUNC",
    "DATETIME",
    "DATETIME_ADD",
    "DATETIME_DIFF",
    "DATETIME_SUB",
    "DATETIME_TRUNC",
    "DENSE_RANK",
    "DETERMINISTIC_DECRYPT_BYTES",
    "DETERMINISTIC_DECRYPT_STRING",
    "DETERMINISTIC_ENCRYPT",
    "DIV",
    "ENDS_WITH",
    "ERROR",
    "EXP",
    "EXTERNAL_OBJECT_TRANSFORM",
    "EXTRACT",
    "FARM_FINGERPRINT",
    "FIRST_VALUE",
    "FLOAT64",
    "FLOOR",
    "FORMAT",
    "FORMAT_DATE",
    "FORMAT_DATETIME",
    "FORMAT_TIME",
    "FORMAT_TIMESTAMP",
    "FROM_BASE32",
    "FROM_BASE64",
    "FROM_HEX",
    "GENERATE_ARRAY",
    "GENERATE_DATE_ARRAY",
    "GENERATE_TIMESTAMP_ARRAY",
    "GENERATE_UUID",
    "GREATEST",
    "HLL_COUNT.EXTRACT",
    "HLL_COUNT.INIT",
    "HLL_COUNT.MERGE",
    "HLL_COUNT.MERGE_PARTIAL",
    "IEEE_DIVIDE",
    "IF",
    "IFNULL",
    "INITCAP",
    "INSTR",
    "INT64",
    "IS_INF",
    "IS_NAN",
    "JSON_EXTRACT",
    "JSON_EXTRACT_ARRAY",
    "JSON_EXTRACT_SCALAR",
    "JSON_EXTRACT_STRING_ARRAY",
    "JSON_QUERY",
    "JSON_QUERY_ARRAY",
    "JSON_TYPE",
    "JSON_VALUE",
    "JSON_VALUE_ARRAY",
    "JUSTIFY_DAYS",
    "JUSTIFY_HOURS",
    "JUSTIFY_INTERVAL",
    "KEYS.ADD_KEY_FROM_RAW_BYTES",
    "KEYS.KEYSET_CHAIN",
    "KEYS.KEYSET_FROM_JSON",
    "KEYS.KEYSET_LENGTH",
    "KEYS.KEYSET_TO_JSON",
    "KEYS.NEW_KEYSET",
    "KEYS.NEW_WRAPPED_KEYSET",
    "KEYS.REWRAP_KEYSET",
    "KEYS.ROTATE_KEYSET",
    "KEYS.ROTATE_WRAPPED_KEYSET",
    "LAG",
    "LAST_DAY",
    "LAST_VALUE",
    "LEAD",
    "LEAST",
    "LEFT",
    "LENGTH",
    "LN",
    "LOG",
    "LOG10",
    "LOGICAL_AND",
    "LOGICAL_OR",
    "LOWER",
    "LPAD",
    "LTRIM",
    "MAKE_INTERVAL",
    "MAX",
    "MD5",
    "MIN",
    "MOD",
    "NET.HOST",
    "NET.IP_FROM_STRING",
    "NET.IP_NET_MASK",
    "NET.IP_TO_STRING",
    "NET.IP_TRUNC",
    "NET.IPV4_FROM_INT64",
    "NET.IPV4_TO_INT64",
    "NET.PUBLIC_SUFFIX",
    "NET.REG_DOMAIN",
    "NET.SAFE_IP_FROM_STRING",
    "NORMALIZE",
    "NORMALIZE_AND_CASEFOLD",
    "NTH_VALUE",
    "NTILE",
    "NULLIF",
    "OCTET_LENGTH",
    "OFFSET",
    "ORDINAL",
    "PARSE_BIGNUMERIC",
    "PARSE_DATE",
    "PARSE_DATETIME",
    "PARSE_JSON",
    "PARSE_NUMERIC",
    "PARSE_TIME",
    "PARSE_TIMESTAMP",
    "PERCENT_RANK",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    "POW",
    "POWER",
    "RAND",
    "RANGE_BUCKET",
    "RANK",
    "REGEXP_CONTAINS",
    "REGEXP_EXTRACT",
    "REGEXP_EXTRACT_ALL",
    "REGEXP_INSTR",
    "REGEXP_REPLACE",
    "REGEXP_SUBSTR",
    "REPEAT",
    "REPLACE",
    "REVERSE",
    "RIGHT",
    "ROUND",
    "ROW_NUMBER",
    "RPAD",
    "RTRIM",
    "S2_CELLIDFROMPOINT",
    "S2_COVERINGCELLIDS",
    "SAFE_ADD",
    "SAFE_CAST",
    "SAFE_CONVERT_BYTES_TO_STRING",
    "SAFE_DIVIDE",
    "SAFE_MULTIPLY",
    "SAFE_NEGATE",
    "SAFE_OFFSET",
    "SAFE_ORDINAL",
    "SAFE_SUBTRACT",
    "SEC",
    "SECH",
    "SESSION_USER",
    "SHA1",
    "SHA256",
    "SHA512",
    "SIGN",
    "SIN",
    "SINH",
    "SOUNDEX",
    "SPLIT",
    "SQRT",
    "ST_ANGLE",
    "ST_AREA",
    "ST_ASBINARY",
    "ST_ASGEOJSON",
    "ST_ASTEXT",
    "ST_AZIMUTH",
    "ST_BOUNDARY",
    "ST_BOUNDINGBOX",
    "ST_BUFFER",
    "ST_BUFFERWITHTOLERANCE",
    "ST_CENTROID",
    "ST_CENTROID_AGG",
    "ST_CLOSESTPOINT",
    "ST_CLUSTERDBSCAN",
    "ST_CONTAINS",
    "ST_CONVEXHULL",
    "ST_COVEREDBY",
    "ST_COVERS",
    "ST_DIFFERENCE",
    "ST_DIMENSION",
    "ST_DISJOINT",
    "ST_DISTANCE",
    "ST_DUMP",
    "ST_DWITHIN",
    "ST_ENDPOINT",
    "ST_EQUALS",
    "ST_EXTENT",
    "ST_EXTERIORRING",
    "ST_GEOGFROM",
    "ST_GEOGFROMGEOJSON",
    "ST_GEOGFROMTEXT",
    "ST_GEOGFROMWKB",
    "ST_GEOGPOINT",
    "ST_GEOGPOINTFROMGEOHASH",
    "ST_GEOHASH",
    "ST_GEOMETRYTYPE",
    "ST_INTERIORRINGS",
    "ST_INTERSECTION",
    "ST_INTERSECTS",
    "ST_INTERSECTSBOX",
    "ST_ISCLOSED",
    "ST_ISCOLLECTION",
    "ST_ISEMPTY",
    "ST_ISRING",
    "ST_LENGTH",
    "ST_MAKELINE",
    "ST_MAKEPOLYGON",
    "ST_MAKEPOLYGONORIENTED",
    "ST_MAXDISTANCE",
    "ST_NPOINTS",
    "ST_NUMGEOMETRIES",
    "ST_NUMPOINTS",
    "ST_PERIMETER",
    "ST_POINTN",
    "ST_SIMPLIFY",
    "ST_SNAPTOGRID",
    "ST_STARTPOINT",
    "ST_TOUCHES",
    "ST_UNION",
    "ST_UNION_AGG",
    "ST_WITHIN",
    "ST_X",
    "ST_Y",
    "STARTS_WITH",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "STRING",
    "STRING_AGG",
    "STRPOS",
    "SUBSTR",
    "SUBSTRING",
    "SUM",
    "TAN",
    "TANH",
    "TIME",
    "TIME_ADD",
    "TIME_DIFF",
    "TIME_SUB",
    "TIME_TRUNC",
    "TIMESTAMP",
    "TIMESTAMP_ADD",
    "TIMESTAMP_DIFF",
    "TIMESTAMP_MICROS",
    "TIMESTAMP_MILLIS",
    "TIMESTAMP_SECONDS",
    "TIMESTAMP_SUB",
    "TIMESTAMP_TRUNC",
    "TO_BASE32",
    "TO_BASE64",
    "TO_CODE_POINTS",
    "TO_HEX",
    "TO_JSON",
    "TO_JSON_STRING",
    "TRANSLATE",
    "TRIM",
    "TRUNC",
    "UNICODE",
    "UNIX_DATE",
    "UNIX_MICROS",
    "UNIX_MILLIS",
    "UNIX_SECONDS",
    "UPPER",
    "VAR_POP",
    "VAR_SAMP",
    "VARIANCE",
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
        "(?:" + "|".join(pattern.replace(" ", r"\s+") for pattern in words) + r")\b",
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
        """Detect format off/on comments."""
        self.format_off = self._format_off.search(self.value) is not None
        self.format_on = self._format_on.search(self.value) is not None


class LineComment(Comment):
    """Comment that spans to the end of the current line, sans trailing whitespace."""

    pattern = re.compile(r"\n?[^\S\n]*(#|--)([^\n]*\S)?")


class BlockComment(Comment):
    """Comment that may span multiple lines."""

    pattern = re.compile(r"\n?[^\S\n]*/\*.*?\*/", re.DOTALL)


class Whitespace(Token):
    """One or more whitespace characters on a single line."""

    pattern = re.compile(r"\s[^\S\n]*")


class ReservedKeyword(Token):
    """Token that gets capitalized and separates words with a single space."""

    pattern = _keyword_pattern(RESERVED_KEYWORDS)


class SpaceBeforeBracketKeyword(ReservedKeyword):
    """Keyword that should be separated by a space from a following opening bracket."""

    pattern = _keyword_pattern(
        [
            "IN",
            r"\* EXCEPT",
            r"\* REPLACE",
            "NOT",
            "OVER",
            "IS DISTINCT FROM",
            "IS NOT DISTINCT FROM",
        ]
    )


class BlockKeyword(ReservedKeyword):
    """Keyword that separates indented blocks, such as conditionals."""


class BlockStartKeyword(BlockKeyword):
    """Keyword that gets its own line followed by increased indent."""

    pattern = _keyword_pattern(
        [
            "CREATE OR REPLACE PROCEDURE",
            "CREATE PROCEDURE IF NOT EXISTS",
            "CREATE PROCEDURE",
            # negative lookahead prevents matching IF function
            r"IF(?!(\s|\n)*[(])",
            "WHILE",
            "LOOP",
            "CASE",
        ]
    )


class BlockEndKeyword(BlockKeyword):
    """Keyword that gets its own line preceded by decreased indent."""

    pattern = _keyword_pattern(["END( (WHILE|LOOP|IF))?"])


class BlockMiddleKeyword(BlockStartKeyword, BlockEndKeyword):
    """Keyword that ends one indented block and starts another."""

    pattern = _keyword_pattern(
        [r"BEGIN(?!\s+TRANSACTION\b)", "EXCEPTION WHEN ERROR THEN", "ELSEIF", "DO"]
    )


class AliasSeparator(SpaceBeforeBracketKeyword):
    """Keyword separating an expression from an alias.

    May be followed by an alias identifier that would otherwise be a reserved keyword.

    Must not be followed by the keyword WITH, SELECT, STRUCT or ARRAY.
    """

    pattern = re.compile(
        r"AS(?=\s+(?!(WITH|SELECT|STRUCT|ARRAY)\b)[a-z_`({])", re.IGNORECASE
    )


class NewlineKeyword(SpaceBeforeBracketKeyword):
    """Keyword that should start a new line."""

    pattern = _keyword_pattern(NEWLINE_KEYWORDS)


class TopLevelKeyword(NewlineKeyword):
    """Keyword that should get its own line followed by increased indent."""

    pattern = _keyword_pattern(TOP_LEVEL_KEYWORDS)


class CaseSubclause(NewlineKeyword):
    """Subclause within a CASE."""

    pattern = _keyword_pattern(["WHEN"])


class MaybeCaseSubclause(ReservedKeyword):
    """Keyword that needs context to determine whether it is for a CASE or an IF."""

    pattern = _keyword_pattern(["THEN", "ELSE"])


class AngleBracketKeyword(ReservedKeyword):
    """Keyword indicating that if the next token is '<' it is a bracket."""

    pattern = _keyword_pattern(["ARRAY", "STRUCT"])


class Identifier(Token):
    """Identifier for a column, table, or other database object."""

    pattern = re.compile(r"[A-Za-z_][A-Za-z_0-9]*|`(?:\\.|[^\\`])+`")


class QualifiedIdentifier(Identifier):
    """Fully or partially qualified identifier for a column, table, or other database object."""

    pattern = re.compile(
        rf"(?:(?:{Identifier.pattern.pattern})\.)+(?:{Identifier.pattern.pattern})"
    )


class BuiltInFunctionIdentifier(Identifier):
    """Identifier for a built-in function."""

    pattern = re.compile(
        r"(?:SAFE\.)?(?:"
        + "|".join(re.escape(f) for f in BUILTIN_FUNCTIONS)
        + r")(?=\()",
        re.IGNORECASE,
    )


class QueryParameter(Identifier):
    """Query parameter."""

    pattern = re.compile("@[A-Za-z_][A-Za-z_0-9]*")


class Literal(Token):
    """A constant value of a built-in data type."""

    pattern = re.compile(
        # String literal
        rf"(?:r?b|b?r)?({QUOTE})(?:{STRING_CONTENT})*?\1"
        # Hexadecimal integer literal
        "|0[xX][0-9a-fA-F]+"
        # Decimal integer or float literal
        r"|\d+\.?\d*(?:[Ee][+-]?)?\d*"
    )


class JinjaExpression(Token):
    """Jinja expression delimiters {{ }}."""

    pattern = re.compile(r"{{.*?}}", re.DOTALL)


class JinjaStatement(Token):
    """Jinja statement delimiters {% %}.

    May be followed by no whitespace or a new line and increased indent.
    """

    pattern = re.compile(r"{%.*?%}", re.DOTALL)


class JinjaBlockStatement(JinjaStatement):
    """Statements that start and/or end Jinja blocks."""


class JinjaBlockStart(JinjaBlockStatement):
    """Jinja block starts get their own line followed by increased indent."""

    pattern = re.compile(r"{%[-+]? *(block|call|filter|for|if|macro)\b.*?%}", re.DOTALL)


class JinjaBlockEnd(JinjaBlockStatement):
    """Jinja block ends get their own line preceded by decreased indent."""

    pattern = re.compile(
        r"{%[-+]? *end(block|call|filter|for|if|macro)\b.*?%}", re.DOTALL
    )


class JinjaBlockMiddle(JinjaBlockEnd, JinjaBlockStart):
    """Ends one indented Jinja block and starts another."""

    pattern = re.compile(r"{%[-+]? *(elif|else)\b.*?%}", re.DOTALL)


class JinjaComment(BlockComment):
    """Jinja comment that may span multiple lines."""

    pattern = re.compile(r"\n?[^\S\n]*{#.*?#}", re.DOTALL)


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

    pattern = re.compile(r"<<|>>|>=|<=|=>|<>|!=|.")


class ConcatenationOperator(Token):
    """Concatenation operator."""

    pattern = re.compile(r"\|\|")


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
    JinjaComment,
    Whitespace,
    JinjaExpression,
    JinjaBlockStart,
    JinjaBlockMiddle,
    JinjaBlockEnd,
    JinjaStatement,
    BlockMiddleKeyword,
    BlockStartKeyword,
    BlockEndKeyword,
    AliasSeparator,
    TopLevelKeyword,
    MaybeCaseSubclause,
    CaseSubclause,
    NewlineKeyword,
    AngleBracketKeyword,
    SpaceBeforeBracketKeyword,
    ReservedKeyword,
    ConcatenationOperator,
    Literal,
    BuiltInFunctionIdentifier,
    QualifiedIdentifier,
    Identifier,
    QueryParameter,
    OpeningBracket,
    ClosingBracket,
    MaybeOpeningAngleBracket,
    MaybeClosingAngleBracket,
    FieldAccessOperator,
    ExpressionSeparator,
    StatementSeparator,
    Operator,
]


def tokenize(query, token_priority=BIGQUERY_TOKEN_PRIORITY) -> Iterator[Token]:
    """Split query into a series of tokens."""
    open_blocks: list[BlockStartKeyword] = []
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
            if isinstance(token, MaybeCaseSubclause):
                if open_blocks and open_blocks[-1].value.upper() == "CASE":
                    token = CaseSubclause(token.value)
                else:
                    token = BlockMiddleKeyword(token.value)
            elif isinstance(token, MaybeOpeningAngleBracket):
                if angle_bracket_is_operator:
                    continue  # prevent matching operator as opening bracket
                token = OpeningBracket(token.value)
                open_angle_brackets += 1
            elif isinstance(token, MaybeClosingAngleBracket):
                if angle_bracket_is_operator:
                    continue  # prevent matching operator as closing bracket
                token = ClosingBracket(token.value)
                open_angle_brackets -= 1
            elif (
                reserved_keyword_is_identifier
                and isinstance(token, ReservedKeyword)
                and Identifier.pattern.match(token.value) is not None
            ):
                continue  # prevent matching identifier as keyword
            yield token
            length = len(token.value)
            query = query[length:]
            # update stateful conditions for next token
            if isinstance(token, BlockEndKeyword) and open_blocks:
                open_blocks.pop()
            if isinstance(token, BlockStartKeyword):
                open_blocks.append(token)
            if not isinstance(token, (Comment, Whitespace)):
                # angle brackets are operators unless already in angle bracket
                # block or preceded by an AngleBracketKeyword
                angle_bracket_is_operator = not (
                    open_angle_brackets > 0 or isinstance(token, AngleBracketKeyword)
                )
                # field access operator may be followed by an identifier that
                # would otherwise be a reserved keyword.
                reserved_keyword_is_identifier = isinstance(
                    token, (FieldAccessOperator, AliasSeparator)
                )
            break
        else:
            raise ValueError(f"Could not determine next token in {query!r}")


if __name__ == "__main__":
    # entrypoint for inspecting tokenize results
    for token in tokenize(sys.stdin.read()):
        print(f"{type(token).__name__}: {token.value!r}")
