"""Format SQL."""

import re
from dataclasses import replace

from .tokenizer import (
    AliasSeparator,
    BlockEndKeyword,
    BlockKeyword,
    BlockStartKeyword,
    BuiltInFunctionIdentifier,
    CaseSubclause,
    ClosingBracket,
    Comment,
    ExpressionSeparator,
    FieldAccessOperator,
    Identifier,
    JinjaBlockEnd,
    JinjaBlockStart,
    JinjaBlockStatement,
    JinjaComment,
    JinjaExpression,
    JinjaStatement,
    LineComment,
    Literal,
    NewlineKeyword,
    OpeningBracket,
    Operator,
    ReservedKeyword,
    SpaceBeforeBracketKeyword,
    StatementSeparator,
    TopLevelKeyword,
    Whitespace,
    tokenize,
)


def simple_format(tokens, indent="  "):
    """Format tokens in a single pass."""
    first_token = True
    require_newline_before_next_token = False
    allow_space_before_next_bracket = False
    allow_space_before_next_token = False
    prev_was_block_end = False
    prev_was_jinja = False
    prev_was_statement_separator = False
    prev_was_unary_operator = False
    next_operator_is_unary = True
    indent_types = []
    can_format = True
    for token in tokens:
        # skip original whitespace tokens, unless formatting is disabled
        if isinstance(token, Whitespace):
            if not can_format:
                yield token
            continue

        # update state for current token
        if isinstance(token, Comment):
            # enable to disable formatting
            if can_format and token.format_off:
                can_format = False
            elif not can_format and token.format_on:
                can_format = True
        elif isinstance(token, ClosingBracket):
            # decrease indent to match last OpeningBracket
            while indent_types and indent_types.pop() is not OpeningBracket:
                pass
        elif isinstance(token, TopLevelKeyword):
            # decrease indent from previous TopLevelKeyword
            if indent_types and indent_types[-1] is TopLevelKeyword:
                indent_types.pop()
        elif isinstance(token, BlockEndKeyword):
            # decrease indent to match last BlockKeyword
            while indent_types and indent_types.pop() is not BlockKeyword:
                pass
            prev_was_statement_separator = False
        elif isinstance(token, JinjaBlockEnd):
            # decrease indent to match last JinjaBlockStart
            while indent_types and indent_types.pop() is not JinjaBlockStart:
                pass
        elif isinstance(token, CaseSubclause):
            if token.value.upper() in ("WHEN", "ELSE"):
                # Have WHEN and ELSE clauses indented one level more than CASE.
                while indent_types and indent_types[-1] is CaseSubclause:
                    indent_types.pop()
        elif isinstance(
            token, (AliasSeparator, ExpressionSeparator, FieldAccessOperator)
        ):
            if prev_was_block_end or prev_was_jinja:
                require_newline_before_next_token = False

        # yield whitespace
        if not can_format or isinstance(token, StatementSeparator) or first_token:
            # except between statements
            # no new whitespace when formatting is disabled
            # no space before statement separator
            # no space before first token
            pass
        elif isinstance(token, Comment):
            # blank line before comments only if they start on their own line
            # and come after a statement separator
            if token.value.startswith("\n") and prev_was_statement_separator:
                yield Whitespace("\n")
        elif (
            require_newline_before_next_token
            or isinstance(
                token,
                (NewlineKeyword, ClosingBracket, BlockKeyword, JinjaBlockStatement),
            )
            or prev_was_statement_separator
        ):
            if prev_was_statement_separator:
                yield Whitespace("\n")
            yield Whitespace("\n" + indent * len(indent_types))
        elif (
            allow_space_before_next_token
            and (
                allow_space_before_next_bracket or not isinstance(token, OpeningBracket)
            )
            and not isinstance(token, (FieldAccessOperator, ExpressionSeparator))
            and not (
                prev_was_unary_operator and isinstance(token, (Literal, Identifier))
            )
        ):
            yield Whitespace(" ")

        if can_format:
            # uppercase keywords and replace contained whitespace with single spaces
            if isinstance(token, ReservedKeyword):
                token = replace(token, value=re.sub(r"\s+", " ", token.value.upper()))
            # uppercase built-in function names
            elif isinstance(token, BuiltInFunctionIdentifier):
                token = replace(token, value=token.value.upper())

        yield token

        # update state for next token
        require_newline_before_next_token = isinstance(
            token,
            (
                Comment,
                BlockKeyword,
                TopLevelKeyword,
                OpeningBracket,
                ExpressionSeparator,
                StatementSeparator,
                JinjaStatement,
            ),
        )
        allow_space_before_next_token = not isinstance(token, FieldAccessOperator)
        prev_was_block_end = isinstance(token, BlockEndKeyword)
        prev_was_statement_separator = isinstance(token, StatementSeparator)
        prev_was_unary_operator = next_operator_is_unary and isinstance(token, Operator)
        prev_was_jinja = isinstance(
            token, (JinjaExpression, JinjaComment, JinjaStatement)
        )
        if not isinstance(token, Comment):
            # format next operator as unary if there is no preceding argument
            next_operator_is_unary = not isinstance(
                token, (Literal, Identifier, ClosingBracket)
            )
        allow_space_before_next_bracket = isinstance(
            token, (SpaceBeforeBracketKeyword, Operator)
        )
        if isinstance(token, TopLevelKeyword) and token.value == "WITH":
            # don't indent CTE's and don't put the first one on a new line
            require_newline_before_next_token = False
        elif isinstance(token, BlockStartKeyword):
            # increase indent
            indent_types.append(BlockKeyword)
        elif isinstance(token, JinjaBlockStart):
            # increase indent
            indent_types.append(JinjaBlockStart)
        elif isinstance(token, (TopLevelKeyword, OpeningBracket, CaseSubclause)):
            # increase indent
            indent_types.append(type(token))
        elif isinstance(token, StatementSeparator):
            # decrease for previous top level keyword
            if indent_types and indent_types[-1] is TopLevelKeyword:
                indent_types.pop()
        first_token = False


class Line:
    """Container for a line of tokens."""

    def __init__(self, indent_token=None, can_format=True):
        """Initialize."""
        self.indent_token = indent_token
        self.can_format = can_format and not isinstance(indent_token, Comment)
        if indent_token is None:
            self.indent_level = 0
        else:
            self.indent_level = len(indent_token.value)
            if indent_token.value.startswith("\n"):
                self.indent_level -= 1
        self.inline_tokens = []
        self.inline_length = 0

    def add(self, token):
        """Add a token to this line."""
        self.inline_length += len(token.value)
        self.inline_tokens.append(token)

    @property
    def tokens(self):
        """Get a list of all the tokens in this line."""
        if self.indent_token is None:
            return self.inline_tokens
        else:
            return [self.indent_token] + self.inline_tokens

    @property
    def can_start_inline_block(self):
        """Determine if this line starts a bracket block that may be inlined.

        This line starts a bracket block if it ends with an OpeningBracket.

        This line can't be inlined if formatting is disabled.

        Blocks preceded by an alias, such as common table expressions and
        window expressions, should not be inlined.
        """
        return (
            self.can_format
            and self.ends_with_opening_bracket
            and not (
                len(self.inline_tokens) > 2
                and isinstance(self.inline_tokens[-3], AliasSeparator)
            )
        )

    @property
    def ends_with_opening_bracket(self):
        """Determine if this line ends with an OpeningBracket."""
        return self.inline_tokens and isinstance(self.inline_tokens[-1], OpeningBracket)

    @property
    def starts_with_closing_bracket(self):
        """Determine if this line starts with a ClosingBracket."""
        return self.inline_tokens and isinstance(self.inline_tokens[0], ClosingBracket)

    @property
    def ends_with_line_comment(self):
        """Determine if this line ends with a line comment."""
        return self.inline_tokens and isinstance(self.inline_tokens[-1], LineComment)


def inline_block_format(tokens, max_line_length=100):
    """Extend simple_format to inline each bracket block if possible.

    A bracket block is a series of tokens from an opening bracket to the
    matching closing bracket. To inline a block means to put it on a single
    line, instead of multiple lines.

    Inline a block if the result would be shorter than max_line_length.

    Do not inline if block if contains a comment.

    For example, this formatter may convert:

        IF(
          condition,
          value_if_true,
          value_if_false,
        )

    to

        IF(condition, value_if_true, value_if_false)

    Implementation requires simple_format to put opening brackets at the end of
    the line and closing brackets at the beginning of the line, unless there is
    a comment between them.
    """
    # format tokens using simple_format, then group into lines
    lines = [Line()]
    can_format = True
    for token in simple_format(tokens):
        if token.value.startswith("\n"):
            lines.append(Line(token, can_format))
        else:
            lines[-1].add(token)
        if isinstance(token, Comment):
            if can_format and token.format_off:
                # disable formatting for current and following lines
                lines[-1].can_format = False
                can_format = False
            elif not can_format and token.format_on:
                # enable formatting for following lines
                can_format = True

    # combine all lines in each bracket block that fits in max_line_length
    skip_lines = 0
    for index, line in enumerate(lines):
        if skip_lines > 0:
            skip_lines -= 1
            continue
        yield from line.tokens
        if line.can_start_inline_block:
            indent_level = line.indent_level
            line_length = indent_level + line.inline_length
            pending_lines = 0
            pending = []
            open_brackets = 1
            index += 1  # start on the next line
            previous_line = line
            for line in lines[index:]:
                if not line.can_format:
                    break
                # Line comments can't be moved into the middle of a line.
                if previous_line.ends_with_line_comment:
                    break
                if (
                    not previous_line.ends_with_opening_bracket
                    and not line.starts_with_closing_bracket
                ):
                    pending.append(Whitespace(" "))
                    line_length += 1
                pending_lines += 1
                pending.extend(line.inline_tokens)
                line_length += line.inline_length
                if line_length > max_line_length:
                    break
                if line.starts_with_closing_bracket:
                    open_brackets -= 1
                if open_brackets == 0:
                    # flush pending and handle next block if present
                    yield from pending
                    skip_lines += pending_lines
                    if line.can_start_inline_block:
                        pending_lines = 0
                        pending = []
                    else:
                        break
                if line.ends_with_opening_bracket:
                    open_brackets += 1
                previous_line = line


def reformat(query, format_=inline_block_format, trailing_newline=False):
    """Reformat query and return as a string."""
    tokens = format_(tokenize(query))
    return "".join(token.value for token in tokens) + ("\n" if trailing_newline else "")
