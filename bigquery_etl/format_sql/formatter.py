"""Format SQL."""

from dataclasses import replace
import re

from .tokenizer import (
    ClosingBracket,
    Comment,
    ExpressionSeparator,
    FieldAccessOperator,
    Identifier,
    Literal,
    NewlineKeyword,
    Operator,
    OpeningBracket,
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
    prev_was_unary_operator = False
    next_operator_is_unary = True
    indent_types = []
    for token in tokens:
        # skip original whitespace tokens
        if isinstance(token, Whitespace):
            continue

        # uppercase keywords and replace contained whitespace with single spaces
        if isinstance(token, ReservedKeyword):
            token = replace(token, value=re.sub(r"\s+", " ", token.value.upper()))

        # update state for current token
        if isinstance(token, ClosingBracket):
            # decrease indent to match last OpeningBracket
            while indent_types and indent_types.pop() is not OpeningBracket:
                pass
        elif isinstance(token, TopLevelKeyword):
            # decrease indent to top of current block
            while indent_types and indent_types[-1] is TopLevelKeyword:
                indent_types.pop()

        # yield whitespace
        if first_token or isinstance(token, Comment):
            pass  # no space before first token and comments contain original whitespace
        elif require_newline_before_next_token or isinstance(
            token, (NewlineKeyword, ClosingBracket)
        ):
            yield Whitespace("\n" + indent * len(indent_types))
        elif (
            allow_space_before_next_token
            and (
                allow_space_before_next_bracket or not isinstance(token, OpeningBracket)
            )
            and not isinstance(
                token, (FieldAccessOperator, ExpressionSeparator, StatementSeparator)
            )
            and not (
                prev_was_unary_operator and isinstance(token, (Literal, Identifier))
            )
        ):
            yield Whitespace(" ")

        yield token

        # update state for next token
        require_newline_before_next_token = isinstance(
            token,
            (
                Comment,
                TopLevelKeyword,
                OpeningBracket,
                ExpressionSeparator,
                StatementSeparator,
            ),
        )
        allow_space_before_next_token = not isinstance(token, FieldAccessOperator)
        prev_was_unary_operator = next_operator_is_unary and isinstance(token, Operator)
        if not isinstance(token, Comment):
            # format next operator as unary if there is no preceding argument
            next_operator_is_unary = not isinstance(
                token, (Literal, Identifier, ClosingBracket)
            )
        allow_space_before_next_bracket = isinstance(
            token, (SpaceBeforeBracketKeyword, Operator)
        )
        if isinstance(token, (TopLevelKeyword, OpeningBracket)):
            # increase indent
            indent_types.append(type(token))
        elif isinstance(token, StatementSeparator):
            # reset indent
            indent_types = []
        first_token = False


class Line:
    """Container for a line of tokens."""

    def __init__(self, indent_token=None):
        """Initialize."""
        self.indent_token = indent_token
        if indent_token is None:
            self.indent_level = 0
        else:
            self.indent_level = len(indent_token.value)
            if indent_token.value.startswith("\n"):
                self.indent_level -= 1
        self.inline_tokens = []
        self.inline_length = 0
        self.contains_comment = isinstance(indent_token, Comment)
        self.can_start_inline_block = False

    def add(self, token):
        """Add a token to this line."""
        self.inline_length += len(token.value)
        self.inline_tokens.append(token)
        self.contains_comment = self.contains_comment or isinstance(token, Comment)
        self.can_start_inline_block = isinstance(token, OpeningBracket)

    @property
    def tokens(self):
        """Get a list of all the tokens in this line."""
        if self.indent_token is None:
            return self.inline_tokens
        else:
            return [self.indent_token] + self.inline_tokens


def inline_block_format(tokens, max_line_length=100):
    """Extend simple_format with inline blocks.

    Inline a bracket block if it would be shorter than max_line_length.

    Do not inline bracket blocks that contain a comment.

    Implementation assumes simple_format will put opening brackets
    at the end of the line and closing brackets at the beginning of the line,
    unless there is a comment between them.
    """
    # format tokens using simple_format, then group into lines
    lines = [Line()]
    for token in simple_format(tokens):
        if token.value.startswith("\n"):
            lines.append(Line(token))
        else:
            lines[-1].add(token)

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
            last_token_is_opening_bracket = True
            index = index + 1  # start on the next line
            for line in lines[index:]:
                if line.contains_comment:
                    break
                if not last_token_is_opening_bracket and not isinstance(
                    line.inline_tokens[0], ClosingBracket
                ):
                    pending.append(Whitespace(" "))
                    line_length += 1
                pending_lines += 1
                pending.extend(line.inline_tokens)
                line_length += line.inline_length
                last_token_is_opening_bracket = isinstance(
                    line.inline_tokens[-1], OpeningBracket
                )
                if line_length > max_line_length:
                    break
                if line.indent_level <= indent_level:
                    # flush pending and handle next block if present
                    yield from pending
                    skip_lines += pending_lines
                    if line.can_start_inline_block:
                        pending_lines = 0
                        pending = []
                    else:
                        break


def reformat(query, format_=inline_block_format):
    """Reformat query and return as a string."""
    tokens = format_(tokenize(query))
    return "".join(token.value for token in tokens)
