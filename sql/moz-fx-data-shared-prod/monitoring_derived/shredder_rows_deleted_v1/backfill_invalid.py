"""Intentionally invalid Python, used to exercise `bqetl backfill validate --dry-run`.

The function definition below has a syntax error (missing closing paren), so the
syntax check during backfill validation fails.
"""


def main(:
    return 1
