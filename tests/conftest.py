import pytest

def pytest_configure():
    exec(open("script/generate_sql").read())
