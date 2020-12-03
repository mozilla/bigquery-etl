import pytest


@pytest.fixture
def example_dependency():
    return "test"


class TestMain:
    def test_something(self, example_dependency):
        assert example_dependency == "test"
