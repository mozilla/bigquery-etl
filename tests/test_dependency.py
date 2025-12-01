from bigquery_etl.dependency import extract_table_references


class TestDependency:
    def test_extract_table_refs_correctly_ignores_unpivot(self):
        unpivot_query = "SELECT * FROM a UNPIVOT(b FOR c IN (d, e, f))"
        refs = extract_table_references(unpivot_query)

        assert refs == ["a"]

    def test_extract_table_refs_correctly_ignores_pivot(self):
        pivot_query = """SELECT * FROM Produce
          PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
        """
        refs = extract_table_references(pivot_query)

        assert refs == ["Produce"]

    def test_extract_table_refs_pivot_and_join(self):
        pivot_query = """SELECT * FROM Produce
          PIVOT(SUM(sales) FOR quarter IN ('Q1', 'Q2', 'Q3', 'Q4'))
          JOIN Perishable_Mints USING (name)
        """
        refs = extract_table_references(pivot_query)

        assert set(refs) == {"Produce", "Perishable_Mints"}

    def test_extract_table_refs_with_ctes(self):
        sql = """
          WITH foo AS (SELECT * FROM bar)
          SELECT * FROM foo
        """
        refs = extract_table_references(sql)

        assert refs == ["bar"]

    def test_extract_table_refs_with_temp_udfs(self):
        sql = """
          CREATE TEMP FUNCTION foo() AS ((SELECT MAX(foo) FROM bar));
          SELECT foo()
        """
        refs = extract_table_references(sql)

        assert refs == ["bar"]

    def test_extract_table_refs_with_temp_tables(self):
        sql = """
          CREATE TEMP TABLE foo AS SELECT * FROM bar;
          SELECT * FROM foo
        """
        refs = extract_table_references(sql)

        assert refs == ["bar"]
