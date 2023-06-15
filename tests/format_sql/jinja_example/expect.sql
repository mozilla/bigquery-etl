{% set options = ["a", "b", "c"] %}{# sample comment #}
SELECT
  {% for option in options %}
    {% if option == "a" %}
      "option a" AS a,
    {% else %}
      "{{ option }}" AS {{ option }},
    {% endif %}
  {% endfor %}
  test,{# another comment #}
  foo
