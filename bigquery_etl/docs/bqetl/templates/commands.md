{% macro command_docs(cmd, n) -%}
{{ "#" * n }} `{{ cmd.name }}`

{{ cmd.description }}

{% if cmd.commands | length > 0 -%}
{% for command in cmd.commands -%}
    {{ command_docs(command, n + 1) }}
{% endfor -%}
{% else -%}
**Usage**

```bash
$ ./bqetl{{ cmd.path }} [OPTIONS]  
{%- for arg in cmd.arguments -%}
{{ "" }} [{{ arg.name }}]
{%- endfor %}

{% if cmd.options | length > 1 -%}
Options:

{% for option in cmd.options -%}
--{{ option.name }}: {{ option.description }}
{% endfor -%}
{% endif -%}
```

{% if cmd.examples -%}
**Examples**

```bash
{{ cmd.examples | replace("\n    ", "\n") }}
```
{% endif -%}
{% endif -%}
{% endmacro -%}

{% for command_group in command_groups -%}
    {{ command_docs(command_group, 3) }}
{% endfor -%}
