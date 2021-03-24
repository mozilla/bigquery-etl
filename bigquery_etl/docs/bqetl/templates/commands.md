{% for command_group in command_groups -%}
### `{{command_group.name}}`

{{command_group.description}}

{% for command in command_group.commands -%}
#### `{{command.name}}`

{{ command.description }}

##### Usage

```bash
$ bqetl {{ command_group.name }} {{ command.name }} [OPTIONS]
```

{% if command.examples -%}
##### Examples

```bash
{{ command.examples }}
```
{% endif -%}
{% endfor -%}

{% if command_group.commands | length == 0 -%}
#### Usage

```bash
$ bash bqetl {{ command_group.name }} [OPTIONS]
```

{% if command_group.examples -%}
#### Examples

```bash
{{ command_group.examples }}
```
{% endif -%}
{% endif -%}
{% endfor -%}
