## [{{ table_name }}](#{{ table_name }})

{% if metadata.friendly_name -%}
**{{ metadata.friendly_name }}**

{% endif -%}

`{{ qualified_table_name }}`

{{ metadata.description or "" | e }}

{% if metadata.labels -%}
{% if metadata.labels.schedule -%}
* Schedule: {{metadata.labels.schedule}}
{% endif -%}
{% endif -%}

{% if metadata.owners -%}
* Owners:
{% for email in metadata.owners -%}
{% filter indent(width=4) %}
- [{{email}}](mailto:{{email}})
{% endfilter %}
{%- endfor %}
{% endif %}

{{ readme_content or "" }}

{% if schema -%}

<table>
<caption>Schema</caption>
	<tr>
		<th>Column</th>
		<th>Description</th>
		<th>Type</th>
		<th>Nullable</th>
 	</tr>
{% for field in schema -%}
 	<tr>
        <td>{{ field.name }}</td>
        <td>{{ field.description or "" }}</td>
        <td>{{ field.type | capitalize }}</td>
        <td>{{ 'Yes' if field.mode == 'NULLABLE' else 'No' }}</td>
    </tr>
{%- endfor %}
</table>

{% endif %}

{% if referenced_tables -%}
<table>
<caption>Referenced Tables</caption>
	<tr>
		<th>Project</th>
		<th>Dataset</th>
		<th>Table</th>
 	</tr>
{% for table in referenced_tables -%}
 	<tr>
  		<td><a href={{ project_url + "/" + table.project_id }}>{{ table.project_id }}</a></td>
  		<td><a href={{ project_url + "/" + table.project_id+ "/" + table.dataset_id }}>{{ table.dataset_id }}</a></td>
        <td><a href={{ project_url + "/" + table.project_id + "/" + table.dataset_id + "/" + table.table_id }}>{{ table.table_id }}</a></td>
        </tr>
{%- endfor %}
</table>
{% endif %}

{% for key, value in source_urls.items() -%} [{{key}}]({{ value }}) {{ " | " if not loop.last else "" }} {%- endfor %}

---

