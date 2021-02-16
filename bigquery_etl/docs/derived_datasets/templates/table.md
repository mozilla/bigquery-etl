## [{{ table_name }}](#{{ table_name }})

{{ metadata.description | e }}

{% if metadata.friendly_name -%}
* Friendly name: {{metadata.friendly_name}}
{% endif -%}

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
  		<td><a href={{ project_url + "/" + table.projectId }}>{{ table.projectId }}</a></td>
  		<td><a href={{ project_url + "/" + table.projectId+ "/" + table.datasetId }}>{{ table.datasetId }}</a></td>
        <td><a href={{ project_url + "/" + table.projectId + "/" + table.datasetId + "/" + table.tableId }}>{{ table.tableId }}</a></td>
        </tr>
{%- endfor %}	
</table>
{% endif %}

{% for key, value in source_urls.items() -%} [{{key}}]({{ value }}) {{ " | " if not loop.last else "" }} {%- endfor %}

---

