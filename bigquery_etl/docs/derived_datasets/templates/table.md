## [{{ table_name }}]({{ source_urls["Source"] }})

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

{% for key, value in source_urls.items() -%}
[{{key}}]({{ value }}) {{ " | " if not loop.last else "" }}
{%- endfor %} 

---

