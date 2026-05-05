#fail
{{ is_unique(['dl_token', 'ga_client_id', 'stub_session_id', 'download_source']) }}

#fail
{{ min_row_count(1000) }}
