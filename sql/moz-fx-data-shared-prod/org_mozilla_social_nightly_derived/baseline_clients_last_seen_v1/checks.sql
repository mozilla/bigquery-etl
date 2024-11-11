-- Override the generated checks, as this application doesn't send a lot of data.
-- Eventually this app will be shut down completely: https://mozilla-hub.atlassian.net/wiki/spaces/MozSocial/pages/313229496/Mozilla+Social+Shutdown

#warn
{{ is_unique(["client_id"], where="submission_date = @submission_date") }}
