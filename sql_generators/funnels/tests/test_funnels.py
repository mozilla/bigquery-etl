import os
import sys
from pathlib import Path

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from textwrap import dedent

from __init__ import generate_funnels

BASE_DIR = Path(os.path.dirname(__file__)).parent


class TestFunnels:
    def test_generate_funnels(self, tmp_path):
        output_dir = tmp_path / "sql" / "test-project" / "mozilla_vpn_derived"
        output_dir.mkdir(parents=True)

        config_dir = tmp_path / "configs"
        config_dir.mkdir(parents=True)

        config = dedent(
            """
            destination_dataset = "mozilla_vpn_derived"
            platform = "mozilla_vpn"
            owners = ["example@mozilla.org"]  # optional; users getting notification if funnel run fails
            version = "1"  # optional; default is set to 1

            [funnels]

            [funnels.subscription_funnel]

            friendly_name = "Start Subscription Funnel"
            description = "Funnel from Signup to starting a subscription"
            steps = ["signup", "verify", "start_subscription"]
            dimensions = ["os"]

            [funnels.subscription_error_funnel]
            friendly_name = "Subscription Error Funnel"
            description = "Funnel from Signup to running into an error"
            steps = ["signup", "verify", "error_subscription"]


            [steps]

            [steps.signup]
            friendly_name = "Sign up"
            description = "Sign up for VPN"
            data_source = "events"
            filter_expression = '''
                event_name = 'authentication_inapp_step' AND
                `mozfun.map.get_key`(event_extra, 'state') = 'StateVerifyingSessionEmailCode'
            '''
            join_previous_step_on = "client_info.client_id"
            select_expression = "client_info.client_id"
            aggregation = "count distinct"

            [steps.verify]
            friendly_name = "Verify"
            description = "Verify email"
            data_source = "events"
            select_expression = "client_info.client_id"
            where_expression = '''
                event_name = 'authentication_inapp_step' AND
                `mozfun.map.get_key`(event_extra, 'state') = 'StateVerifyingSessionEmailCode'
            '''
            aggregation = "count distinct"
            join_previous_step_on = "client_info.client_id"

            [steps.start_subscription]
            friendly_name = "Start Subscription"
            description = "Start VPN subscription"
            data_source = "events"
            select_expression = "client_info.client_id"
            where_expression = "event_name = 'iap_subscription_started'"
            aggregation = "count distinct"
            join_previous_step_on = "client_info.client_id"

            [steps.error_subscription]
            friendly_name = "Subscription Error"
            description = "subscription error"
            data_source = "events"
            select_expression = "client_info.client_id"
            where_expression = "event_name = 'error_alert_shown'"
            aggregation = "count"
            join_previous_step_on = "client_info.client_id"


            [data_sources]

            [data_sources.events]
            from_expression = '''
                (SELECT * FROM mozdata.mozilla_vpn.events_unnested
                WHERE client_info.app_channel = 'production' AND client_info.os = 'iOS')
            '''
            submission_date_column = "DATE(submission_timestamp)"
            client_id_column = "client_info.client_id"


            [dimensions]

            [dimensions.os]
            data_source = "events"
            select_expression = "normalized_os"
            friendly_name = "Operating System"
            description = "Normalized Operating System"
            client_id_column = "client_info.client_id"
            """
        )
        (config_dir / "test-funnel.toml").write_text(config)

        generate_funnels(
            target_project="test-project",
            path=config_dir,
            output_dir=output_dir.parent.parent,
        )

        (output_dir / "test_funnel_v1" / "query.sql").read_text() == (
            BASE_DIR / "tests" / "test_funnel"
        ).read_text()
