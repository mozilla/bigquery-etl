import json
import pandas as pd
import prophet

from datetime import datetime
from dataclasses import dataclass
from kpi_forecasting.models.base_forecast import BaseForecast
from typing import Dict


@dataclass
class ProphetForecast(BaseForecast):
    @property
    def column_names_map(self) -> Dict[str, str]:
        return {"submission_date": "ds", "value": "y"}

    def _fit(self) -> None:
        self.model = prophet.Prophet(
            **self.parameters,
            uncertainty_samples=self.number_of_simulations,
            mcmc_samples=0,
        )

        if self.use_holidays:
            self.model.add_country_holidays(country_name="US")

        # Modify observed data to have column names that Prophet expects, and fit
        # the model
        self.model.fit(self.observed_df.rename(columns=self.column_names_map))

    def _predict(self) -> pd.DataFrame:
        # generate the forecast samples
        samples = self.model.predictive_samples(
            self.dates_to_predict.rename(columns=self.column_names_map)
        )
        df = pd.DataFrame(samples["yhat"])
        df["submission_date"] = self.dates_to_predict

        return df

    def _predict_legacy(self) -> pd.DataFrame:
        """
        Recreate the legacy format used in
        `moz-fx-data-shared-prod.telemetry_derived.kpi_automated_forecast_v1`.
        """
        # TODO: This method should be removed once the forecasting data model is updated:
        # https://mozilla-hub.atlassian.net/browse/DS-2676

        df = self.model.predict(
            self.dates_to_predict.rename(columns=self.column_names_map)
        )

        # set legacy column values
        if "dau" in self.metric_hub.alias.lower():
            df["metric"] = "DAU"
        else:
            df["metric"] = self.metric_hub.alias

        df["forecast_date"] = str(datetime.utcnow().date())
        df["forecast_parameters"] = str(
            json.dumps({**self.parameters, "holidays": self.use_holidays})
        )

        alias = self.metric_hub.alias.lower()

        if ("desktop" in alias) and ("mobile" in alias):
            raise ValueError(
                "Metric Hub alias must include either 'desktop' or 'mobile', not both."
            )
        elif "desktop" in alias:
            df["target"] = "desktop"
        elif "mobile" in alias:
            df["target"] = "mobile"
        else:
            raise ValueError(
                "Metric Hub alias must include either 'desktop' or 'mobile'."
            )

        columns = [
            "ds",
            "trend",
            "yhat_lower",
            "yhat_upper",
            "trend_lower",
            "trend_upper",
            "additive_terms",
            "additive_terms_lower",
            "additive_terms_upper",
            "extra_regressors_additive",
            "extra_regressors_additive_lower",
            "extra_regressors_additive_upper",
            "holidays",
            "holidays_lower",
            "holidays_upper",
            "regressor_00",
            "regressor_00_lower",
            "regressor_00_upper",
            "weekly",
            "weekly_lower",
            "weekly_upper",
            "yearly",
            "yearly_lower",
            "yearly_upper",
            "multiplicative_terms",
            "multiplicative_terms_lower",
            "multiplicative_terms_upper",
            "yhat",
            "target",
            "forecast_date",
            "forecast_parameters",
            "metric",
        ]

        for column in columns:
            if column not in df.columns:
                df[column] = 0.0

        return df[columns]
