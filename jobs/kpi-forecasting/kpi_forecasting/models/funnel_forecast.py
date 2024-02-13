from dataclasses import dataclass
from typing import Dict, List, Optional, Union

import pandas as pd
import prophet

from kpi_forecasting.configs.model_inputs import ProphetHoliday, ProphetRegressor


@dataclass
class SegmentModelHolder:
    """
    Holds the configuration and results for each segment
    in a funnel forecasting model.
    """

    segment: Dict[str, str]
    start_date: str
    end_date: str
    holidays: List[ProphetHoliday]
    regressors: List[ProphetRegressor]
    parameters: Dict[str, Union[List[float], float]]
    cv_settings: Dict[str, str]
    trend_change: Optional[str] = ""

    # Hold results as models are trained and forecasts made
    segment_model: prophet.Prophet = None
    trained_parameters: Dict[str, str] = None
    forecast_df: pd.DataFrame = None
