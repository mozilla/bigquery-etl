from kpi_forecasting.inputs import CLI, YAML
from kpi_forecasting.models.prophet_forecast import ProphetForecast
from kpi_forecasting.metric_hub import MetricHub


# A dictionary of available models in the `models` directory.
MODELS = {
    "prophet": ProphetForecast,
}


def main() -> None:
    # Load the config
    config = YAML(filepath=CLI().args.config).data
    model_type = config.forecast_model.model_type

    if model_type in MODELS:
        metric_hub = MetricHub(**config.metric_hub)
        model = MODELS[model_type](metric_hub=metric_hub, **config.forecast_model)

        model.fit()
        model.predict()
        model.summarize(**config.summarize)
        model.write_results(**config.write_results)

    else:
        raise ValueError(f"Don't know how to forecast using {model_type}.")


if __name__ == "__main__":
    main()
