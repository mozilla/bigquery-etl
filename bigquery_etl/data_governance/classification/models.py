"""Call a Vertex-hosted Gemini model for a structured classification answer.

The classifier labels one column per model call. This module wraps that call:
build the Vertex client for a run, send a prompt with a response schema, and
return the parsed JSON object. Transient upstream failures are retried with
exponential backoff; anything else re-raises so the caller can skip that column.

Token usage accumulates on the model instance (`model.totals`) rather than being
returned per call, so a whole run can be costed from a single log line.
"""

import logging
import time
from dataclasses import dataclass
from typing import Any

from google import genai
from google.genai import errors, types

from .config import ClassificationConfig

DEFAULT_RETRIES = 4
DEFAULT_BASE_DELAY_SECONDS = 2.0

# Status codes signalling a transient upstream failure a short retry can ride
# out. Vertex in particular returns sporadic 502s under load, which previously
# dropped a column's classification silently. Same set the SDK uses for its own
# (opt-in) retry policy, so a code outside it is one Google does not treat as
# transient either.
_RETRYABLE_STATUS_CODES = (408, 429, 500, 502, 503, 504)

_CLASSIFICATION_RESPONSE_SCHEMA = types.Schema(
    type=types.Type.OBJECT,
    properties={
        "primary_label": types.Schema(
            type=types.Type.STRING,
            description="The single most specific taxonomy label for the column.",
        ),
        "secondary_labels": types.Schema(
            type=types.Type.ARRAY,
            items=types.Schema(type=types.Type.STRING),
            nullable=True,
            description="Additional taxonomy labels that also apply.",
        ),
        "confidence": types.Schema(
            type=types.Type.STRING,
            nullable=True,
            description="Self-reported confidence: high, medium, or low.",
        ),
        "reasoning": types.Schema(
            type=types.Type.STRING,
            nullable=True,
            description="One or two sentences explaining the signals used.",
        ),
        "needs_review": types.Schema(
            type=types.Type.BOOLEAN,
            nullable=True,
            description="True when confidence is low or signals conflict.",
        ),
    },
    # The runner deliberately accepts a terse answer and fills the other fields
    # with [] or None. Keep only the label mandatory to preserve that contract.
    required=["primary_label"],
    property_ordering=[
        "primary_label",
        "secondary_labels",
        "confidence",
        "reasoning",
        "needs_review",
    ],
)

_GENERATION_CONFIG = types.GenerateContentConfig(
    response_mime_type="application/json",
    response_schema=_CLASSIFICATION_RESPONSE_SCHEMA,
)


def _make_client(project: str, location: str) -> genai.Client:
    """Vertex client for the given project and location."""
    return genai.Client(
        vertexai=True,
        project=project,
        location=location,
        http_options=types.HttpOptions(api_version="v1"),
    )


def _is_transient(exc: BaseException) -> bool:
    """Return True if the error is one Vertex may answer differently next time."""
    return isinstance(exc, errors.APIError) and exc.code in _RETRYABLE_STATUS_CODES


@dataclass
class TokenTotals:
    """Running LLM usage for one classification run."""

    calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0


class VertexModel:
    """One Gemini model, called serially, with retries and running token totals."""

    def __init__(
        self,
        model: str,
        project: str,
        location: str,
        retries: int = DEFAULT_RETRIES,
        base_delay: float = DEFAULT_BASE_DELAY_SECONDS,
        client: Any = None,
    ):
        """Build the client unless one is injected, and start the totals at zero.

        retries and base_delay control the transient-failure backoff; only the
        tests set them. Pass client to inject an alternative to the Vertex client.
        """
        self.model = model
        self.retries = retries
        self.base_delay = base_delay
        self._client = client if client is not None else _make_client(project, location)
        self.totals = TokenTotals()

    def _call_with_retry(self, prompt: str) -> Any:
        """Send one prompt, retrying transient failures with exponential backoff.

        Retries flaky 5xx and rate-limit (429) responses; everything else (auth,
        a dropped connection) re-raises immediately. A hard-quota 429 is
        indistinguishable from a rate-limit 429 here, so it is retried (backing
        off) before failing.
        """
        attempts = self.retries + 1
        for attempt in range(1, attempts + 1):
            try:
                return self._client.models.generate_content(
                    model=self.model,
                    contents=prompt,
                    config=_GENERATION_CONFIG,
                )
            except Exception as exc:
                if not _is_transient(exc) or attempt == attempts:
                    raise
                delay = self.base_delay * (2 ** (attempt - 1))
                logging.warning(
                    "Transient model error, retrying in %.0fs (attempt %s/%s): %s",
                    delay,
                    attempt,
                    attempts,
                    exc,
                )
                time.sleep(delay)
        raise RuntimeError(f"Exhausted retries calling {self.model}")

    def _accumulate(self, response: Any) -> None:
        """Add one response's token usage to the running totals."""
        u = getattr(response, "usage_metadata", None)
        self.totals.calls += 1
        self.totals.input_tokens += getattr(u, "prompt_token_count", 0) or 0
        # candidates_token_count excludes thinking tokens; add them so cost is
        # right for thinking-enabled Gemini models.
        self.totals.output_tokens += (getattr(u, "candidates_token_count", 0) or 0) + (
            getattr(u, "thoughts_token_count", 0) or 0
        )

    def generate_json(self, prompt: str) -> dict[str, Any]:
        """Call the model and return its schema-constrained JSON object.

        Raises on a model error that outlived the retries, on a response
        carrying no text, and on text that is not a JSON object, so the caller's
        per-column skip applies to all three.
        """
        response = self._call_with_retry(prompt)
        # Billed on arrival, so count before inspecting the parsed result: a
        # response blocked by a safety filter still costs what it cost.
        self._accumulate(response)
        result = getattr(response, "parsed", None)
        if isinstance(result, dict):
            return result
        # The SDK swallows its own JSONDecodeError and leaves parsed unset.
        # Preserve enough metadata to distinguish that from an empty response,
        # but do not put model output (which may echo prompt data) in the logs.
        text = getattr(response, "text", None)
        if not text:
            raise ValueError("model returned no text")
        raise ValueError(
            "model response could not be parsed as a JSON object "
            f"({len(text)} characters)"
        )


def model_for(config: ClassificationConfig) -> VertexModel:
    """Build the VertexModel for a run."""
    project = config.vertex_project or config.project
    logging.info(
        "Vertex model %s in %s (%s)", config.model, project, config.vertex_location
    )
    return VertexModel(
        model=config.model,
        project=project,
        location=config.vertex_location,
    )
