"""Call a Vertex-hosted Gemini model and parse its JSON answer.

The classifier labels one column per model call. This module wraps that call:
build the Vertex client for a run, send a prompt, strip any markdown fences off
the answer, and parse it as JSON. Transient upstream failures are retried with
exponential backoff; anything else re-raises so the caller can skip that column.

Token usage accumulates on the model instance (`model.totals`) rather than being
returned per call, so a whole run can be costed from a single log line.
"""

import json
import logging
import re
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


def _make_client(project: str, location: str) -> genai.Client:
    """Vertex client for the given project and location."""
    return genai.Client(
        vertexai=True,
        project=project,
        location=location,
        http_options=types.HttpOptions(api_version="v1"),
    )


def _strip_json_fences(text: str) -> str:
    """Remove optional ```json ... ``` markdown fences."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text


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
        """Call the model and return its answer parsed as JSON.

        Raises on a model error that outlived the retries, and on a response whose
        text is not JSON, so the caller's per-column skip applies either way.
        """
        response = self._call_with_retry(prompt)
        # Billed on arrival, so count before parsing: a response whose text is
        # unusable (malformed JSON, or a safety filter returning none at all)
        # still costs what it cost.
        self._accumulate(response)
        return json.loads(_strip_json_fences(response.text))


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
