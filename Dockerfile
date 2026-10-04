# Dedicated LiteLLM Gateway with Native Bedrock ConverseStream + Zscaler AI Guard Inspection
FROM ghcr.io/berriai/litellm@sha256:114aca7726c311915c8ea5120fcc44d32a0648c3ae3aec41a1014f0e846b16d1

# Install adapter to separate directory to preserve /app/.venv and /app/docker from base image
WORKDIR /opt/converse-guard

COPY native_input.py native_guard.py converse_app.py config.yaml ./

ENV PYTHONPATH="/opt/converse-guard:/app"
ENV CONFIG_FILE_PATH="/opt/converse-guard/config.yaml"
ENV LITELLM_LOCAL_MODEL_COST_MAP="True"

EXPOSE 4000

# Explicitly override upstream ENTRYPOINT ["docker/prod_entrypoint.sh"] which runs 'litellm "$@"'
ENTRYPOINT ["/app/.venv/bin/python3", "-m", "uvicorn", "converse_app:app", "--host", "0.0.0.0", "--port", "4000", "--log-level", "info"]
