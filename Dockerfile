FROM python:3.12-slim

WORKDIR /app

# install debian packages
RUN apt-get update && \
    apt-get install -y curl unzip git locales

# Uncomment en_US.UTF-8 and generate its data
RUN sed -i '/en_US.UTF-8/s/^# //g' /etc/locale.gen && \
    locale-gen

# Set environment variables so Python and other processes use UTF-8
ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8

RUN groupadd -r -g 1000 app \
 && useradd  -r -u 100 -g 1000 -d /app app
RUN mkdir /venv
RUN chown -R app:app /app /venv
USER app

# install poetry
RUN curl -sSL https://install.python-poetry.org | POETRY_VERSION=2.1.1 python3 -
ENV PATH="/app/.local/bin:$PATH"

# ask python to activate a new virtualenv for poetry
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"
ENV VIRTUAL_ENV=/venv

ENV PYTHONPATH="/app"

# Copy Poetry files and install dependencies
COPY --chown=app:app pyproject.toml poetry.lock* ./
RUN poetry install --no-root

# Copy application code
COPY --chown=app:app sensor_pipeline/ ./sensor_pipeline/
COPY --chown=app:app sensor_pipeline_prefect/ ./sensor_pipeline_prefect/
COPY --chown=app:app data/ ./data/

# Create output directory
RUN mkdir -p out

# Set entrypoint and default command for CLI usage
ENTRYPOINT ["python", "-m", "sensor_pipeline.cli"]
CMD ["data/sensor_data.json", "out/mesh_summary.json"] 