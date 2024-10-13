FROM asia-docker.pkg.dev/vertex-ai/training/tf-cpu.2-14.py310:latest

ENV POETRY_HOME=/usr/local

RUN apt update && apt install curl
RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.8.3

WORKDIR /app

COPY . ./
RUN poetry install --only-root
ENTRYPOINT  ["python3", "trainer/task.py"]