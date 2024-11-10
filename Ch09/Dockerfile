FROM asia-docker.pkg.dev/vertex-ai/training/tf-cpu.2-14.py310:latest

WORKDIR /app
COPY . ./

ENTRYPOINT  ["python3", "-m", "trainer.task"]