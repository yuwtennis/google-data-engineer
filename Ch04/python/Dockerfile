FROM python:3.9-bookworm
WORKDIR /app
COPY requirements.txt /app
RUN pip install --upgrade pip wheel -r requirements.txt
COPY . /app