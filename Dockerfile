FROM python:3.7-slim-buster

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc python-dev vim nano \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt \
    && pip install . \
    && apt-get purge -y --auto-remove gcc python-dev

RUN mkdir files
WORKDIR /files