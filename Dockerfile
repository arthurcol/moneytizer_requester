FROM python:3.10.6-buster

COPY requirements.txt requirements.txt

RUN pip install -U pip
RUN pip install -r requirements.txt

COPY main.py main.py
COPY params.py params.py

COPY api.py api.py

CMD uvicorn api:app --port=$PORT --host 0.0.0.0
