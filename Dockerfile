FROM python:3.10.6-buster

COPY requirements.txt requirements.txt

RUN pip install -U pip
RUN pip install -r requirements.txt

COPY main.py main.py
COPY params.py params.py

COPY test.py test.py

CMD python3 test.py
