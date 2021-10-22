FROM python:3

RUN pip3 install pandas datetime sqlalchemy boto3 psycopg2
COPY main.py ./

ENTRYPOINT [ "python", "main.py" ]