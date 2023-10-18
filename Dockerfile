FROM python

WORKDIR /app
COPY pipeline.py pipeline.py
COPY Animal.csv Animal.csv
RUN pip install pandas sqlalchemy psycopg2
# RUN python pipeline.py Animal.csv
ENTRYPOINT ["python","pipeline.py"]