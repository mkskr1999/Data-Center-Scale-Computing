FROM python:3.11

WORKDIR /app

COPY Homework1.py Homework_1.py
COPY Housing_Data_1.csv Housing_Data_1.csv

RUN pip install pandas

RUN python Homework_1.py Housing_Data_1.csv Housing_Data_out.csv

ENTRYPOINT ["bash"]
