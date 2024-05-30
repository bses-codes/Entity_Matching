FROM apache/airflow:latest

RUN pip install scikit-learn
RUN pip install pymysql
RUN pip install sqlalchemy