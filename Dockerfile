FROM bitnami/spark

USER root

# Environment variables
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

ENV STOCK_LEVEL_FILENAME='stock-level.csv'
ENV TRANSACTIONS_FILENAME='transactions.csv'
ENV OUT_OF_STOCK_FACTOR=1.5
ENV OUT_OF_STOCK_MIN_HOURS=4
ENV RECURRENT_OUT_OF_STOCK_MIN_TIMES=2

# Install Pipenv
RUN python -m pip install pipenv

# Install requirements
COPY Pipfile .
COPY Pipfile.lock .
RUN pipenv lock -r > requirements.txt
RUN python -m pip install -r requirements.txt

# Copy the code to the image
COPY app.py .

# Copy source data to the image
COPY ./data/*.csv /opt/bitnami/spark/data

ENTRYPOINT ["python", "app.py"]