FROM bde2020/spark-submit:2.4.5-hadoop2.7

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py
ENV SPARK_APPLICATION_ARGS "DBX"

RUN apk add build-base g++ python3-dev cyrus-sasl-dev cyrus-sasl-plain

COPY . /app
RUN pip3 install -r app/requirements.txt

COPY submit.sh /
CMD ["/bin/bash", "/submit.sh"]