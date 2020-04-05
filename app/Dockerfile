FROM bde2020/spark-submit:2.4.5-hadoop2.7

RUN apk add build-base g++ python3-dev cyrus-sasl-dev

COPY template.sh /
COPY requirements.txt /app/requirements.txt
COPY app.py /app/app.py
COPY submit.sh /submit.sh

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py

CMD ["/bin/bash", "/template.sh"]
