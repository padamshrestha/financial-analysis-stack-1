FROM bde2020/spark-submit:2.4.5-hadoop2.7

ENV SPARK_APPLICATION_PYTHON_LOCATION /app/app.py

RUN apk add build-base g++ python3-dev cyrus-sasl-dev cyrus-sasl-plain

COPY . /app
RUN cd /app \
      && pip3 install -r requirements.txt

COPY submit.sh /
CMD ["/bin/bash", "/submit.sh"]