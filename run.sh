#!/bin/bash

symbol=
date=

while [[ $symbol = "" ]]; do
   read -e -p "Stock Symbol [AAPL]: " symbol
done

while [[ $date = "" ]]; do
   read -e -p "Date [2021-04-10]: " date
done

echo "SPARK_APPLICATION_ARGS=$symbol $date" > app.env

exec docker-compose up -d --no-deps app

echo "Spark application starting..."
