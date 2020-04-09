#!/usr/bin/env python3
from argparse import ArgumentParser
from datetime import date
from os import getcwd
from subprocess import run

APP_ENV_FILE = 'app.env'
APP_SERVICE_NAME = 'app'

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('symbol',
                        type=lambda sym: sym if 0 < len(sym) <= 5 else False,
                        help="Stock listing to model for")
    parser.add_argument('date',
                        type=date.fromisoformat,
                        help="Predict stock's price at <yyyy-mm-dd>")
    args = parser.parse_args()

    with open(APP_ENV_FILE, 'w') as env_file:
        env_file.write(f"APP_SYMBOL={args.symbol}\n")
        env_file.write(f"APP_DATE={args.date.isoformat()}\n")

    run(args=['docker-compose', 'up', '-d', '--no-deps', APP_SERVICE_NAME],
        cwd=getcwd())
