#!/bin/bash

docker compose up -d 

aws --endpoint-url=http://localhost:4566 s3 mb s3://nyc-duration

pipenv install
pipenv run python integration_test.py

docker compose down