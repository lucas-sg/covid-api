#!/bin/bash

# Create a virtual environment
python3 -m venv env

# Activate
source env/bin/activate

# Create .env file
cp /app/docs/env.txt /app/covid_api/.env  # In development

# Install Django and Django REST framework into the virtual environment
pip3 install -r requirements.txt

# Run migrations
python3 manage.py migrate