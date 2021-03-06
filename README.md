# Argentine COVID-19 API

This API uses the [Argentinian Ministry of Health (msal.gob.ar) dataset](http://datos.salud.gob.ar/dataset/covid-19-casos-registrados-en-la-republica-argentina)
# 2020
## Installation
Clone the repository using ssh or http

```shell script
# Clone repo using ssh
git clone git@github.com:alavarello/covid-api.git
# Clone repo using http
git clone https://github.com/alavarello/covid-api.git

cd covid-api

docker build -t covid-api --no-cache .
``` 

## Execution

```shell script
docker run -d --name covid-api-container -p 5555:5432 -p 8000:8000 covid-api
docker exec -it covid-api-container ./run.sh
```

# 2019

## Installation

Clone the repository using ssh or http

```shell script
# Clone repo using ssh
git clone git@github.com:alavarello/covid-api.git
# Clone repo using http
git clone https://github.com/alavarello/covid-api.git
``` 

Create the virtual environment and install all the requirements.\
For more information about virtual environments click [here](https://docs.python.org/3/library/venv.html#module-venv) \
The global python version must be > 3.6

```shell script
# Create a virtual environment
python -m venv env
# Activate
source env/bin/activate  # On Windows use `env\Scripts\activate`

# Install Django and Django REST framework into the virtual environment
pip install -r requirements.txt

# Run migrations
python manage.py migrate

# Create .env file
cp docs/env.txt covid_api/.env  # In development
cp docs/env_production.txt covid_api/.env # In production
```

**Important**: Once you copy the env file change the secret key for a random string 

## Update data

**Important**: Always make sure you have the `env` activated before running any command.

To add the cron that updates the data.
```shell script
python manage.py crontab add
```

To force the update
```shell script
python manage.py update_data
```

**Note**: This may take up to an hour to update.
 
## Run

**Important**: Always make sure you have the `env` activated before running any command.

### Development
Make sure the port 8000 is not being used.
```shell script
python manage.py runserver
```

### Production
Make sure the port 8000 is not being used.
```shell script
gunicorn covid_api.wsgi --workers 3 --timeout 600 --bind 0.0.0.0:8000 -D
```

## Docs
```shell script
# Access swagger
http://localhost:8000/api/v1/swagger/
```
