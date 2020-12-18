# Use postgres base image as parent image
FROM postgres:latest

# install Python 3
RUN apt-get update -y && apt-get install -y python3 python3-pip python3-venv python3-dev gcc

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

ENV LANG C.UTF-8
#ENV HOME=/app

# Install the application itself
RUN --mount=type=ssh ./install.sh

# Create postgres schema
ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD postgres
ENV POSTGRES_DB testdb
COPY init.sql /docker-entrypoint-initdb.d/