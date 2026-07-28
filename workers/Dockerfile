# Use a slim Python image as the base
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy only the requirements file first to leverage Docker layer caching
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

ARG WORKER_MODULE

# The command to run the worker will be provided by docker-compose
CMD python workers/${WORKER_MODULE}/main.py