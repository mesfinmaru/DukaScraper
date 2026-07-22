FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ ./app/
COPY workers/ ./workers/

ARG WORKER_MODULE
ENV WORKER_MODULE=${WORKER_MODULE}

# Worker directories use hyphens, which are not valid Python module names.
# String format ensures Docker smoothly interpolates the environment variable with spaces intact.
CMD python /app/workers/${WORKER_MODULE}/main.py