FROM python:3.7
WORKDIR /app
ADD . /app
RUN pip install -r requirements.txt
CMD ["celery", "worker", "-A", "worker_tasks", "-l", "info", "-E", "-n", "fxp-tst@%h", "--concurrency","3"]
