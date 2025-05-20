FROM python:3.13-alpine

WORKDIR /worker

COPY ./requirements.txt /worker/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /worker/requirements.txt

COPY . /worker/

# Run the worker using arq demo.WorkerSettings --watch path/to/src
CMD ["arq", "main.WorkerSettings", "--watch", "/worker"]