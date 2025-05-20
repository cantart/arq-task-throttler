FROM python:3.13-alpine

WORKDIR /worker

COPY ./requirements.txt /worker/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /worker/requirements.txt

COPY . /worker/

ENV PYTHONPATH=/worker

CMD ["arq", "main.WorkerSettings", "--watch", "."]