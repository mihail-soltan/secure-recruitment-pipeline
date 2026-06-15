FROM python:3.10.18-alpine3.22

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN apk add --no-cache build-base libffi-dev gcc musl-dev python3-dev git bash openssl-dev cyrus-sasl-dev


RUN git clone https://github.com/edenhill/librdkafka.git && cd librdkafka \
    && git checkout tags/v2.13.0 && ./configure --clean \
    && ./configure --prefix /usr/local \
    && make && make install
RUN pip install confluent-kafka==2.13.0

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]