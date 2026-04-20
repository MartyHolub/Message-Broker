# Message-Broker

Jednoduchý asynchronní Message Broker (Pub/Sub) ve FastAPI přes WebSockety.

## Instalace

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Spuštění brokera

```bash
uvicorn app.main:app --reload
```

Endpoint: `ws://127.0.0.1:8000/broker`

## Alembic migrace

```bash
alembic upgrade head
```

## Klient

Publisher (JSON):

```bash
python mb_client.py --mode publisher --topic sensors --format json --count 10
```

Subscriber (MessagePack):

```bash
python mb_client.py --mode subscriber --topic sensors --format msgpack
```

## Benchmark

```bash
python benchmark.py --publishers 5 --subscribers 5 --messages-per-publisher 10000
```

## Testy

```bash
pytest -q
```
