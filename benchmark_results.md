# Benchmark Results

## Konfigurace testovacího stroje

- OS: Linux (GitHub Actions runner)
- CPU: 2 vCPU (standardní `ubuntu-latest` runner)
- RAM: ~7 GB
- Python: 3.12
- Server: Uvicorn + FastAPI (`app.main:app`)

## Spuštění benchmarku

```bash
python benchmark.py --publishers 5 --subscribers 5 --messages-per-publisher 10000
```

## Naměřené výsledky

Reálně spuštěné měření v tomto sandboxu (kvůli časovému limitu běhu):

```bash
python benchmark.py --publishers 2 --subscribers 2 --messages-per-publisher 10
```

| Formát      | Čas [s] | Propustnost [msg/s] |
|-------------|---------|---------------------|
| JSON        | 0.271   | 147.45              |
| MessagePack | 0.185   | 216.49              |

## Zhodnocení JSON vs MessagePack

MessagePack měl v tomto měření vyšší propustnost, protože přenáší kompaktnější binární data a snižuje režii serializace/deserializace textového JSON formátu.  
Skript je připraven i na plné zadání (5/5/10000), ale tento konkrétní report obsahuje čísla z kratšího validančního běhu v CI sandboxu.

## AI Report

AI pomohla navrhnout `ConnectionManager`, který odděluje mapování `topic -> websockety` od metadat připojení (odběry a preferovaný formát).  
Díky tomu je odpojování klientů bezpečné a centrální, bez duplicitní logiky v endpointu.

U asynchronních testů v `pytest` AI doporučila izolovat synchronní `TestClient` do `asyncio.to_thread`, takže testy mohou běžet pod `pytest-asyncio`, ale stále pohodlně testovat WebSocket scénáře.  
To pomohlo vyhnout se blokování event loopu a udělat testy stabilnější.
