import asyncio
import websockets
import json
import pandas as pd
import os
from datetime import datetime

# Основной URL Binance WebSocket API
BASE_URL = "wss://stream.binance.com:9443/ws"

# Доступные потоки
STREAMS = {
    "klines": "{symbol}@kline_{interval}",  # Интервал указывается дополнительно
    "trades": "{symbol}@trade",
    "aggTrades": "{symbol}@aggTrade",
    "depth": "{symbol}@depth20@100ms"  # Пример частичной книги заявок
}

# Глобальный DataFrame для хранения данных
data_frames = {
    "klines": pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"]),
    "trades": pd.DataFrame(columns=["trade_id", "price", "quantity", "timestamp"]),
    "aggTrades": pd.DataFrame(columns=["agg_id", "price", "quantity", "first_id", "last_id", "timestamp"]),
    "depth": pd.DataFrame(columns=["timestamp", "bids", "asks"])
}

# Функция для подключения к WebSocket
async def connect_to_stream(symbol, stream_type, interval=None, save_interval=60):
    if stream_type not in STREAMS:
        raise ValueError(f"Неверный тип потока: {stream_type}. Допустимые значения: {', '.join(STREAMS.keys())}")

    # Формируем имя потока
    if stream_type == "klines" and interval:
        stream_name = STREAMS[stream_type].format(symbol=symbol, interval=interval)
    elif stream_type == "klines" and not interval:
        raise ValueError("Для потока klines необходимо указать параметр interval.")
    else:
        stream_name = STREAMS[stream_type].format(symbol=symbol)

    # Полный URL для подключения
    url = f"{BASE_URL}/{stream_name}"
    print(f"Подключение к потоку: {url}")

    # Подключение к WebSocket
    async with websockets.connect(url) as websocket:
        save_task = asyncio.create_task(save_data_periodically(stream_type, save_interval))
        while True:
            try:
                # Получение сообщения
                data = await websocket.recv()
                process_data(json.loads(data), stream_type)
            except websockets.ConnectionClosed as e:
                print(f"Соединение закрыто: {e}")
                break
        save_task.cancel()

# Обработчики данных для разных типов
def process_data(data, stream_type):
    global data_frames
    if stream_type == "klines":
        kline = data["k"]
        new_row = {
            "time": datetime.utcfromtimestamp(kline["t"] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
            "open": float(kline["o"]),
            "high": float(kline["h"]),
            "low": float(kline["l"]),
            "close": float(kline["c"]),
            "volume": float(kline["v"]),
        }
    elif stream_type == "trades":
        new_row = {
            "trade_id": data["t"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "timestamp": datetime.utcfromtimestamp(data["T"] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
        }
    elif stream_type == "aggTrades":
        new_row = {
            "agg_id": data["a"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "first_id": data["f"],
            "last_id": data["l"],
            "timestamp": datetime.utcfromtimestamp(data["T"] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
        }
    elif stream_type == "depth":
        new_row = {
            "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "bids": data["b"],
            "asks": data["a"],
        }
    else:
        print(f"Неизвестный поток: {data}")
        return

    # Добавление строки в DataFrame
    data_frames[stream_type] = pd.concat(
        [data_frames[stream_type], pd.DataFrame([new_row])], ignore_index=True
    )

# Функция для сохранения данных в CSV
async def save_data_periodically(stream_type, interval):
    while True:
        await asyncio.sleep(interval)
        save_to_csv(stream_type)

def save_to_csv(stream_type):
    global data_frames
    file_name = f"{stream_type}_data.csv"
    if not data_frames[stream_type].empty:
        if os.path.exists(file_name):
            data_frames[stream_type].to_csv(file_name, mode="a", header=False, index=False)
        else:
            data_frames[stream_type].to_csv(file_name, index=False)
        print(f"Данные сохранены в {file_name}.")
        data_frames[stream_type] = data_frames[stream_type][0:0]  # Очистка DataFrame после сохранения

# Основной метод запуска
def run_streamer(symbol, stream_type, interval=None, save_interval=60):
    asyncio.run(connect_to_stream(symbol, stream_type, interval, save_interval))

# Пример использования
if __name__ == "__main__":
    # Ввод данных
    symbol = input("Введите символ торговой пары (например, btcusdt): ").strip().lower()
    stream_type = input("Выберите тип потока (klines, trades, aggTrades, depth): ").strip()
    interval = None
    if stream_type == "klines":
        interval = input("Введите интервал для klines (например, 1m, 4h, 1d): ").strip()

    # Запуск стримера
    try:
        run_streamer(symbol, stream_type, interval)
    except ValueError as e:
        print(f"Ошибка: {e}")
