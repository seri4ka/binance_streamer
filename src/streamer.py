import logging
from binance.websocket.spot.websocket_stream import SpotWebsocketStreamClient

# Настройка логирования
logging.basicConfig(level=logging.INFO)

class BinanceStreamer:
    def __init__(self):
        # Инициализация WebSocket клиента
        self.ws_client = SpotWebsocketStreamClient(on_message=self.message_handler)

    def message_handler(self, message, data=None):
        """
        Функция для обработки сообщений, полученных через WebSocket.
        """
        # Выводим сообщение и дополнительные данные
        logging.info(f"Received message: {message}")
        if data:
            logging.info(f"Additional data: {data}")


    def start_trade_stream(self, symbol):
        """
        Подписка на поток сделок (trade stream) для указанного торгового символа.
        """
        self.ws_client.trade(symbol=symbol)

    def start_combined_stream(self, streams):
        """
        Подписка на комбинированный поток (несколько стримов одновременно).
        """
        self.ws_client.instant_subscribe(stream=streams)

    def stop(self):
        """
        Остановка WebSocket клиента.
        """
        self.ws_client.stop()
