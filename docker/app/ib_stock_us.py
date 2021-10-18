from loguru import logger
import asyncio
import nest_asyncio
import base
from ib_mysql import MysqlDb, UsStockLiveMktData, generate_id
from config import USD_STOCK, TWS_IP, TWS_PORT, USD_STOCK_TIMEZONE, USD_EXCHANGE_TIMESCOPE
from time import sleep
import pytz
from interval import Interval

logger.add(sink='logs/ib_stock_us.log',
           level='ERROR',
           format='{time:YYYY-MM-DD  :mm:ss} - {level} - {file} - {line} - {message}',
           enqueue=True,
           backtrace=True,
           diagnose=True,
           rotation='00:00',
           retention='7 days')

class App:
    @logger.catch(level='ERROR')
    def __init__(self):
        self.db = MysqlDb()
        nest_asyncio.apply()
        self.ib = base.IbAPI()
        self.ib.TimezoneTWS = pytz.timezone(USD_STOCK_TIMEZONE)

    @logger.catch(level='ERROR')
    async def UsStockRun(self):
        with await self.ib.connectAsync(host=TWS_IP, port=TWS_PORT, clientId=2):
            self.ib.reqMarketDataType(1)
            contracts = [base.Stock(symbol, 'SMART', 'USD') for symbol in USD_STOCK]
            for contract in contracts:
                self.ib.reqMktData(contract, genericTickList=233)

            while True:
                sleep(60)
                async for tickers in self.ib.pendingTickersEvent:
                    logger.info(tickers)
                    for ticker in tickers:
                        # logger.info(ticker.time.now(self.ib.TimezoneTWS).isoformat())   # Method 1 to convert timezone
                        # logger.info(ticker.time.astimezone(self.ib.TimezoneTWS).isoformat())    # Method 2 to convert timezone
                        ticker_time = ticker.time.astimezone(self.ib.TimezoneTWS)
                        ex_date = ticker_time.strptime(str(ticker_time.date()), "%Y-%m-%d").weekday()+1
                        if 1 <= ex_date <= 5:
                            now_tmp_time = ticker_time.time().replace(microsecond=0)
                            now_time = int(f'{now_tmp_time.hour:02d}{now_tmp_time.minute:02d}{now_tmp_time.second:02d}')
                            if now_time not in Interval.between(USD_EXCHANGE_TIMESCOPE[0], USD_EXCHANGE_TIMESCOPE[1], closed=False):
                                break
                            logger.info(f'{ticker_time.tzinfo} -> {ticker_time.date()} -> {ticker_time.time().replace(microsecond=0)} -> {ticker.contract.symbol} -> {ticker.contract.currency} -> {ticker.last} -> {ticker.volume}')
                            stock_data = UsStockLiveMktData(id=generate_id(), timezone=ticker_time.tzinfo, date=ticker_time.date(), time=ticker_time.time().replace(microsecond=0), stock=ticker.contract.symbol, lastPrice=ticker.last, volume=ticker.volume, currency=ticker.contract.currency)
                            self.db.add_data(add_data=stock_data, table_name='UsStockLiveMktData')
                    break

    @logger.catch(level='ERROR')
    def stop(self):
        self.ib.disconnect()

if __name__ == '__main__':
    app = App()
    try:
        asyncio.run(app.UsStockRun())
    except (KeyboardInterrupt, SystemExit):
        app.stop()
