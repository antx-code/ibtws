from loguru import logger
from interval import Interval
from datetime import datetime
import pytz
from base import *
from ib_mysql import MysqlDb, AccountValue, generate_id
from config import TWS_IP, TWS_PORT, ACCOUNT_ID, USD_STOCK_TIMEZONE, HKD_STOCK_TIMEZONE, USD_EXCHANGE_TIMESCOPE, HKD_EXCHANGE_TIMESCOPE

logger.add(sink='logs/ib_account.log',
           level='ERROR',
           format='{time:YYYY-MM-DD  :mm:ss} - {level} - {file} - {line} - {message}',
           enqueue=True,
           backtrace=True,
           diagnose=True,
           rotation='00:00',
           retention='7 days')

@logger.catch(level='ERROR')
def poc_ib_account_api():
    ib = IbAPI()
    ib.connect(host=TWS_IP, port=TWS_PORT, clientId=1)
    ib.reqMarketDataType(1)
    db = MysqlDb()

    while True:
        result = ib.accountSummary(ACCOUNT_ID)
        us_timezone = pytz.timezone(USD_STOCK_TIMEZONE)
        hk_timezone = pytz.timezone(HKD_STOCK_TIMEZONE)
        tz_us_time = datetime.now(tz=us_timezone)
        tz_hk_time = datetime.now(tz=hk_timezone)
        us_date = tz_us_time.date()
        hk_date = tz_hk_time.date()
        now_us_tmp_time = tz_us_time.time().replace(microsecond=0)
        now_hk_tmp_time = tz_hk_time.time().replace(microsecond=0)
        us_ex_date = datetime.strptime(str(us_date), "%Y-%m-%d").weekday() + 1
        hk_ex_date = datetime.strptime(str(hk_date), "%Y-%m-%d").weekday() + 1
        if 1 <= us_ex_date <= 5:
            now_us_time = int(f'{now_us_tmp_time.hour:02d}{now_us_tmp_time.minute:02d}{now_us_tmp_time.second:02d}')
            if now_us_time in Interval.between(USD_EXCHANGE_TIMESCOPE[0], USD_EXCHANGE_TIMESCOPE[1], closed=False):
                logger.info(f'{USD_STOCK_TIMEZONE} -> {us_date} -> {now_us_tmp_time} -> {result[-1].account} -> {result[-1].tag} -> {result[-1].value} -> {result[-1].currency}')
                account_data = AccountValue(id=generate_id(), timezone=USD_STOCK_TIMEZONE, date=us_date, time=now_us_tmp_time,
                                            account=result[-1].account, tag=result[-1].tag, asset=result[-1].value,
                                            currency=result[-1].currency)
                db.add_data(add_data=account_data, table_name='AccountValue')
        elif 1 <= hk_ex_date <= 5:
            now_hk_time = int(f'{now_hk_tmp_time.hour:02d}{now_hk_tmp_time.minute:02d}{now_hk_tmp_time.second:02d}')
            if now_hk_time in Interval.between(HKD_EXCHANGE_TIMESCOPE[0], 1200, closed=False) or now_hk_time in Interval.between(1300, HKD_EXCHANGE_TIMESCOPE[1], closed=False):
                logger.info(
                    f'{HKD_STOCK_TIMEZONE} -> {hk_date} -> {now_hk_tmp_time} -> {result[-1].account} -> {result[-1].tag} -> {result[-1].value} -> {result[-1].currency}')
                account_data = AccountValue(id=generate_id(), timezone=HKD_STOCK_TIMEZONE, date=hk_date, time=now_hk_tmp_time,
                                            account=result[-1].account, tag=result[-1].tag, asset=result[-1].value,
                                            currency=result[-1].currency)
                db.add_data(add_data=account_data, table_name='AccountValue')
        ib.sleep(180)
    ib.disconnect()

if __name__ == '__main__':
    poc_ib_account_api()