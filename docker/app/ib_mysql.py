from sqlalchemy import Column, String, Integer, ForeignKey, func, DECIMAL, Float, DATE, Time
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from loguru import logger
from datetime import datetime
from bson import ObjectId
from config import MYSQL_ADDRESS, MYSQL_PORT, MYSQL_USERNAME, MYSQL_PASSWD, MYSQL_DB

Base = declarative_base()

@logger.catch(level='ERROR')
class UsStockLiveMktData(Base):
    # 表的名字:
    __tablename__ = 'UsStockLiveMktData'
    # 表的结构:
    id = Column(String(100), primary_key=True)
    timezone = Column(String(100))
    date = Column(DATE)
    time = Column(Time)
    stock = Column(String(100))
    lastPrice = Column(String(100))
    volume = Column(String(100))
    currency = Column(String(100))

@logger.catch(level='ERROR')
class HkStockLiveMktData(Base):
    # 表的名字:
    __tablename__ = 'HkStockLiveMktData'
    # 表的结构:
    id = Column(String(100), primary_key=True)
    timezone = Column(String(100))
    date = Column(DATE)
    time = Column(Time)
    stock = Column(String(100))
    lastPrice = Column(String(100))
    volume = Column(String(100))
    currency = Column(String(100))

@logger.catch(level='ERROR')
class AccountValue(Base):
    # 表的名字:
    __tablename__ = 'AccountValue'
    # 表的结构:
    id = Column(String(100), primary_key=True)
    timezone = Column(String(100))
    date = Column(DATE)
    time = Column(Time)
    account = Column(String(100))
    tag = Column(String(100))
    asset = Column(String(100))
    currency = Column(String(100))

@logger.catch(level='ERROR')
def poc_date(tz='Asia/Hong_Kong'):
    ori_date = datetime.now(tz=tz)
    spec_date = ori_date.date()
    spec_time = ori_date.time().replace(microsecond=0)
    return spec_date, spec_time

def generate_id():
    oid = ObjectId().__str__()
    return oid

class MysqlDb():
    @logger.catch(level='ERROR')
    def __init__(self):
        self.engine = create_engine(f'mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWD}@{MYSQL_ADDRESS}:{MYSQL_PORT}/{MYSQL_DB}')
        DbSession = sessionmaker(bind=self.engine)
        self.session = DbSession()

    @logger.catch(level='ERROR')
    def add_data(self, add_data, table_name):
        if not self.engine.dialect.has_table(self.engine, table_name):  # If table don't exist, Create.
            Base.metadata.create_all(self.engine)
        self.session.add(add_data)
        self.session.commit()
        self.session.close()
