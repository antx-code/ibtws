from dataclasses import dataclass, field
from typing import List, NamedTuple, Optional
from loguru import logger
import source.base.util as util

logger.add(sink='logs/base_contract.log',
           level='ERROR',
           format='{time:YYYY-MM-DD  :mm:ss} - {level} - {file} - {line} - {message}',
           enqueue=True,
           backtrace=True,
           diagnose=True,
           rotation='00:00',
           retention='7 days')

__all__ = (
    'Contract Stock CFD '
    'TagValue ComboLeg DeltaNeutralContract ContractDetails '
    'ContractDescription ScanData').split()


@dataclass
class Contract:

    secType: str = ''
    conId: int = 0
    symbol: str = ''
    lastTradeDateOrContractMonth: str = ''
    strike: float = 0.0
    right: str = ''
    multiplier: str = ''
    exchange: str = ''
    primaryExchange: str = ''
    currency: str = ''
    localSymbol: str = ''
    tradingClass: str = ''
    includeExpired: bool = False
    secIdType: str = ''
    secId: str = ''
    comboLegsDescrip: str = ''
    comboLegs: List['ComboLeg'] = field(default_factory=list)
    deltaNeutralContract: Optional['DeltaNeutralContract'] = None

    @staticmethod
    def create(**kwargs) -> 'Contract':
        secType = kwargs.get('secType', '')
        cls = {
            '': Contract,
            'STK': Stock,
            'CFD': CFD,
            'NEWS': Contract
        }.get(secType, Contract)
        if cls is not Contract:
            kwargs.pop('secType', '')
        return cls(**kwargs)

    def isHashable(self) -> bool:
        return bool(
            self.conId and self.conId != 28812380
            and self.secType != 'BAG')

    def __eq__(self, other):
        return (
            isinstance(other, Contract)
            and (
                    self.conId and self.conId == other.conId
                    or util.dataclassAsDict(self) == util.dataclassAsDict(other)))

    def __hash__(self):
        if not self.isHashable():
            raise ValueError(f'Contract {self} can\'t be hashed')
        if self.secType == 'CONTFUT':
            h = -self.conId
        else:
            h = self.conId
        return h

    def __repr__(self):
        attrs = util.dataclassNonDefaults(self)
        if self.__class__ is not Contract:
            attrs.pop('secType', '')
        clsName = self.__class__.__qualname__
        kwargs = ', '.join(f'{k}={v!r}' for k, v in attrs.items())
        return f'{clsName}({kwargs})'

    __str__ = __repr__


class Stock(Contract):

    def __init__(
            self, symbol: str = '', exchange: str = '', currency: str = '',
            **kwargs):
        Contract.__init__(
            self, secType='STK', symbol=symbol,
            exchange=exchange, currency=currency, **kwargs)


class CFD(Contract):

    def __init__(self, symbol: str = '', exchange: str = '', currency: str = '', **kwargs):
        Contract.__init__(
            self, 'CFD', symbol=symbol,
            exchange=exchange, currency=currency, **kwargs)


class TagValue(NamedTuple):
    tag: str
    value: str


@dataclass
class ComboLeg:
    conId: int = 0
    ratio: int = 0
    action: str = ''
    exchange: str = ''
    openClose: int = 0
    shortSaleSlot: int = 0
    designatedLocation: str = ''
    exemptCode: int = -1


@dataclass
class DeltaNeutralContract:
    conId: int = 0
    delta: float = 0.0
    price: float = 0.0


@dataclass
class ContractDetails:
    contract: Optional[Contract] = None
    marketName: str = ''
    minTick: float = 0.0
    orderTypes: str = ''
    validExchanges: str = ''
    priceMagnifier: int = 0
    underConId: int = 0
    longName: str = ''
    contractMonth: str = ''
    industry: str = ''
    category: str = ''
    subcategory: str = ''
    timeZoneId: str = ''
    tradingHours: str = ''
    liquidHours: str = ''
    evRule: str = ''
    evMultiplier: int = 0
    mdSizeMultiplier: int = 0
    aggGroup: int = 0
    underSymbol: str = ''
    underSecType: str = ''
    marketRuleIds: str = ''
    secIdList: List[TagValue] = field(default_factory=list)
    realExpirationDate: str = ''
    lastTradeTime: str = ''
    stockType: str = ''
    cusip: str = ''
    ratings: str = ''
    descAppend: str = ''
    bondType: str = ''
    couponType: str = ''
    callable: bool = False
    putable: bool = False
    coupon: int = 0
    convertible: bool = False
    maturity: str = ''
    issueDate: str = ''
    nextOptionDate: str = ''
    nextOptionType: str = ''
    nextOptionPartial: bool = False
    notes: str = ''


@dataclass
class ContractDescription:
    contract: Optional[Contract] = None
    derivativeSecTypes: List[str] = field(default_factory=list)


@dataclass
class ScanData:
    rank: int
    contractDetails: ContractDetails
    distance: str
    benchmark: str
    projection: str
    legsStr: str
