# flake8: noqa

import sys
if sys.version_info < (3, 6, 0):
    raise RuntimeError('ib_insync requires Python 3.6 or higher')

from eventkit import Event

from base.version import __version__, __version_info__
from base.objects import (
    SoftDollarTier, PriceIncrement, Execution, CommissionReport,
    BarList, BarDataList, RealTimeBarList, BarData, RealTimeBar,
    HistogramData, NewsProvider, DepthMktDataDescription,
    ScannerSubscription, ScanDataList,
    ExecutionFilter, PnL, PnLSingle, AccountValue, TickData,
    TickByTickAllLast, TickByTickBidAsk, TickByTickMidPoint,
    HistoricalTick, HistoricalTickBidAsk, HistoricalTickLast,
    TickAttrib, TickAttribBidAsk, TickAttribLast, FundamentalRatios,
    MktDepthData, DOMLevel, TradeLogEntry, FamilyCode, SmartComponent,
    PortfolioItem, Position, Fill, OptionComputation, OptionChain, Dividends,
    NewsArticle, HistoricalNews, NewsTick, NewsBulletin, ConnectionStats)
from base.contract import (
    Contract, Stock, CFD, TagValue, ComboLeg, DeltaNeutralContract, ContractDetails,
    ContractDescription, ScanData)
from base.ticker import Ticker
from base.ib_api import IbAPI
from base.client import Client
from base.wrapper import RequestError, Wrapper
from base.flexreport import FlexReport, FlexError
from base.ibcontroller import IBC, IBController, Watchdog
import source.base.util as util

__all__ = ['util', 'Event']
for _m in (objects, contract, ticker, ib_api,  # type: ignore
		client, wrapper, flexreport, ibcontroller):  # type: ignore
    __all__ += _m.__all__

# compatibility with old Object
import dataclasses
for obj in locals().copy().values():
    if dataclasses.is_dataclass(obj):
        obj.dict = util.dataclassAsDict
        obj.tuple = util.dataclassAsTuple
        obj.update = util.dataclassUpdate
        obj.nonDefaults = util.dataclassNonDefaults

del sys
del dataclasses
