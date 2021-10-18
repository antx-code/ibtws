from dataclasses import dataclass, field
from datetime import datetime
from loguru import logger
from typing import ClassVar, List, Optional, Union
from eventkit import Event, Op
from base.contract import Contract
from base.objects import (
    BarList, DOMLevel, Dividends, FundamentalRatios, MktDepthData,
    OptionComputation, TickByTickAllLast, TickByTickBidAsk, TickByTickMidPoint,
    TickData)
from source.base.util import dataclassRepr, isNan

logger.add(sink='logs/base_tricker.log',
           level='ERROR',
           format='{time:YYYY-MM-DD  :mm:ss} - {level} - {file} - {line} - {message}',
           enqueue=True,
           backtrace=True,
           diagnose=True,
           rotation='00:00',
           retention='7 days')

__all__ = ['Ticker']

nan = float('nan')


@dataclass
class Ticker:
    events: ClassVar = ('updateEvent',)

    contract: Optional[Contract] = None
    time: Optional[datetime] = None
    marketDataType: int = 1
    bid: float = nan
    bidSize: float = nan
    ask: float = nan
    askSize: float = nan
    last: float = nan
    lastSize: float = nan
    prevBid: float = nan
    prevBidSize: float = nan
    prevAsk: float = nan
    prevAskSize: float = nan
    prevLast: float = nan
    prevLastSize: float = nan
    volume: float = nan
    open: float = nan
    high: float = nan
    low: float = nan
    close: float = nan
    vwap: float = nan
    low13week: float = nan
    high13week: float = nan
    low26week: float = nan
    high26week: float = nan
    low52week: float = nan
    high52week: float = nan
    bidYield: float = nan
    askYield: float = nan
    lastYield: float = nan
    markPrice: float = nan
    halted: float = nan
    rtHistVolatility: float = nan
    rtVolume: float = nan
    rtTradeVolume: float = nan
    rtTime: Optional[datetime] = None
    avVolume: float = nan
    tradeCount: float = nan
    tradeRate: float = nan
    volumeRate: float = nan
    shortableShares: float = nan
    indexFuturePremium: float = nan
    futuresOpenInterest: float = nan
    putOpenInterest: float = nan
    callOpenInterest: float = nan
    putVolume: float = nan
    callVolume: float = nan
    avOptionVolume: float = nan
    histVolatility: float = nan
    impliedVolatility: float = nan
    dividends: Optional[Dividends] = None
    fundamentalRatios: Optional[FundamentalRatios] = None
    ticks: List[TickData] = field(default_factory=list)
    tickByTicks: List[Union[
        TickByTickAllLast, TickByTickBidAsk, TickByTickMidPoint]] = \
        field(default_factory=list)
    domBids: List[DOMLevel] = field(default_factory=list)
    domAsks: List[DOMLevel] = field(default_factory=list)
    domTicks: List[MktDepthData] = field(default_factory=list)
    bidGreeks: Optional[OptionComputation] = None
    askGreeks: Optional[OptionComputation] = None
    lastGreeks: Optional[OptionComputation] = None
    modelGreeks: Optional[OptionComputation] = None
    auctionVolume: float = nan
    auctionPrice: float = nan
    auctionImbalance: float = nan

    def __post_init__(self):
        self.updateEvent = TickerUpdateEvent('updateEvent')

    def __eq__(self, other):
        return self is other

    def __hash__(self):
        return id(self)

    __repr__ = dataclassRepr
    __str__ = dataclassRepr

    def hasBidAsk(self) -> bool:
        return (
            self.bid != -1 and not isNan(self.bid) and self.bidSize > 0
            and self.ask != -1 and not isNan(self.ask) and self.askSize > 0)

    def midpoint(self) -> float:
        return (self.bid + self.ask) * 0.5 if self.hasBidAsk() else nan

    def marketPrice(self) -> float:
        price = self.last if (
            self.hasBidAsk() and self.bid <= self.last <= self.ask) else \
            self.midpoint()
        if isNan(price):
            price = self.close
        return price


class TickerUpdateEvent(Event):
    __slots__ = ()

    def trades(self) -> "Tickfilter":
        """Emit trade ticks."""
        return Tickfilter((4, 5, 48, 68, 71), self)

    def bids(self) -> "Tickfilter":
        """Emit bid ticks."""
        return Tickfilter((0, 1, 66, 69), self)

    def asks(self) -> "Tickfilter":
        """Emit ask ticks."""
        return Tickfilter((2, 3, 67, 70), self)

    def bidasks(self) -> "Tickfilter":
        """Emit bid and ask ticks."""
        return Tickfilter((0, 1, 66, 69, 2, 3, 67, 70), self)

    def midpoints(self) -> "Tickfilter":
        """Emit midpoint ticks."""
        return Midpoints((), self)


class Tickfilter(Op):
    __slots__ = ('_tickTypes',)

    def __init__(self, tickTypes, source=None):
        Op.__init__(self, source)
        self._tickTypes = set(tickTypes)

    def on_source(self, ticker):
        for t in ticker.ticks:
            if t.tickType in self._tickTypes:
                self.emit(t.time, t.price, t.size)

    def timebars(self, timer: Event) -> "TimeBars":
        return TimeBars(timer, self)

    def tickbars(self, count: int) -> "TickBars":
        return TickBars(count, self)


class Midpoints(Tickfilter):
    __slots__ = ()

    def on_source(self, ticker):
        if ticker.ticks:
            self.emit(ticker.time, ticker.midpoint(), 0)


@dataclass
class Bar:
    time: Optional[datetime]
    open: float = nan
    high: float = nan
    low: float = nan
    close: float = nan
    volume: int = 0
    count: int = 0


class TimeBars(Op):
    __slots__ = ('_timer', 'bars',)
    __doc__ = Tickfilter.timebars.__doc__

    def __init__(self, timer, source=None):
        Op.__init__(self, source)
        self._timer = timer
        self._timer.connect(self._on_timer, None, self._on_timer_done)
        self.bars: BarList = BarList()

    def on_source(self, time, price, size):
        if not self.bars:
            return
        bar = self.bars[-1]
        if isNan(bar.open):
            bar.open = bar.high = bar.low = price
        bar.high = max(bar.high, price)
        bar.low = min(bar.low, price)
        bar.close = price
        bar.volume += size
        bar.count += 1
        self.bars.updateEvent.emit(self.bars, False)

    def _on_timer(self, time):
        if self.bars:
            bar = self.bars[-1]
            if isNan(bar.close) and len(self.bars) > 1:
                bar.open = bar.high = bar.low = bar.close = \
                    self.bars[-2].close
            self.bars.updateEvent.emit(self.bars, True)
            self.emit(bar)
        self.bars.append(Bar(time))

    def _on_timer_done(self, timer):
        self._timer = None
        self.set_done()


class TickBars(Op):
    __slots__ = ('_count', 'bars')
    __doc__ = Tickfilter.tickbars.__doc__

    def __init__(self, count, source=None):
        Op.__init__(self, source)
        self._count = count
        self.bars: BarList = BarList()

    def on_source(self, time, price, size):
        if not self.bars or self.bars[-1].count == self._count:
            bar = Bar(time, price, price, price, price, size, 1)
            self.bars.append(bar)
        else:
            bar = self.bars[-1]
            bar.high = max(bar.high, price)
            bar.low = min(bar.low, price)
            bar.close = price
            bar.volume += size
            bar.count += 1
        if bar.count == self._count:
            self.bars.updateEvent.emit(self.bars, True)
            self.emit(self.bars)
