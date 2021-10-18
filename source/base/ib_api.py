import asyncio
import datetime
import logging
import time
from loguru import logger
from typing import Awaitable, Iterator, List, Optional, Union
from eventkit import Event
import source.base.util as util
from base.client import Client
from base.contract import Contract, ContractDescription, ContractDetails
from base.objects import (
    AccountValue, BarDataList, DepthMktDataDescription, Execution,
    ExecutionFilter, Fill, HistogramData, HistoricalNews, NewsArticle,
    NewsBulletin, NewsProvider, NewsTick, OptionChain, OptionComputation,
    PnL, PnLSingle, PortfolioItem, Position, PriceIncrement,
    RealTimeBarList, ScanDataList, ScannerSubscription, TagValue)
from base.ticker import Ticker
from base.wrapper import Wrapper

logger.add(sink='logs/ib_api.log',
           level='ERROR',
           format='{time:YYYY-MM-DD  :mm:ss} - {level} - {file} - {line} - {message}',
           enqueue=True,
           backtrace=True,
           diagnose=True,
           rotation='00:00',
           retention='7 days')

__all__ = ['IbAPI']

class IbAPI:
    events = (
        'connectedEvent', 'disconnectedEvent', 'updateEvent',
        'pendingTickersEvent', 'barUpdateEvent',
        'newOrderEvent', 'orderModifyEvent', 'cancelOrderEvent',
        'openOrderEvent', 'orderStatusEvent',
        'execDetailsEvent', 'commissionReportEvent',
        'updatePortfolioEvent', 'positionEvent', 'accountValueEvent',
        'accountSummaryEvent', 'pnlEvent', 'pnlSingleEvent',
        'scannerDataEvent', 'tickNewsEvent', 'newsBulletinEvent',
        'errorEvent', 'timeoutEvent')

    RequestTimeout: float = 0
    RaiseRequestErrors: bool = False
    MaxSyncedSubAccounts: int = 50
    TimezoneTWS = None

    def __init__(self):
        self._createEvents()
        self.wrapper = Wrapper(self)
        self.client = Client(self.wrapper)
        self.errorEvent += self._onError
        self.client.apiEnd += self.disconnectedEvent
        self._logger = logging.getLogger('ib_insync.ib')

    def _createEvents(self):
        self.connectedEvent = Event('connectedEvent')
        self.disconnectedEvent = Event('disconnectedEvent')
        self.updateEvent = Event('updateEvent')
        self.pendingTickersEvent = Event('pendingTickersEvent')
        self.barUpdateEvent = Event('barUpdateEvent')
        self.newOrderEvent = Event('newOrderEvent')
        self.orderModifyEvent = Event('orderModifyEvent')
        self.cancelOrderEvent = Event('cancelOrderEvent')
        self.openOrderEvent = Event('openOrderEvent')
        self.orderStatusEvent = Event('orderStatusEvent')
        self.execDetailsEvent = Event('execDetailsEvent')
        self.commissionReportEvent = Event('commissionReportEvent')
        self.updatePortfolioEvent = Event('updatePortfolioEvent')
        self.positionEvent = Event('positionEvent')
        self.accountValueEvent = Event('accountValueEvent')
        self.accountSummaryEvent = Event('accountSummaryEvent')
        self.pnlEvent = Event('pnlEvent')
        self.pnlSingleEvent = Event('pnlSingleEvent')
        self.scannerDataEvent = Event('scannerDataEvent')
        self.tickNewsEvent = Event('tickNewsEvent')
        self.newsBulletinEvent = Event('newsBulletinEvent')
        self.errorEvent = Event('errorEvent')
        self.timeoutEvent = Event('timeoutEvent')

    def __del__(self):
        self.disconnect()

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.disconnect()

    def __repr__(self):
        conn = (f'connected to {self.client.host}:'
                f'{self.client.port} clientId={self.client.clientId}' if
                self.client.isConnected() else 'not connected')
        return f'<{self.__class__.__qualname__} {conn}>'

    def connect(
            self, host: str = '127.0.0.1', port: int = 7497, clientId: int = 1,
            timeout: float = 4, readonly: bool = False, account: str = ''):
        return self._run(self.connectAsync(
            host, port, clientId, timeout, readonly, account))

    def disconnect(self):
        if not self.client.isConnected():
            return
        stats = self.client.connectionStats()
        self._logger.info(
            f'Disconnecting from {self.client.host}:{self.client.port}, '
            f'{util.formatSI(stats.numBytesSent)}B sent '
            f'in {stats.numMsgSent} messages, '
            f'{util.formatSI(stats.numBytesRecv)}B received '
            f'in {stats.numMsgRecv} messages, '
            f'session time {util.formatSI(stats.duration)}s.')
        self.client.disconnect()
        self.disconnectedEvent.emit()

    def isConnected(self) -> bool:
        return self.client.isReady()

    def _onError(self, reqId, errorCode, errorString, contract):
        if errorCode == 1102:
            asyncio.ensure_future(self.reqAccountSummaryAsync())

    run = staticmethod(util.run)
    schedule = staticmethod(util.schedule)
    sleep = staticmethod(util.sleep)
    timeRange = staticmethod(util.timeRange)
    timeRangeAsync = staticmethod(util.timeRangeAsync)
    waitUntil = staticmethod(util.waitUntil)

    def _run(self, *awaitables: Awaitable):
        return util.run(*awaitables, timeout=self.RequestTimeout)

    def waitOnUpdate(self, timeout: float = 0) -> bool:
        if timeout:
            try:
                util.run(asyncio.wait_for(self.updateEvent, timeout))
            except asyncio.TimeoutError:
                return False
        else:
            util.run(self.updateEvent)
        return True

    def loopUntil(
            self, condition=None, timeout: float = 0) -> Iterator[object]:
        endTime = time.time() + timeout
        while True:
            test = condition and condition()
            if test:
                yield test
                return
            elif timeout and time.time() > endTime:
                yield False
                return
            else:
                yield test
            self.waitOnUpdate(endTime - time.time() if timeout else 0)

    def setTimeout(self, timeout: float = 60):
        self.wrapper.setTimeout(timeout)

    def managedAccounts(self) -> List[str]:
        return list(self.wrapper.accounts)

    def accountValues(self, account: str = '') -> List[AccountValue]:
        if account:
            return [v for v in self.wrapper.accountValues.values()
                    if v.account == account]
        else:
            return list(self.wrapper.accountValues.values())

    def accountSummary(self, account: str = '') -> List[AccountValue]:
        return self._run(self.accountSummaryAsync(account))

    def portfolio(self) -> List[PortfolioItem]:
        account = self.wrapper.accounts[0]
        return [v for v in self.wrapper.portfolio[account].values()]

    def positions(self, account: str = '') -> List[Position]:
        if account:
            return list(self.wrapper.positions[account].values())
        else:
            return [v for d in self.wrapper.positions.values()
                    for v in d.values()]

    def pnl(self, account='', modelCode='') -> List[PnL]:
        return [v for v in self.wrapper.reqId2PnL.values() if
                (not account or v.account == account)
                and (not modelCode or v.modelCode == modelCode)]

    def pnlSingle(
            self, account: str = '', modelCode: str = '',
            conId: int = 0) -> List[PnLSingle]:
        return [v for v in self.wrapper.reqId2PnlSingle.values() if
                (not account or v.account == account)
                and (not modelCode or v.modelCode == modelCode)
                and (not conId or v.conId == conId)]

    def fills(self) -> List[Fill]:
        return list(self.wrapper.fills.values())

    def executions(self) -> List[Execution]:
        return list(fill.execution for fill in self.wrapper.fills.values())

    def ticker(self, contract: Contract) -> Ticker:
        return self.wrapper.tickers.get(id(contract))

    def tickers(self) -> List[Ticker]:
        return list(self.wrapper.tickers.values())

    def pendingTickers(self) -> List[Ticker]:
        return list(self.wrapper.pendingTickers)

    def realtimeBars(self) -> List[Union[BarDataList, RealTimeBarList]]:
        return list(self.wrapper.reqId2Subscriber.values())

    def newsTicks(self) -> List[NewsTick]:
        return self.wrapper.newsTicks

    def newsBulletins(self) -> List[NewsBulletin]:
        return list(self.wrapper.msgId2NewsBulletin.values())

    def reqTickers(
            self, *contracts: Contract,
            regulatorySnapshot: bool = False) -> List[Ticker]:
        return self._run(
            self.reqTickersAsync(
                *contracts, regulatorySnapshot=regulatorySnapshot))

    def qualifyContracts(self, *contracts: Contract) -> List[Contract]:

        return self._run(self.qualifyContractsAsync(*contracts))

    def reqGlobalCancel(self):
        self.client.reqGlobalCancel()
        self._logger.info('reqGlobalCancel')

    def reqCurrentTime(self) -> datetime.datetime:
        return self._run(self.reqCurrentTimeAsync())

    def reqAccountUpdates(self, account: str = ''):
        self._run(self.reqAccountUpdatesAsync(account))

    def reqAccountUpdatesMulti(self, account: str = '', modelCode: str = ''):
        self._run(self.reqAccountUpdatesMultiAsync(account, modelCode))

    def reqAccountSummary(self):
        self._run(self.reqAccountSummaryAsync())

    def reqAutoOpenOrders(self, autoBind: bool = True):
        self.client.reqAutoOpenOrders(autoBind)

    def reqExecutions(self, execFilter: ExecutionFilter = None) -> List[Fill]:
        return self._run(self.reqExecutionsAsync(execFilter))

    def reqPositions(self) -> List[Position]:
        return self._run(self.reqPositionsAsync())

    def reqPnL(self, account: str, modelCode: str = '') -> PnL:
        key = (account, modelCode)
        assert key not in self.wrapper.pnlKey2ReqId
        reqId = self.client.getReqId()
        self.wrapper.pnlKey2ReqId[key] = reqId
        pnl = PnL(account, modelCode)
        self.wrapper.reqId2PnL[reqId] = pnl
        self.client.reqPnL(reqId, account, modelCode)
        return pnl

    def cancelPnL(self, account, modelCode: str = ''):
        key = (account, modelCode)
        reqId = self.wrapper.pnlKey2ReqId.pop(key, None)
        if reqId:
            self.client.cancelPnL(reqId)
            self.wrapper.reqId2PnL.pop(reqId, None)
        else:
            self._logger.error(
                'cancelPnL: No subscription for '
                f'account {account}, modelCode {modelCode}')

    def reqPnLSingle(self, account: str, modelCode: str, conId: int) -> PnLSingle:
        key = (account, modelCode, conId)
        assert key not in self.wrapper.pnlSingleKey2ReqId
        reqId = self.client.getReqId()
        self.wrapper.pnlSingleKey2ReqId[key] = reqId
        pnlSingle = PnLSingle(account, modelCode, conId)
        self.wrapper.reqId2PnlSingle[reqId] = pnlSingle
        self.client.reqPnLSingle(reqId, account, modelCode, conId)
        return pnlSingle

    def cancelPnLSingle(self, account: str, modelCode: str, conId: int):
        key = (account, modelCode, conId)
        reqId = self.wrapper.pnlSingleKey2ReqId.pop(key, None)
        if reqId:
            self.client.cancelPnLSingle(reqId)
            self.wrapper.reqId2PnlSingle.pop(reqId, None)
        else:
            self._logger.error(
                'cancelPnLSingle: No subscription for '
                f'account {account}, modelCode {modelCode}, conId {conId}')

    def reqContractDetails(self, contract: Contract) -> List[ContractDetails]:
        return self._run(self.reqContractDetailsAsync(contract))

    def reqMatchingSymbols(self, pattern: str) -> List[ContractDescription]:
        return self._run(self.reqMatchingSymbolsAsync(pattern))

    def reqMarketRule(self, marketRuleId: int) -> PriceIncrement:
        return self._run(self.reqMarketRuleAsync(marketRuleId))

    def reqRealTimeBars(
            self, contract: Contract, barSize: int,
            whatToShow: str, useRTH: bool,
            realTimeBarsOptions: List[TagValue] = []) -> RealTimeBarList:
        reqId = self.client.getReqId()
        bars = RealTimeBarList()
        bars.reqId = reqId
        bars.contract = contract
        bars.barSize = barSize
        bars.whatToShow = whatToShow
        bars.useRTH = useRTH
        bars.realTimeBarsOptions = realTimeBarsOptions or []
        self.wrapper.startSubscription(reqId, bars, contract)
        self.client.reqRealTimeBars(
            reqId, contract, barSize, whatToShow, useRTH, realTimeBarsOptions)
        return bars

    def cancelRealTimeBars(self, bars: RealTimeBarList):
        self.client.cancelRealTimeBars(bars.reqId)
        self.wrapper.endSubscription(bars)

    def reqHistoricalData(
            self, contract: Contract,
            endDateTime: Union[datetime.datetime, datetime.date, str, None],
            durationStr: str, barSizeSetting: str, whatToShow: str,
            useRTH: bool, formatDate: int = 1, keepUpToDate: bool = False,
            chartOptions: List[TagValue] = [],
            timeout: float = 60) -> BarDataList:
        return self._run(
            self.reqHistoricalDataAsync(
                contract, endDateTime, durationStr, barSizeSetting, whatToShow,
                useRTH, formatDate, keepUpToDate, chartOptions, timeout))

    def cancelHistoricalData(self, bars: BarDataList):
        self.client.cancelHistoricalData(bars.reqId)
        self.wrapper.endSubscription(bars)

    def reqHistoricalTicks(
            self, contract: Contract,
            startDateTime: Union[str, datetime.date],
            endDateTime: Union[str, datetime.date],
            numberOfTicks: int, whatToShow: str, useRth: bool,
            ignoreSize: bool = False,
            miscOptions: List[TagValue] = []) -> List:
        return self._run(
            self.reqHistoricalTicksAsync(
                contract, startDateTime, endDateTime, numberOfTicks,
                whatToShow, useRth, ignoreSize, miscOptions))

    def reqMarketDataType(self, marketDataType: int):
        self.client.reqMarketDataType(marketDataType)

    def reqHeadTimeStamp(
            self, contract: Contract, whatToShow: str,
            useRTH: bool, formatDate: int = 1) -> datetime.datetime:
        return self._run(
            self.reqHeadTimeStampAsync(
                contract, whatToShow, useRTH, formatDate))

    def reqMktData(
            self, contract: Contract, genericTickList: str = '',
            snapshot: bool = False, regulatorySnapshot: bool = False,
            mktDataOptions: List[TagValue] = None) -> Ticker:
        reqId = self.client.getReqId()
        ticker = self.wrapper.startTicker(reqId, contract, 'mktData')
        self.client.reqMktData(
            reqId, contract, genericTickList, snapshot,
            regulatorySnapshot, mktDataOptions)
        return ticker

    def cancelMktData(self, contract: Contract):
        ticker = self.ticker(contract)
        reqId = self.wrapper.endTicker(ticker, 'mktData')
        self.wrapper.reqId2MarketDataType.pop(reqId, None)
        if reqId:
            self.client.cancelMktData(reqId)
        else:
            self._logger.error(
                'cancelMktData: ' f'No reqId found for contract {contract}')

    def reqTickByTickData(
            self, contract: Contract, tickType: str,
            numberOfTicks: int = 0, ignoreSize: bool = False) -> Ticker:
        reqId = self.client.getReqId()
        ticker = self.wrapper.startTicker(reqId, contract, tickType)
        self.client.reqTickByTickData(
            reqId, contract, tickType, numberOfTicks, ignoreSize)
        return ticker

    def cancelTickByTickData(self, contract: Contract, tickType: str):
        ticker = self.ticker(contract)
        reqId = self.wrapper.endTicker(ticker, tickType)
        if reqId:
            self.client.cancelTickByTickData(reqId)
        else:
            self._logger.error(
                f'cancelMktData: No reqId found for contract {contract}')

    def reqMktDepthExchanges(self) -> List[DepthMktDataDescription]:
        return self._run(self.reqMktDepthExchangesAsync())

    def reqMktDepth(
            self, contract: Contract, numRows: int = 5,
            isSmartDepth: bool = False, mktDepthOptions=None) -> Ticker:
        reqId = self.client.getReqId()
        ticker = self.wrapper.startTicker(reqId, contract, 'mktDepth')
        self.client.reqMktDepth(
            reqId, contract, numRows, isSmartDepth, mktDepthOptions)
        return ticker

    def cancelMktDepth(self, contract: Contract, isSmartDepth=False):
        ticker = self.ticker(contract)
        ticker.domBids.clear()
        ticker.domAsks.clear()
        reqId = self.wrapper.endTicker(ticker, 'mktDepth')
        if reqId:
            self.client.cancelMktDepth(reqId, isSmartDepth)
        else:
            self._logger.error(
                f'cancelMktDepth: No reqId found for contract {contract}')

    def reqHistogramData(
            self, contract: Contract,
            useRTH: bool, period: str) -> List[HistogramData]:
        return self._run(
            self.reqHistogramDataAsync(contract, useRTH, period))

    def reqFundamentalData(
            self, contract: Contract, reportType: str,
            fundamentalDataOptions: List[TagValue] = []) -> str:
        return self._run(
            self.reqFundamentalDataAsync(
                contract, reportType, fundamentalDataOptions))

    def reqScannerData(
            self, subscription: ScannerSubscription,
            scannerSubscriptionOptions: List[TagValue] = [],
            scannerSubscriptionFilterOptions: List[TagValue] = []) -> \
            ScanDataList:
        return self._run(self.reqScannerDataAsync(
                subscription, scannerSubscriptionOptions,
                scannerSubscriptionFilterOptions))

    def reqScannerSubscription(
            self, subscription: ScannerSubscription,
            scannerSubscriptionOptions: List[TagValue] = [],
            scannerSubscriptionFilterOptions:
            List[TagValue] = []) -> ScanDataList:
        reqId = self.client.getReqId()
        dataList = ScanDataList()
        dataList.reqId = reqId
        dataList.subscription = subscription
        dataList.scannerSubscriptionOptions = scannerSubscriptionOptions or []
        dataList.scannerSubscriptionFilterOptions = \
            scannerSubscriptionFilterOptions or []
        self.wrapper.startSubscription(reqId, dataList)
        self.client.reqScannerSubscription(
            reqId, subscription, scannerSubscriptionOptions,
            scannerSubscriptionFilterOptions)
        return dataList

    def cancelScannerSubscription(self, dataList: ScanDataList):
        self.client.cancelScannerSubscription(dataList.reqId)
        self.wrapper.endSubscription(dataList)

    def reqScannerParameters(self) -> str:
        return self._run(self.reqScannerParametersAsync())

    def calculateImpliedVolatility(
            self, contract: Contract,
            optionPrice: float, underPrice: float,
            implVolOptions: List[TagValue] = []) -> OptionComputation:
        return self._run(
            self.calculateImpliedVolatilityAsync(
                contract, optionPrice, underPrice, implVolOptions))

    def calculateOptionPrice(
            self, contract: Contract,
            volatility: float, underPrice: float,
            optPrcOptions=None) -> OptionComputation:
        return self._run(
            self.calculateOptionPriceAsync(
                contract, volatility, underPrice, optPrcOptions))

    def reqSecDefOptParams(
            self, underlyingSymbol: str,
            futFopExchange: str, underlyingSecType: str,
            underlyingConId: int) -> List[OptionChain]:
        return self._run(
            self.reqSecDefOptParamsAsync(
                underlyingSymbol, futFopExchange,
                underlyingSecType, underlyingConId))

    def exerciseOptions(
            self, contract: Contract, exerciseAction: int,
            exerciseQuantity: int, account: str, override: int):
        reqId = self.client.getReqId()
        self.client.exerciseOptions(
            reqId, contract, exerciseAction, exerciseQuantity,
            account, override)

    def reqNewsProviders(self) -> List[NewsProvider]:
        return self._run(self.reqNewsProvidersAsync())

    def reqNewsArticle(
            self, providerCode: str, articleId: str,
            newsArticleOptions: List[TagValue] = None) -> NewsArticle:
        return self._run(
            self.reqNewsArticleAsync(
                providerCode, articleId, newsArticleOptions))

    def reqHistoricalNews(
            self, conId: int, providerCodes: str,
            startDateTime: Union[str, datetime.date],
            endDateTime: Union[str, datetime.date],
            totalResults: int,
            historicalNewsOptions: List[TagValue] = None) -> HistoricalNews:
        return self._run(
            self.reqHistoricalNewsAsync(
                conId, providerCodes, startDateTime, endDateTime,
                totalResults, historicalNewsOptions))

    def reqNewsBulletins(self, allMessages: bool):
        self.client.reqNewsBulletins(allMessages)

    def cancelNewsBulletins(self):
        self.client.cancelNewsBulletins()

    def requestFA(self, faDataType: int):
        return self._run(self.requestFAAsync(faDataType))

    def replaceFA(self, faDataType: int, xml: str):
        self.client.replaceFA(faDataType, xml)

    # now entering the parallel async universe

    async def connectAsync(
            self, host: str = '127.0.0.1', port: int = 7497,
            clientId: int = 1, timeout: float = 4, readonly: bool = False,
            account: str = ''):

        if self.isConnected():
            self._logger.warning('Already connected')
            return self
        self.wrapper.clientId = clientId

        try:
            # establish API connection
            await self.client.connectAsync(
                host, port, clientId, timeout or None)

            # autobind manual orders
            if clientId == 0:
                self.reqAutoOpenOrders(True)

            accounts = self.client.getAccounts()
            if not account and len(accounts) == 1:
                account = accounts[0]

            # prepare initializing  requests
            reqs = {}  # name -> request
            reqs['positions'] = self.reqPositionsAsync()
            if account:
                reqs['account updates'] =self.reqAccountUpdatesAsync(account)
            if len(accounts) <= self.MaxSyncedSubAccounts:
                for acc in accounts:
                    reqs[f'account updates for {acc}'] = \
                        self.reqAccountUpdatesMultiAsync(acc)

            # run initializing requests concurrently and log if any times out
            tasks = [
                asyncio.wait_for(req, timeout or None)
                for req in reqs.values()]
            resps = await asyncio.gather(*tasks, return_exceptions=True)
            for name, resp in zip(reqs, resps):
                if isinstance(resp, asyncio.TimeoutError):
                    self._logger.error(f'{name} request timed out')

            # the request for executions must come after all orders are in
            await asyncio.wait_for(self.reqExecutionsAsync(), timeout or None)

            self._logger.info('Synchronization complete')
            self.connectedEvent.emit()
        except Exception:
            self.disconnect()
            raise
        return self

    async def qualifyContractsAsync(self, *contracts: Contract) -> \
            List[Contract]:
        detailsLists = await asyncio.gather(
            *(self.reqContractDetailsAsync(c) for c in contracts))
        result = []
        for contract, detailsList in zip(contracts, detailsLists):
            if not detailsList:
                self._logger.error(f'Unknown contract: {contract}')
            elif len(detailsList) > 1:
                possibles = [details.contract for details in detailsList]
                self._logger.error(
                    f'Ambiguous contract: {contract}, '
                    f'possibles are {possibles}')
            else:
                c = detailsList[0].contract
                expiry = c.lastTradeDateOrContractMonth
                if expiry:
                    # remove time and timezone part as it will cause problems
                    expiry = expiry.split()[0]
                    c.lastTradeDateOrContractMonth = expiry
                if contract.exchange == 'SMART':
                    # overwriting 'SMART' exchange can create invalid contract
                    c.exchange = contract.exchange
                util.dataclassUpdate(contract, c)
                result.append(contract)
        return result

    async def reqTickersAsync(
            self, *contracts: Contract, regulatorySnapshot: bool = False) -> \
            List[Ticker]:
        futures = []
        tickers = []
        reqIds = []
        for contract in contracts:
            reqId = self.client.getReqId()
            reqIds.append(reqId)
            future = self.wrapper.startReq(reqId, contract)
            futures.append(future)
            ticker = self.wrapper.startTicker(reqId, contract, 'snapshot')
            tickers.append(ticker)
            self.client.reqMktData(
                reqId, contract, '', True, regulatorySnapshot, [])
        await asyncio.gather(*futures)
        for ticker in tickers:
            self.wrapper.endTicker(ticker, 'snapshot')
        for reqId in reqIds:
            self.wrapper.reqId2MarketDataType.pop(reqId, None)
        return tickers

    def reqCurrentTimeAsync(self) -> Awaitable[datetime.datetime]:
        future = self.wrapper.startReq('currentTime')
        self.client.reqCurrentTime()
        return future

    def reqAccountUpdatesAsync(self, account: str) -> Awaitable[None]:
        future = self.wrapper.startReq('accountValues')
        self.client.reqAccountUpdates(True, account)
        return future

    def reqAccountUpdatesMultiAsync(
            self, account: str, modelCode: str = '') -> Awaitable[None]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId)
        self.client.reqAccountUpdatesMulti(reqId, account, modelCode, False)
        return future

    async def accountSummaryAsync(self, account: str = '') -> \
            List[AccountValue]:
        if not self.wrapper.acctSummary:
            # loaded on demand since it takes ca. 250 ms
            await self.reqAccountSummaryAsync()
        if account:
            return [v for v in self.wrapper.acctSummary.values()
                    if v.account == account]
        else:
            return list(self.wrapper.acctSummary.values())

    def reqAccountSummaryAsync(self) -> Awaitable[None]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId)
        tags = (
            'AccountType,NetLiquidation,TotalCashValue,SettledCash,'
            'AccruedCash,BuyingPower,EquityWithLoanValue,'
            'PreviousEquityWithLoanValue,GrossPositionValue,ReqTEquity,'
            'ReqTMargin,SMA,InitMarginReq,MaintMarginReq,AvailableFunds,'
            'ExcessLiquidity,Cushion,FullInitMarginReq,FullMaintMarginReq,'
            'FullAvailableFunds,FullExcessLiquidity,LookAheadNextChange,'
            'LookAheadInitMarginReq,LookAheadMaintMarginReq,'
            'LookAheadAvailableFunds,LookAheadExcessLiquidity,'
            'HighestSeverity,DayTradesRemaining,Leverage,$LEDGER:ALL')
        self.client.reqAccountSummary(reqId, 'All', tags)
        return future

    def reqExecutionsAsync(
            self, execFilter: ExecutionFilter = None) -> Awaitable[List[Fill]]:
        execFilter = execFilter or ExecutionFilter()
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId)
        self.client.reqExecutions(reqId, execFilter)
        return future

    def reqPositionsAsync(self) -> Awaitable[List[Position]]:
        future = self.wrapper.startReq('positions')
        self.client.reqPositions()
        return future

    def reqContractDetailsAsync(self, contract: Contract) -> \
            Awaitable[List[ContractDetails]]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId, contract)
        self.client.reqContractDetails(reqId, contract)
        return future

    async def reqMatchingSymbolsAsync(self, pattern: str) -> \
            Optional[List[ContractDescription]]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId)
        self.client.reqMatchingSymbols(reqId, pattern)
        try:
            await asyncio.wait_for(future, 4)
            return future.result()
        except asyncio.TimeoutError:
            self._logger.error('reqMatchingSymbolsAsync: Timeout')
            return None

    async def reqMarketRuleAsync(
            self, marketRuleId: int) -> Optional[PriceIncrement]:
        future = self.wrapper.startReq(f'marketRule-{marketRuleId}')
        try:
            self.client.reqMarketRule(marketRuleId)
            await asyncio.wait_for(future, 1)
            return future.result()
        except asyncio.TimeoutError:
            self._logger.error('reqMarketRuleAsync: Timeout')
            return None

    async def reqHistoricalDataAsync(
            self, contract: Contract,
            endDateTime: Union[datetime.datetime, datetime.date, str, None],
            durationStr: str, barSizeSetting: str,
            whatToShow: str, useRTH: bool,
            formatDate: int = 1, keepUpToDate: bool = False,
            chartOptions: List[TagValue] = [], timeout: float = 60) -> \
            BarDataList:
        reqId = self.client.getReqId()
        bars = BarDataList()
        bars.reqId = reqId
        bars.contract = contract
        bars.endDateTime = endDateTime
        bars.durationStr = durationStr
        bars.barSizeSetting = barSizeSetting
        bars.whatToShow = whatToShow
        bars.useRTH = useRTH
        bars.formatDate = formatDate
        bars.keepUpToDate = keepUpToDate
        bars.chartOptions = chartOptions or []
        future = self.wrapper.startReq(reqId, contract, container=bars)
        if keepUpToDate:
            self.wrapper.startSubscription(reqId, bars, contract)
        end = util.formatIBDatetime(endDateTime)
        self.client.reqHistoricalData(
            reqId, contract, end, durationStr, barSizeSetting,
            whatToShow, useRTH, formatDate, keepUpToDate, chartOptions)
        task = asyncio.wait_for(future, timeout) if timeout else future
        try:
            await task
        except asyncio.TimeoutError:
            self.client.cancelHistoricalData(reqId)
            self._logger.warning(f'reqHistoricalData: Timeout for {contract}')
            bars.clear()
        return bars

    def reqHistoricalTicksAsync(
            self, contract: Contract,
            startDateTime: Union[str, datetime.date],
            endDateTime: Union[str, datetime.date],
            numberOfTicks: int, whatToShow: str, useRth: bool,
            ignoreSize: bool = False,
            miscOptions: List[TagValue] = []) -> Awaitable[List]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId, contract)
        start = util.formatIBDatetime(startDateTime)
        end = util.formatIBDatetime(endDateTime)
        self.client.reqHistoricalTicks(
            reqId, contract, start, end, numberOfTicks, whatToShow, useRth,
            ignoreSize, miscOptions)
        return future

    def reqHeadTimeStampAsync(
            self, contract: Contract, whatToShow: str,
            useRTH: bool, formatDate: int) -> Awaitable[datetime.datetime]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId, contract)
        self.client.reqHeadTimeStamp(
            reqId, contract, whatToShow, useRTH, formatDate)
        return future

    def reqMktDepthExchangesAsync(self) -> \
            Awaitable[List[DepthMktDataDescription]]:
        future = self.wrapper.startReq('mktDepthExchanges')
        self.client.reqMktDepthExchanges()
        return future

    def reqHistogramDataAsync(
            self, contract: Contract, useRTH: bool, period: str) -> \
            Awaitable[List[HistogramData]]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId, contract)
        self.client.reqHistogramData(reqId, contract, useRTH, period)
        return future

    def reqFundamentalDataAsync(
            self, contract: Contract, reportType: str,
            fundamentalDataOptions: List[TagValue] = []) -> \
            Awaitable[str]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId, contract)
        self.client.reqFundamentalData(
            reqId, contract, reportType, fundamentalDataOptions)
        return future

    async def reqScannerDataAsync(
            self, subscription: ScannerSubscription,
            scannerSubscriptionOptions: List[TagValue] = [],
            scannerSubscriptionFilterOptions: List[TagValue] = []) \
            -> ScanDataList:
        dataList = self.reqScannerSubscription(
            subscription, scannerSubscriptionOptions or [],
            scannerSubscriptionFilterOptions or [])
        future = self.wrapper.startReq(dataList.reqId, container=dataList)
        await future
        self.client.cancelScannerSubscription(dataList.reqId)
        return future.result()

    def reqScannerParametersAsync(self) -> Awaitable[str]:
        future = self.wrapper.startReq('scannerParams')
        self.client.reqScannerParameters()
        return future

    async def calculateImpliedVolatilityAsync(
            self, contract: Contract,
            optionPrice: float, underPrice: float,
            implVolOptions: List[TagValue] = []) -> \
            Optional[OptionComputation]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId, contract)
        self.client.calculateImpliedVolatility(
            reqId, contract, optionPrice, underPrice, implVolOptions)
        try:
            await asyncio.wait_for(future, 4)
            return future.result()
        except asyncio.TimeoutError:
            self._logger.error('calculateImpliedVolatilityAsync: Timeout')
            return None
        finally:
            self.client.cancelCalculateImpliedVolatility(reqId)

    async def calculateOptionPriceAsync(
            self, contract: Contract,
            volatility: float, underPrice: float,
            optPrcOptions: List[TagValue] = []) -> Optional[OptionComputation]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId, contract)
        self.client.calculateOptionPrice(
            reqId, contract, volatility, underPrice, optPrcOptions)
        try:
            await asyncio.wait_for(future, 4)
            return future.result()
        except asyncio.TimeoutError:
            self._logger.error('calculateOptionPriceAsync: Timeout')
            return None
        finally:
            self.client.cancelCalculateOptionPrice(reqId)

    def reqSecDefOptParamsAsync(
            self, underlyingSymbol: str,
            futFopExchange: str, underlyingSecType: str,
            underlyingConId: int) -> Awaitable[List[OptionChain]]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId)
        self.client.reqSecDefOptParams(
            reqId, underlyingSymbol, futFopExchange,
            underlyingSecType, underlyingConId)
        return future

    def reqNewsProvidersAsync(self) -> Awaitable[List[NewsProvider]]:
        future = self.wrapper.startReq('newsProviders')
        self.client.reqNewsProviders()
        return future

    def reqNewsArticleAsync(
            self, providerCode: str, articleId: str,
            newsArticleOptions: Optional[List[TagValue]]) -> \
            Awaitable[NewsArticle]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId)
        self.client.reqNewsArticle(
            reqId, providerCode, articleId, newsArticleOptions)
        return future

    async def reqHistoricalNewsAsync(
            self, conId: int, providerCodes: str,
            startDateTime: Union[str, datetime.date],
            endDateTime: Union[str, datetime.date],
            totalResults: int,
            historicalNewsOptions: List[TagValue] = None) -> \
            Optional[HistoricalNews]:
        reqId = self.client.getReqId()
        future = self.wrapper.startReq(reqId)
        start = util.formatIBDatetime(startDateTime)
        end = util.formatIBDatetime(endDateTime)
        self.client.reqHistoricalNews(
            reqId, conId, providerCodes, start, end,
            totalResults, historicalNewsOptions)
        try:
            await asyncio.wait_for(future, 4)
            return future.result()
        except asyncio.TimeoutError:
            self._logger.error('reqHistoricalNewsAsync: Timeout')
            return None

    async def requestFAAsync(self, faDataType: int):
        future = self.wrapper.startReq('requestFA')
        self.client.requestFA(faDataType)
        try:
            await asyncio.wait_for(future, 4)
            return future.result()
        except asyncio.TimeoutError:
            self._logger.error('requestFAAsync: Timeout')
