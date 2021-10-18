import asyncio
import io
import logging
from loguru import logger
import struct
import time
from collections import deque
from typing import List
from eventkit import Event
from base.connection import Connection
from base.contract import Contract
from base.decoder import Decoder
from base.objects import ConnectionStats
from source.base.util import UNSET_DOUBLE, UNSET_INTEGER, dataclassAsTuple, run

logger.add(sink='logs/base_client.log',
           level='ERROR',
           format='{time:YYYY-MM-DD  :mm:ss} - {level} - {file} - {line} - {message}',
           enqueue=True,
           backtrace=True,
           diagnose=True,
           rotation='00:00',
           retention='7 days')

__all__ = ['Client']


class Client:
    events = ('apiStart', 'apiEnd', 'apiError', 'throttleStart', 'throttleEnd')

    MaxRequests = 45
    RequestsInterval = 1

    MinClientVersion = 142
    MaxClientVersion = 152

    (DISCONNECTED, CONNECTING, CONNECTED) = range(3)

    def __init__(self, wrapper):
        self.wrapper = wrapper
        self.decoder = Decoder(wrapper, None)
        self.apiStart = Event('apiStart')
        self.apiEnd = Event('apiEnd')
        self.apiError = Event('apiError')
        self.throttleStart = Event('throttleStart')
        self.throttleEnd = Event('throttleEnd')
        self._logger = logging.getLogger('ib_insync.client')
        self.reset()

        # extra optional wrapper methods
        self._priceSizeTick = getattr(wrapper, 'priceSizeTick', None)
        self._tcpDataArrived = getattr(wrapper, 'tcpDataArrived', None)
        self._tcpDataProcessed = getattr(wrapper, 'tcpDataProcessed', None)

    def reset(self):
        self.host = None
        self.port = None
        self.clientId = None
        self.conn = None
        self.connState = Client.DISCONNECTED
        self.optCapab = ''
        self._serverVersion = None
        self._readyEvent = None
        self._data = b''
        self._connectOptions = b''
        self._reqIdSeq = 0
        self._accounts = None
        self._startTime = time.time()
        self._numBytesRecv = 0
        self._numMsgRecv = 0
        self._isThrottling = False
        self._msgQ = deque()
        self._timeQ = deque()

    def serverVersion(self):
        return self._serverVersion

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_forever()

    def isConnected(self):
        return self.connState == Client.CONNECTED

    def isReady(self) -> bool:
        return bool(self._readyEvent) and self._readyEvent.is_set()

    def connectionStats(self) -> ConnectionStats:
        if not self.isReady():
            raise ConnectionError('Not connected')
        return ConnectionStats(
            self._startTime,
            time.time() - self._startTime,
            self._numBytesRecv, self.conn.numBytesSent,
            self._numMsgRecv, self.conn.numMsgSent)

    def getReqId(self) -> int:
        if not self.isReady():
            raise ConnectionError('Not connected')
        newId = self._reqIdSeq
        self._reqIdSeq += 1
        return newId

    def updateReqId(self, minReqId):
        self._reqIdSeq = max(self._reqIdSeq, minReqId)

    def getAccounts(self) -> List[str]:
        if not self.isReady():
            raise ConnectionError('Not connected')
        return self._accounts

    def setConnectOptions(self, connectOptions: str):
        self._connectOptions = connectOptions.encode()

    def connect(self, host: str, port: int, clientId: int, timeout: float = 2.0):
        run(self.connectAsync(host, port, clientId, timeout))

    async def connectAsync(self, host, port, clientId, timeout=2.0):

        async def connect():
            self._logger.info(
                f'Connecting to {host}:{port} with clientId {clientId}...')
            self.host = host
            self.port = port
            self.clientId = clientId
            self.connState = Client.CONNECTING
            self.conn = Connection(host, port)
            self.conn.hasData = self._onSocketHasData
            self.conn.disconnected = self._onSocketDisconnected
            self.conn.hasError = self._onSocketHasError
            await asyncio.sleep(0)  # in case of a not yet finished disconnect
            await self.conn.connectAsync()
            self._logger.info('Connected')
            msg = b'API\0' + self._prefix(b'v%d..%d%s' % (
                self.MinClientVersion, self.MaxClientVersion,
                b' ' + self._connectOptions if self._connectOptions else b''))
            self.conn.sendMsg(msg)
            await self._readyEvent.wait()
            self._logger.info('API connection ready')
            self.apiStart.emit()

        self._readyEvent = asyncio.Event()
        try:
            await asyncio.wait_for(connect(), timeout or None)
        except Exception as e:
            self.disconnect()
            msg = f'API connection failed: {e!r}'
            self._logger.error(msg)
            self.apiError.emit(msg)
            if isinstance(e, ConnectionRefusedError):
                self._logger.error('Make sure API port on TWS/IBG is open')
            raise

    def disconnect(self):
        """Disconnect from IB connection."""
        self.connState = Client.DISCONNECTED
        if self.conn is not None:
            self._logger.info('Disconnecting')
            self.conn.disconnect()
            self.reset()

    def send(self, *fields):
        """Serialize and send the given fields using the IB socket protocol."""
        if not self.isConnected():
            raise ConnectionError('Not connected')

        msg = io.StringIO()
        for field in fields:
            typ = type(field)
            if field in (None, UNSET_INTEGER, UNSET_DOUBLE):
                s = ''
            elif typ in (str, int, float):
                s = str(field)
            elif typ is bool:
                s = '1' if field else '0'
            elif typ is list:
                # list of TagValue
                s = ''.join(f'{v.tag}={v.value};' for v in field)
            elif isinstance(field, Contract):
                c = field
                s = '\0'.join(str(f) for f in (
                    c.conId, c.symbol, c.secType,
                    c.lastTradeDateOrContractMonth, c.strike,
                    c.right, c.multiplier, c.exchange,
                    c.primaryExchange, c.currency,
                    c.localSymbol, c.tradingClass))
            else:
                s = str(field)
            msg.write(s)
            msg.write('\0')
        self.sendMsg(msg.getvalue())

    def sendMsg(self, msg):
        loop = asyncio.get_event_loop()
        t = loop.time()
        times = self._timeQ
        msgs = self._msgQ
        while times and t - times[0] > self.RequestsInterval:
            times.popleft()
        if msg:
            msgs.append(msg)
        while msgs and (len(times) < self.MaxRequests or not self.MaxRequests):
            msg = msgs.popleft()
            self.conn.sendMsg(self._prefix(msg.encode()))
            times.append(t)
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug('>>> %s', msg[:-1].replace('\0', ','))
        if msgs:
            if not self._isThrottling:
                self._isThrottling = True
                self.throttleStart.emit()
                self._logger.debug('Started to throttle requests')
            loop.call_at(
                times[0] + self.RequestsInterval,
                self.sendMsg, None)
        else:
            if self._isThrottling:
                self._isThrottling = False
                self.throttleEnd.emit()
                self._logger.debug('Stopped to throttle requests')

    def _prefix(self, msg):
        # prefix a message with its length
        return struct.pack('>I', len(msg)) + msg

    def _onSocketHasData(self, data):
        debug = self._logger.isEnabledFor(logging.DEBUG)
        if self._tcpDataArrived:
            self._tcpDataArrived()

        self._data += data
        self._numBytesRecv += len(data)

        while True:
            if len(self._data) <= 4:
                break
            # 4 byte prefix tells the message length
            msgEnd = 4 + struct.unpack('>I', self._data[:4])[0]
            if len(self._data) < msgEnd:
                # insufficient data for now
                break
            msg = self._data[4:msgEnd].decode(errors='backslashreplace')
            self._data = self._data[msgEnd:]
            fields = msg.split('\0')
            fields.pop()  # pop off last empty element
            self._numMsgRecv += 1

            if debug:
                self._logger.debug('<<< %s', ','.join(fields))

            if not self._serverVersion and len(fields) == 2:
                # this concludes the handshake
                version, _connTime = fields
                self._serverVersion = int(version)
                if self._serverVersion < self.MinClientVersion:
                    self._onSocketHasError(
                        'TWS/gateway version must be >= 972')
                    return
                self.decoder.serverVersion = self._serverVersion
                self.connState = Client.CONNECTED
                self.startApi()
                self.wrapper.connectAck()
                self._logger.info(
                    f'Logged on to server version {self._serverVersion}')
            else:
                if self._readyEvent and not self._readyEvent.is_set():
                    # snoop for nextValidId and managedAccounts response,
                    # when both are in then the client is ready
                    msgId = int(fields[0])
                    if msgId == 9:
                        _, _, validId = fields
                        self._reqIdSeq = int(validId)
                        if self._accounts:
                            self._readyEvent.set()
                    elif msgId == 15:
                        _, _, accts = fields
                        self._accounts = [a for a in accts.split(',') if a]
                        if self._reqIdSeq:
                            self._readyEvent.set()

                # decode and handle the message
                self.decoder.interpret(fields)

        if self._tcpDataProcessed:
            self._tcpDataProcessed()

    def _onSocketDisconnected(self):
        wasReady = self.isReady()
        if self.isConnected():
            msg = 'Peer closed connection'
            self._logger.error(msg)
            if not wasReady:
                msg = f'clientId {self.clientId} already in use?'
                self._logger.error(msg)
            self.apiError.emit(msg)
        else:
            self._logger.info('Disconnected')
        if wasReady:
            self.wrapper.connectionClosed()
        self.reset()
        if wasReady:
            self.apiEnd.emit()

    def _onSocketHasError(self, msg):
        wasReady = self.isReady()
        self._logger.error(msg)
        if wasReady:
            self.wrapper.connectionClosed()
        self.reset()
        self.apiError.emit(msg)
        if wasReady:
            self.apiEnd.emit()

    def reqMktData(
            self, reqId, contract, genericTickList, snapshot,
            regulatorySnapshot, mktDataOptions):
        fields = [1, 11, reqId, contract]

        if contract.secType == 'BAG':
            legs = contract.comboLegs or []
            fields += [len(legs)]
            for leg in legs:
                fields += [leg.conId, leg.ratio, leg.action, leg.exchange]

        dnc = contract.deltaNeutralContract
        if dnc:
            fields += [True, dnc.conId, dnc.delta, dnc.price]
        else:
            fields += [False]

        fields += [
            genericTickList, snapshot, regulatorySnapshot, mktDataOptions]
        self.send(*fields)

    def cancelMktData(self, reqId):
        self.send(2, 2, reqId)

    def placeOrder(self, orderId, contract, order):
        version = self.serverVersion()
        fields = [3]
        if version < 145:
            fields += [45]
        fields += [
            orderId,
            contract,
            contract.secIdType,
            contract.secId,
            order.action,
            order.totalQuantity,
            order.orderType,
            order.lmtPrice,
            order.auxPrice,
            order.tif,
            order.ocaGroup,
            order.account,
            order.openClose,
            order.origin,
            order.orderRef,
            order.transmit,
            order.parentId,
            order.blockOrder,
            order.sweepToFill,
            order.displaySize,
            order.triggerMethod,
            order.outsideRth,
            order.hidden]

        if contract.secType == 'BAG':
            legs = contract.comboLegs or []
            fields += [len(legs)]
            for leg in legs:
                fields += [
                    leg.conId,
                    leg.ratio,
                    leg.action,
                    leg.exchange,
                    leg.openClose,
                    leg.shortSaleSlot,
                    leg.designatedLocation,
                    leg.exemptCode]

            legs = order.orderComboLegs or []
            fields += [len(legs)]
            for leg in legs:
                fields += [leg.price]

            params = order.smartComboRoutingParams or []
            fields += [len(params)]
            for param in params:
                fields += [param.tag, param.value]

        fields += [
            '',
            order.discretionaryAmt,
            order.goodAfterTime,
            order.goodTillDate,
            order.faGroup,
            order.faMethod,
            order.faPercentage,
            order.faProfile,
            order.modelCode,
            order.shortSaleSlot,
            order.designatedLocation,
            order.exemptCode,
            order.ocaType,
            order.rule80A,
            order.settlingFirm,
            order.allOrNone,
            order.minQty,
            order.percentOffset,
            order.eTradeOnly,
            order.firmQuoteOnly,
            order.nbboPriceCap,
            order.auctionStrategy,
            order.startingPrice,
            order.stockRefPrice,
            order.delta,
            order.stockRangeLower,
            order.stockRangeUpper,
            order.overridePercentageConstraints,
            order.volatility,
            order.volatilityType,
            order.deltaNeutralOrderType,
            order.deltaNeutralAuxPrice]

        if order.deltaNeutralOrderType:
            fields += [
                order.deltaNeutralConId,
                order.deltaNeutralSettlingFirm,
                order.deltaNeutralClearingAccount,
                order.deltaNeutralClearingIntent,
                order.deltaNeutralOpenClose,
                order.deltaNeutralShortSale,
                order.deltaNeutralShortSaleSlot,
                order.deltaNeutralDesignatedLocation]

        fields += [
            order.continuousUpdate,
            order.referencePriceType,
            order.trailStopPrice,
            order.trailingPercent,
            order.scaleInitLevelSize,
            order.scaleSubsLevelSize,
            order.scalePriceIncrement]

        if (0 < order.scalePriceIncrement < UNSET_DOUBLE):
            fields += [
                order.scalePriceAdjustValue,
                order.scalePriceAdjustInterval,
                order.scaleProfitOffset,
                order.scaleAutoReset,
                order.scaleInitPosition,
                order.scaleInitFillQty,
                order.scaleRandomPercent]

        fields += [
            order.scaleTable,
            order.activeStartTime,
            order.activeStopTime,
            order.hedgeType]

        if order.hedgeType:
            fields += [order.hedgeParam]

        fields += [
            order.optOutSmartRouting,
            order.clearingAccount,
            order.clearingIntent,
            order.notHeld]

        dnc = contract.deltaNeutralContract
        if dnc:
            fields += [True, dnc.conId, dnc.delta, dnc.price]
        else:
            fields += [False]

        fields += [order.algoStrategy]
        if order.algoStrategy:
            params = order.algoParams or []
            fields += [len(params)]
            for param in params:
                fields += [param.tag, param.value]

        fields += [
            order.algoId,
            order.whatIf,
            order.orderMiscOptions,
            order.solicited,
            order.randomizeSize,
            order.randomizePrice]

        if order.orderType == 'PEG BENCH':
            fields += [
                order.referenceContractId,
                order.isPeggedChangeAmountDecrease,
                order.peggedChangeAmount,
                order.referenceChangeAmount,
                order.referenceExchangeId]

        fields += [len(order.conditions)]
        if order.conditions:
            for cond in order.conditions:
                fields += dataclassAsTuple(cond)
            fields += [
                order.conditionsIgnoreRth,
                order.conditionsCancelOrder]

        fields += [
            order.adjustedOrderType,
            order.triggerPrice,
            order.lmtPriceOffset,
            order.adjustedStopPrice,
            order.adjustedStopLimitPrice,
            order.adjustedTrailingAmount,
            order.adjustableTrailingUnit,
            order.extOperator,
            order.softDollarTier.name,
            order.softDollarTier.val,
            order.cashQty]

        if version >= 138:
            fields += [order.mifid2DecisionMaker, order.mifid2DecisionAlgo]
        if version >= 139:
            fields += [order.mifid2ExecutionTrader, order.mifid2ExecutionAlgo]
        if version >= 141:
            fields += [order.dontUseAutoPriceForHedge]
        if version >= 145:
            fields += [order.isOmsContainer]
        if version >= 148:
            fields += [order.discretionaryUpToLimitPrice]
        if version >= 151:
            fields += [order.usePriceMgmtAlgo]

        self.send(*fields)

    def cancelOrder(self, orderId):
        self.send(4, 1, orderId)

    def reqOpenOrders(self):
        self.send(5, 1)

    def reqAccountUpdates(self, subscribe, acctCode):
        self.send(6, 2, subscribe, acctCode)

    def reqExecutions(self, reqId, execFilter):
        self.send(
            7, 3, reqId,
            execFilter.clientId,
            execFilter.acctCode,
            execFilter.time,
            execFilter.symbol,
            execFilter.secType,
            execFilter.exchange,
            execFilter.side)

    def reqIds(self, numIds):
        self.send(8, 1, numIds)

    def reqContractDetails(self, reqId, contract):
        self.send(
            9, 8, reqId, contract, contract.includeExpired,
            contract.secIdType, contract.secId)

    def reqMktDepth(
            self, reqId, contract, numRows, isSmartDepth, mktDepthOptions):
        version = self.serverVersion()
        fields = [
            10, 5, reqId,
            contract.conId,
            contract.symbol,
            contract.secType,
            contract.lastTradeDateOrContractMonth,
            contract.strike,
            contract.right,
            contract.multiplier,
            contract.exchange]
        if version >= 149:
            fields += [contract.primaryExchange]
        fields += [
            contract.currency,
            contract.localSymbol,
            contract.tradingClass,
            numRows]
        if version >= 146:
            fields += [isSmartDepth]
        fields += [mktDepthOptions]
        self.send(*fields)

    def cancelMktDepth(self, reqId, isSmartDepth):
        self.send(11, 1, reqId, isSmartDepth)

    def reqNewsBulletins(self, allMsgs):
        self.send(12, 1, allMsgs)

    def cancelNewsBulletins(self):
        self.send(13, 1)

    def setServerLogLevel(self, logLevel):
        self.send(14, 1, logLevel)

    def reqAutoOpenOrders(self, bAutoBind):
        self.send(15, 1, bAutoBind)

    def reqAllOpenOrders(self):
        self.send(16, 1)

    def reqManagedAccts(self):
        self.send(17, 1)

    def requestFA(self, faData):
        self.send(18, 1, faData)

    def replaceFA(self, faData, cxml):
        self.send(19, 1, faData, cxml)

    def reqHistoricalData(
            self, reqId, contract, endDateTime, durationStr, barSizeSetting,
            whatToShow, useRTH, formatDate, keepUpToDate, chartOptions):
        fields = [
            20, reqId, contract, contract.includeExpired,
            endDateTime, barSizeSetting, durationStr, useRTH,
            whatToShow, formatDate]

        if contract.secType == 'BAG':
            legs = contract.comboLegs or []
            fields += [len(legs)]
            for leg in legs:
                fields += [leg.conId, leg.ratio, leg.action, leg.exchange]

        fields += [keepUpToDate, chartOptions]
        self.send(*fields)

    def exerciseOptions(
            self, reqId, contract, exerciseAction,
            exerciseQuantity, account, override):
        self.send(
            21, 2, reqId,
            contract.conId,
            contract.symbol,
            contract.secType,
            contract.lastTradeDateOrContractMonth,
            contract.strike,
            contract.right,
            contract.multiplier,
            contract.exchange,
            contract.currency,
            contract.localSymbol,
            contract.tradingClass,
            exerciseAction, exerciseQuantity, account, override)

    def reqScannerSubscription(
            self, reqId, subscription, scannerSubscriptionOptions,
            scannerSubscriptionFilterOptions):
        version = self.serverVersion()
        sub = subscription
        fields = [22]
        if version < 143:
            fields += [4]
        fields += [
            reqId,
            sub.numberOfRows,
            sub.instrument,
            sub.locationCode,
            sub.scanCode,
            sub.abovePrice,
            sub.belowPrice,
            sub.aboveVolume,
            sub.marketCapAbove,
            sub.marketCapBelow,
            sub.moodyRatingAbove,
            sub.moodyRatingBelow,
            sub.spRatingAbove,
            sub.spRatingBelow,
            sub.maturityDateAbove,
            sub.maturityDateBelow,
            sub.couponRateAbove,
            sub.couponRateBelow,
            sub.excludeConvertible,
            sub.averageOptionVolumeAbove,
            sub.scannerSettingPairs,
            sub.stockTypeFilter]
        if version >= 143:
            fields += [scannerSubscriptionFilterOptions]
        fields += [scannerSubscriptionOptions]
        self.send(*fields)

    def cancelScannerSubscription(self, reqId):
        self.send(23, 1, reqId)

    def reqScannerParameters(self):
        self.send(24, 1)

    def cancelHistoricalData(self, reqId):
        self.send(25, 1, reqId)

    def reqCurrentTime(self):
        self.send(49, 1)

    def reqRealTimeBars(
            self, reqId, contract, barSize, whatToShow,
            useRTH, realTimeBarsOptions):
        self.send(
            50, 3, reqId, contract, barSize, whatToShow,
            useRTH, realTimeBarsOptions)

    def cancelRealTimeBars(self, reqId):
        self.send(51, 1, reqId)

    def reqFundamentalData(
            self, reqId, contract, reportType, fundamentalDataOptions):
        options = fundamentalDataOptions or []
        self.send(
            52, 2, reqId,
            contract.conId,
            contract.symbol,
            contract.secType,
            contract.exchange,
            contract.primaryExchange,
            contract.currency,
            contract.localSymbol,
            reportType, len(options), options)

    def cancelFundamentalData(self, reqId):
        self.send(53, 1, reqId)

    def calculateImpliedVolatility(
            self, reqId, contract, optionPrice, underPrice, implVolOptions):
        self.send(
            54, 3, reqId, contract, optionPrice, underPrice,
            len(implVolOptions), implVolOptions)

    def calculateOptionPrice(
            self, reqId, contract, volatility, underPrice, optPrcOptions):
        self.send(
            55, 3, reqId, contract, volatility, underPrice,
            len(optPrcOptions), optPrcOptions)

    def cancelCalculateImpliedVolatility(self, reqId):
        self.send(56, 1, reqId)

    def cancelCalculateOptionPrice(self, reqId):
        self.send(57, 1, reqId)

    def reqGlobalCancel(self):
        self.send(58, 1)

    def reqMarketDataType(self, marketDataType):
        self.send(59, 1, marketDataType)

    def reqPositions(self):
        self.send(61, 1)

    def reqAccountSummary(self, reqId, groupName, tags):
        self.send(62, 1, reqId, groupName, tags)

    def cancelAccountSummary(self, reqId):
        self.send(63, 1, reqId)

    def cancelPositions(self):
        self.send(64, 1)

    def verifyRequest(self, apiName, apiVersion):
        self.send(65, 1, apiName, apiVersion)

    def verifyMessage(self, apiData):
        self.send(66, 1, apiData)

    def queryDisplayGroups(self, reqId):
        self.send(67, 1, reqId)

    def subscribeToGroupEvents(self, reqId, groupId):
        self.send(68, 1, reqId, groupId)

    def updateDisplayGroup(self, reqId, contractInfo):
        self.send(69, 1, reqId, contractInfo)

    def unsubscribeFromGroupEvents(self, reqId):
        self.send(70, 1, reqId)

    def startApi(self):
        self.send(71, 2, self.clientId, self.optCapab)

    def verifyAndAuthRequest(self, apiName, apiVersion, opaqueIsvKey):
        self.send(72, 1, apiName, apiVersion, opaqueIsvKey)

    def verifyAndAuthMessage(self, apiData, xyzResponse):
        self.send(73, 1, apiData, xyzResponse)

    def reqPositionsMulti(self, reqId, account, modelCode):
        self.send(74, 1, reqId, account, modelCode)

    def cancelPositionsMulti(self, reqId):
        self.send(75, 1, reqId)

    def reqAccountUpdatesMulti(self, reqId, account, modelCode, ledgerAndNLV):
        self.send(76, 1, reqId, account, modelCode, ledgerAndNLV)

    def cancelAccountUpdatesMulti(self, reqId):
        self.send(77, 1, reqId)

    def reqSecDefOptParams(
            self, reqId, underlyingSymbol, futFopExchange,
            underlyingSecType, underlyingConId):
        self.send(
            78, reqId, underlyingSymbol, futFopExchange,
            underlyingSecType, underlyingConId)

    def reqSoftDollarTiers(self, reqId):
        self.send(79, reqId)

    def reqFamilyCodes(self):
        self.send(80)

    def reqMatchingSymbols(self, reqId, pattern):
        self.send(81, reqId, pattern)

    def reqMktDepthExchanges(self):
        self.send(82)

    def reqSmartComponents(self, reqId, bboExchange):
        self.send(83, reqId, bboExchange)

    def reqNewsArticle(
            self, reqId, providerCode, articleId, newsArticleOptions):
        self.send(84, reqId, providerCode, articleId, newsArticleOptions)

    def reqNewsProviders(self):
        self.send(85)

    def reqHistoricalNews(
            self, reqId, conId, providerCodes, startDateTime, endDateTime,
            totalResults, historicalNewsOptions):
        self.send(
            86, reqId, conId, providerCodes, startDateTime, endDateTime,
            totalResults, historicalNewsOptions)

    def reqHeadTimeStamp(
            self, reqId, contract, whatToShow, useRTH, formatDate):
        self.send(
            87, reqId, contract, contract.includeExpired,
            useRTH, whatToShow, formatDate)

    def reqHistogramData(self, tickerId, contract, useRTH, timePeriod):
        self.send(
            88, tickerId, contract, contract.includeExpired,
            useRTH, timePeriod)

    def cancelHistogramData(self, tickerId):
        self.send(89, tickerId)

    def cancelHeadTimeStamp(self, reqId):
        self.send(90, reqId)

    def reqMarketRule(self, marketRuleId):
        self.send(91, marketRuleId)

    def reqPnL(self, reqId, account, modelCode):
        self.send(92, reqId, account, modelCode)

    def cancelPnL(self, reqId):
        self.send(93, reqId)

    def reqPnLSingle(self, reqId, account, modelCode, conid):
        self.send(94, reqId, account, modelCode, conid)

    def cancelPnLSingle(self, reqId):
        self.send(95, reqId)

    def reqHistoricalTicks(
            self, reqId, contract, startDateTime, endDateTime,
            numberOfTicks, whatToShow, useRth, ignoreSize, miscOptions):
        self.send(
            96, reqId, contract, contract.includeExpired,
            startDateTime, endDateTime, numberOfTicks, whatToShow,
            useRth, ignoreSize, miscOptions)

    def reqTickByTickData(
            self, reqId, contract, tickType, numberOfTicks, ignoreSize):
        self.send(97, reqId, contract, tickType, numberOfTicks, ignoreSize)

    def cancelTickByTickData(self, reqId):
        self.send(98, reqId)

    def reqCompletedOrders(self, apiOnly):
        self.send(99, apiOnly)
