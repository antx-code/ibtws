import asyncio
import logging
import math
from loguru import logger
import signal
import sys
import time
from dataclasses import fields, is_dataclass
from datetime import date, datetime, time as time_, timedelta, timezone
from typing import AsyncIterator, Awaitable, Callable, Iterator, List, Union
import eventkit as ev

logger.add(sink='logs/base_util.log',
           level='ERROR',
           format='{time:YYYY-MM-DD  :mm:ss} - {level} - {file} - {line} - {message}',
           enqueue=True,
           backtrace=True,
           diagnose=True,
           rotation='00:00',
           retention='7 days')

globalErrorEvent = ev.Event()
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
UNSET_INTEGER = 2 ** 31 - 1
UNSET_DOUBLE = sys.float_info.max


def df(objs, labels: List[str] = None):
    import pandas as pd
    from .objects import DynamicObject
    if objs:
        objs = list(objs)
        obj = objs[0]
        if is_dataclass(obj):
            df = pd.DataFrame.from_records(dataclassAsTuple(o) for o in objs)
            df.columns = [field.name for field in fields(obj)]
        elif isinstance(obj, DynamicObject):
            df = pd.DataFrame.from_records(o.__dict__ for o in objs)
        else:
            df = pd.DataFrame.from_records(objs)
        if isinstance(obj, tuple):
            _fields = getattr(obj, '_fields', None)
            if _fields:
                df.columns = _fields
    else:
        df = None
    if labels:
        exclude = [label for label in df if label not in labels]
        df = df.drop(exclude, axis=1)
    return df


def dataclassAsDict(obj) -> dict:
    if not is_dataclass(obj):
        raise TypeError(f'Object {obj} is not a dataclass')
    return {field.name: getattr(obj, field.name) for field in fields(obj)}


def dataclassAsTuple(obj) -> tuple:
    if not is_dataclass(obj):
        raise TypeError(f'Object {obj} is not a dataclass')
    return tuple(getattr(obj, field.name) for field in fields(obj))


def dataclassNonDefaults(obj) -> dict:
    if not is_dataclass(obj):
        raise TypeError(f'Object {obj} is not a dataclass')
    values = [getattr(obj, field.name) for field in fields(obj)]
    return {
        field.name: value for field, value in zip(fields(obj), values)
        if value != field.default
        and value == value
        and not (isinstance(value, list) and value == [])}


def dataclassUpdate(obj, *srcObjs, **kwargs) -> object:
    if not is_dataclass(obj):
        raise TypeError(f'Object {obj} is not a dataclass')
    for srcObj in srcObjs:
        obj.__dict__.update(dataclassAsDict(srcObj))
    obj.__dict__.update(**kwargs)
    return obj


def dataclassRepr(obj) -> str:
    attrs = dataclassNonDefaults(obj)
    clsName = obj.__class__.__qualname__
    kwargs = ', '.join(f'{k}={v!r}' for k, v in attrs.items())
    return f'{clsName}({kwargs})'


def isnamedtupleinstance(x):
    t = type(x)
    b = t.__bases__
    if len(b) != 1 or b[0] != tuple:
        return False
    f = getattr(t, '_fields', None)
    if not isinstance(f, tuple):
        return False
    return all(type(n) == str for n in f)


def tree(obj):
    if isinstance(obj, (bool, int, float, str, bytes)):
        return obj
    elif isinstance(obj, (date, time_)):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: tree(v) for k, v in obj.items()}
    elif isnamedtupleinstance(obj):
        return {f: tree(getattr(obj, f)) for f in obj._fields}
    elif isinstance(obj, (list, tuple, set)):
        return [tree(i) for i in obj]
    elif is_dataclass(obj):
        return {obj.__class__.__qualname__: tree(dataclassNonDefaults(obj))}
    else:
        return str(obj)


def barplot(bars, title='', upColor='blue', downColor='red'):
    import pandas as pd
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    from matplotlib.patches import Rectangle

    if isinstance(bars, pd.DataFrame):
        ohlcTups = [
            tuple(v) for v in bars[['open', 'high', 'low', 'close']].values]
    elif bars and hasattr(bars[0], 'open_'):
        ohlcTups = [(b.open_, b.high, b.low, b.close) for b in bars]
    else:
        ohlcTups = [(b.open, b.high, b.low, b.close) for b in bars]

    fig, ax = plt.subplots()
    ax.set_title(title)
    ax.grid(True)
    fig.set_size_inches(10, 6)
    for n, (open_, high, low, close) in enumerate(ohlcTups):
        if close >= open_:
            color = upColor
            bodyHi, bodyLo = close, open_
        else:
            color = downColor
            bodyHi, bodyLo = open_, close
        line = Line2D(
            xdata=(n, n),
            ydata=(low, bodyLo),
            color=color,
            linewidth=1)
        ax.add_line(line)
        line = Line2D(
            xdata=(n, n),
            ydata=(high, bodyHi),
            color=color,
            linewidth=1)
        ax.add_line(line)
        rect = Rectangle(
            xy=(n - 0.3, bodyLo),
            width=0.6,
            height=bodyHi - bodyLo,
            edgecolor=color,
            facecolor=color,
            alpha=0.4,
            antialiased=True
        )
        ax.add_patch(rect)

    ax.autoscale_view()
    return fig


def allowCtrlC():
    """Allow Control-C to end program."""
    signal.signal(signal.SIGINT, signal.SIG_DFL)


def logToFile(path, level=logging.INFO):
    """Create a log handler that logs to the given file."""
    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s')
    handler = logging.FileHandler(path)
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def logToConsole(level=logging.INFO):
    """Create a log handler that logs to the console."""
    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.handlers = [
        h for h in logger.handlers
        if type(h) is not logging.StreamHandler]
    logger.addHandler(handler)


def isNan(x: float) -> bool:
    """Not a number test."""
    return x != x


def formatSI(n: float) -> str:
    """Format the integer or float n to 3 significant digits + SI prefix."""
    s = ''
    if n < 0:
        n = -n
        s += '-'
    if type(n) is int and n < 1000:
        s = str(n) + ' '
    elif n < 1e-22:
        s = '0.00 '
    else:
        assert n < 9.99e26
        log = int(math.floor(math.log10(n)))
        i, j = divmod(log, 3)
        for _try in range(2):
            templ = '%.{}f'.format(2 - j)
            val = templ % (n * 10 ** (-3 * i))
            if val != '1000':
                break
            i += 1
            j = 0
        s += val + ' '
        if i != 0:
            s += 'yzafpnum kMGTPEZY'[i + 8]
    return s


class timeit:
    """Context manager for timing."""

    def __init__(self, title='Run'):
        self.title = title

    def __enter__(self):
        self.t0 = time.time()

    def __exit__(self, *_args):
        print(self.title + ' took ' + formatSI(time.time() - self.t0) + 's')


def run(*awaitables: Awaitable, timeout: float = None):
    loop = asyncio.get_event_loop()
    if not awaitables:
        if loop.is_running():
            return
        loop.run_forever()
        result = None
        all_tasks = (
            asyncio.all_tasks(loop)  # type: ignore
            if sys.version_info >= (3, 7) else asyncio.Task.all_tasks())
        if all_tasks:
            # cancel pending tasks
            f = asyncio.gather(*all_tasks)
            f.cancel()
            try:
                loop.run_until_complete(f)
            except asyncio.CancelledError:
                pass
    else:
        if len(awaitables) == 1:
            future = awaitables[0]
        else:
            future = asyncio.gather(*awaitables)
        if timeout:
            future = asyncio.wait_for(future, timeout)
        task = asyncio.ensure_future(future)

        def onError(_):
            task.cancel()

        globalErrorEvent.connect(onError)
        try:
            result = loop.run_until_complete(task)
        except asyncio.CancelledError as e:
            raise globalErrorEvent.value() or e
        finally:
            globalErrorEvent.disconnect(onError)

    return result


def _fillDate(time: Union[time_, datetime]) -> datetime:
    # use today if date is absent
    if isinstance(time, time_):
        dt = datetime.combine(date.today(), time)
    else:
        dt = time
    return dt


def schedule(time: Union[time_, datetime], callback: Callable, *args):
    dt = _fillDate(time)
    now = datetime.now(dt.tzinfo)
    delay = (dt - now).total_seconds()
    loop = asyncio.get_event_loop()
    return loop.call_later(delay, callback, *args)


def sleep(secs: float = 0.02) -> bool:
    run(asyncio.sleep(secs))
    return True


def timeRange(start: Union[time_, datetime],
        end: Union[time_, datetime],
        step: float) -> Iterator[datetime]:
    assert step > 0
    delta = timedelta(seconds=step)
    t = _fillDate(start)
    tz = timezone.utc if t.tzinfo else None
    now = datetime.now(tz)
    while t < now:
        t += delta
    while t <= _fillDate(end):
        waitUntil(t)
        yield t
        t += delta


def waitUntil(t: Union[time_, datetime]) -> bool:
    now = datetime.now(t.tzinfo)
    secs = (_fillDate(t) - now).total_seconds()
    run(asyncio.sleep(secs))
    return True


async def timeRangeAsync(
        start: Union[time_, datetime],
        end: Union[time_, datetime],
        step: float) -> AsyncIterator[datetime]:
    assert step > 0
    delta = timedelta(seconds=step)
    t = _fillDate(start)
    tz = timezone.utc if t.tzinfo else None
    now = datetime.now(tz)
    while t < now:
        t += delta
    while t <= _fillDate(end):
        await waitUntilAsync(t)
        yield t
        t += delta


async def waitUntilAsync(t: Union[time_, datetime]) -> bool:
    now = datetime.now(t.tzinfo)
    secs = (_fillDate(t) - now).total_seconds()
    await asyncio.sleep(secs)
    return True


def patchAsyncio():
    import nest_asyncio
    nest_asyncio.apply()


def startLoop():
    def _ipython_loop_asyncio(kernel):
        loop = asyncio.get_event_loop()

        def kernel_handler():
            kernel.do_one_iteration()
            loop.call_later(kernel._poll_interval, kernel_handler)

        loop.call_soon(kernel_handler)
        try:
            if not loop.is_running():
                loop.run_forever()
        finally:
            if not loop.is_running():
                loop.run_until_complete(loop.shutdown_asyncgens())
                loop.close()

    patchAsyncio()
    loop = asyncio.get_event_loop()
    if not loop.is_running():
        from ipykernel.eventloops import register_integration, enable_gui
        register_integration('asyncio')(_ipython_loop_asyncio)
        enable_gui('asyncio')


def useQt(qtLib: str = 'PyQt5', period: float = 0.01):
    def qt_step():
        loop.call_later(period, qt_step)
        if not stack:
            qloop = QEventLoop()
            timer = QTimer()
            timer.timeout.connect(qloop.quit)
            stack.append((qloop, timer))
        qloop, timer = stack.pop()
        timer.start(0)
        qloop.exec_()
        timer.stop()
        stack.append((qloop, timer))
        qApp.processEvents()

    if qtLib not in ('PyQt5', 'PySide2'):
        raise RuntimeError(f'Unknown Qt library: {qtLib}')
    if qtLib == 'PyQt5':
        from PyQt5.Qt import QApplication, QTimer, QEventLoop
    else:
        from PySide2.QtWidgets import QApplication
        from PySide2.QtCore import QTimer, QEventLoop
    global qApp
    qApp = QApplication.instance() or QApplication(sys.argv)  # type: ignore
    loop = asyncio.get_event_loop()
    stack: list = []
    qt_step()


def formatIBDatetime(dt: Union[date, datetime, str, None]) -> str:
    if not dt:
        s = ''
    elif isinstance(dt, datetime):
        if dt.tzinfo:
            dt = dt.astimezone()
        s = dt.strftime('%Y%m%d %H:%M:%S')
    elif isinstance(dt, date):
        s = dt.strftime('%Y%m%d 23:59:59')
    else:
        s = dt
    return s


def parseIBDatetime(s: str) -> Union[date, datetime]:
    if len(s) == 8:
        # YYYYmmdd
        y = int(s[0:4])
        m = int(s[4:6])
        d = int(s[6:8])
        dt = date(y, m, d)
    elif s.isdigit():
        dt = datetime.fromtimestamp(int(s), timezone.utc)
    else:
        # YYYYmmdd  HH:MM:SS
        # or
        # YYYY-mm-dd HH:MM:SS.0
        ss = s.replace(' ', '').replace('-', '')[:16]
        dt = datetime.strptime(ss, '%Y%m%d%H:%M:%S')
    return dt
