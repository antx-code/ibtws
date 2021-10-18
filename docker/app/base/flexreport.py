import logging
import time
from loguru import logger
import xml.etree.ElementTree as et
from contextlib import suppress
from urllib.request import urlopen
from source.base import util
from base.objects import DynamicObject

logger.add(sink='logs/base_fixreport.log',
           level='ERROR',
           format='{time:YYYY-MM-DD  :mm:ss} - {level} - {file} - {line} - {message}',
           enqueue=True,
           backtrace=True,
           diagnose=True,
           rotation='00:00',
           retention='7 days')

__all__ = ('FlexReport', 'FlexError')

_logger = logging.getLogger('base.flexreport')


class FlexError(Exception):
    pass


class FlexReport:
    def __init__(self, token=None, queryId=None, path=None):
        self.data = None
        self.root = None
        if token and queryId:
            self.download(token, queryId)
        elif path:
            self.load(path)

    def topics(self):
        return set(node.tag for node in self.root.iter() if node.attrib)

    def extract(self, topic: str, parseNumbers=True) -> list:
        cls = type(topic, (DynamicObject,), {})
        results = [cls(**node.attrib) for node in self.root.iter(topic)]
        if parseNumbers:
            for obj in results:
                d = obj.__dict__
                for k, v in d.items():
                    with suppress(ValueError):
                        d[k] = float(v)
                        d[k] = int(v)
        return results

    def df(self, topic: str, parseNumbers=True):
        return util.df(self.extract(topic, parseNumbers))

    def download(self, token, queryId):
        url = (
            'https://gdcdyn.interactivebrokers.com'
            f'/Universal/servlet/FlexStatementService.SendRequest?'
            f't={token}&q={queryId}&v=3')
        resp = urlopen(url)
        data = resp.read()

        root = et.fromstring(data)
        if root.find('Status').text == 'Success':
            code = root.find('ReferenceCode').text
            baseUrl = root.find('Url').text
            _logger.info('Statement is being prepared...')
        else:
            errorCode = root.find('ErrorCode').text
            errorMsg = root.find('ErrorMessage').text
            raise FlexError(f'{errorCode}: {errorMsg}')

        while True:
            time.sleep(1)
            url = f'{baseUrl}?q={code}&t={token}'
            resp = urlopen(url)
            self.data = resp.read()
            self.root = et.fromstring(self.data)
            if self.root[0].tag == 'code':
                msg = self.root[0].text
                if msg.startswith('Statement generation in progress'):
                    _logger.info('still working...')
                    continue
                else:
                    raise FlexError(msg)
            break
        _logger.info('Statement retrieved.')

    def load(self, path):
        with open(path, 'rb') as f:
            self.data = f.read()
            self.root = et.fromstring(self.data)

    def save(self, path):
        with open(path, 'wb') as f:
            f.write(self.data)