from datetime import datetime, timedelta
from StringIO import StringIO
from tempfile import NamedTemporaryFile
from ConfigParser import RawConfigParser
from shutil import copyfileobj
import requests


def dump(version=None, options=None):
    region = options['region']
    format = options['dump_format']

    if version is None:
        url = '/'.join((
            options['url'], 'dump', 'latest',
            region + '.' + format
        ))
    else:
        url = '/'.join((
            options['url'], 'dump', region,
            region + '-' + version.strftime('%y%m%d') + '.' + format
        ))

    print 'Load from: ' + url
    data = requests.get(url, stream=True)
    metadata = requests.get(url + '.meta')

    datafile = NamedTemporaryFile(suffix='.' + format)
    copyfileobj(data.raw, datafile)
    datafile.flush()
    datafile.seek(0)

    VERSION_FORMAT = '%Y-%m-%d %H:%M:%S'

    cfg = RawConfigParser()
    cfg.readfp(StringIO(metadata.content))
    version = datetime.strptime(cfg.get('DEFAULT', 'version'), VERSION_FORMAT)

    return (datafile, version)


def diff(from_version, options=None):
    region = options['region']
    format = options['diff_format']

    to_version = from_version + timedelta(1)
    url = '/'.join((
        options['url'], 'diff', region,
        region + '-' + from_version.strftime('%y%m%d')
        + '-' + to_version.strftime('%y%m%d')
        + '.' + format
    ))

    data = requests.get(url, stream=True)

    if data.status_code == 404:
        return None
    else:
        datafile = NamedTemporaryFile(suffix='.' + format)
        copyfileobj(data.raw, datafile)
        datafile.flush()
        datafile.seek(0)

        return (datafile, to_version)
