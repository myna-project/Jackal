#!/usr/bin/env python3
""" IEnergyDa Jackal

Jackal is a modular python parser and IEnergyDa (IEnergy Data aggregator) REST
client. It has been developed to address the specific need of parsing text
files from various energy gateways and data loggers and send the parsed data
in JSON format via HTTP(s) to the IEnergyDa backend. It has been released
as an open-source project in November 2019.

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation, either version 3 of the License, or (at your option) any later
version.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
You should have received a copy of the GNU General Public License along with
this program. If not, see <http://www.gnu.org/licenses/>.
"""

__author__      = 'Myna-Project.org Srl'
__contact__     = 'info@myna-project.org'
__copyright__   = 'Copyright 2020, Myna-Project.org Srl'
__credits__     = 'Myna-Project.org Srl'
__email__       = 'info@myna-project.org'
__license__     = 'GPLv3'
__status__      = 'Production'
__summary__     = 'Jackal is a modular python parser and IEnergyDa REST client.'
__title__       = 'IEnergyDa Jackal'
__uri__         = 'https://github.com/myna-project/Jackal'
__version__     = 'v1.4.1'

import configparser
import datetime
import fnmatch
import glob
import io
import json
import logging
import os
import pkgutil
import pyinotify
import queue
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.exceptions import ConnectionError, ConnectTimeout, HTTPError, Timeout, ReadTimeout
import schedule
import signal
import shutil
import threading
import time
import zipfile
import zlib

class JLogger(logging.Logger):

    def __init__(self, name, level=logging.INFO):
        FORMAT = '%(asctime)-s %(name)s %(threadName)s %(levelname)s %(message)s'
        logging.basicConfig(format=FORMAT)
        super(JLogger, self).__init__(name, level)

    def setup(self):
        levels = {'CRITICAL': logging.CRITICAL, 'ERROR': logging.ERROR, 'WARNING': logging.WARNING, 'INFO': logging.INFO, 'DEBUG': logging.DEBUG}
        level = levels.setdefault(config.loglevel, self.level)
        self.setLevel(level)
        logger.debug('%s logging level set to %s' % (__title__, config.loglevel))
        try:
            import http.client as http_client
        except ImportError:
            import httplib as http_client
        if level == logging.DEBUG:
            http_client.HTTPConnection.debuglevel = 1
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(level)
        requests_log.propagate = True


class JConfig(configparser.ConfigParser):

    def __init__(self):
        self.loglevel = 'INFO'
        self.baseurl = 'http://localhost:8080/IEnergyDa'
        self.username = None
        self.password = None
        self.retries = 3
        self.backoff = 0.3
        self.interval = 3600
        self.timeout = 60
        super(JConfig, self).__init__()

    def read(self, filename):
        logger.info('%s reading configuration file %s' % (__title__, filename))
        if not os.path.isfile(filename):
            logger.error('Configuration file %s does not exists, using default settings' % filename)
        try:
            super(JConfig, self).read(filename)
        except configparser.ParsingError as e:
            logger.critical(e)
            os._exit(1)
        self.loglevel = self.get(__name__, 'loglevel', fallback = self.loglevel)
        self.baseurl = self.get(__name__, 'baseurl', fallback = self.baseurl)
        self.username = self.get(__name__, 'username', fallback = self.username)
        self.password = self.get(__name__, 'password', fallback = self.password)
        self.retries = self.getint(__name__, 'retries', fallback = self.retries)
        self.backoff = self.getfloat(__name__, 'backoff', fallback = self.backoff)
        self.interval = self.getint(__name__, 'interval', fallback = self.interval)
        self.timeout = self.getint(__name__, 'timeout', fallback = self.timeout)
        self.token = '%s/token' % self.baseurl
        self.drain = '%s/organization/measuresmatrix' % self.baseurl
        logger.info('%s processing interval %s seconds' % (__title__, self.interval))
        logger.info('%s base REST API URL %s' % (__title__, self.baseurl))


class JRest:

    def __init__(self):
        self.__client = requests.session()
        self.token = config.token
        self.drain = config.drain
        self.username = config.username
        self.password = config.password
        self.backoff = config.backoff
        self.retries = config.retries
        self.timeout = config.timeout
        self.__csrf = None
        self.__recursion = False
        self.proxies = {}
        try:
            self.proxies = urllib.request.getproxies()
        except:
            pass
        retry = Retry(
            total = self.retries,
            read = self.retries,
            connect = self.retries,
            backoff_factor = self.backoff,
            status_forcelist = (500, 502, 504),
        )
        for protocol in ['http://', 'https://']:
            self.__client.adapters[protocol].max_retries = retry

    def __get_token(self):
        try:
            response = self.__client.get(self.token, timeout=self.timeout, verify=False, proxies=self.proxies)
            logger.debug('GET %s' % self.token)
        except (ConnectionError, ConnectTimeout, ReadTimeout) as e:
            logger.error (str(e))
            return False
        except TypeError as e:
            # workaround for urllib3 Retry() bug
            logger.error (str(e.__context__))
            return False
        if 'x-csrf-token' in response.headers:
            self.__csrf = response.headers['x-csrf-token']
            logger.debug('Got token %s' % self.__csrf)
            return True
        return False

    def __post(self, data, url):
        if not self.__csrf:
            if not self.__get_token():
                logger.error('Server %s forbids GET token requests. Check plugin and server configuration.' % (config.baseurl))
                return False
        headers = {}
        headers['X-CSRF-TOKEN'] = self.__csrf
        try:
            auth = None
            if self.username and self.password:
                auth = (self.username, self.password)
            response = self.__client.post(url, auth=auth, json=data, headers=headers, timeout=self.timeout, verify=False, proxies=self.proxies)
            logger.debug('POST %s JSON: %s' % (url, data))
        except (ConnectionError, ConnectTimeout, ReadTimeout) as e:
            logger.error (str(e))
            return False
        except TypeError as e:
            # workaround for urllib3 Retry() bug
            logger.error (str(e.__context__))
            return False
        if response.status_code == 403:
            if not self.__recursion:
                self.__recursion = True
                self.__csrf = None
                return self.__post(data, url)
            else:
                logger.error('Server %s forbids POST requests. Check plugin and server configuration (eg. authentication).' % (config.baseurl))
                self.__recursion = False
        return (response.status_code, response.text)

    def post(self, data):
        if not data:
            return False
        first = data[0]
        last = data[-1]
        response = self.__post(data, self.drain)
        if response:
            status, text = response
            logger.info('POST client id: %s timestamp interval: %s ~ %s measures: %d HTTP status code: %s %s' % (first['client_id'], first['at'], last['at'], len(data), status, text.splitlines()))
        else:
            return False
        if status in (200, 201, 409):
            return True
        return False


class JApp():

    def __init__(self):
        self.__threads = {}
        self.__plugins = []
        path = os.path.join(os.getcwd(), __name__, 'plugins')
        logger.debug('%s plugins path %s' % (__title__, path))
        modules = pkgutil.iter_modules(path = [path])
        for loader, name, ispkg in modules:
            try:
                loaded_mod = __import__('jackal.plugins.%s' % name, fromlist=[name])
            except SyntaxError as e:
                logger.warning('Plugin %s disabled: %s' % (name, str(e)))
                continue
            if name in config.sections():
                logger.debug('Section %s found in configuration' % name)
            else:
                logger.debug('Section %s not found in configuration' % name)
                logger.info('Plugin %s disabled: not configured' % name)
                continue
            try:
                clientid = config.get(name, 'clientid')
                okdir = config.get(name, 'okdir')
                basedir = os.path.normpath(config.get(name, 'basedir'))
                kodir = os.path.normpath(config.get(name, 'kodir'))
                pattern = os.path.normpath(config.get(name, 'pattern'))
                inotify = config.getboolean(name, 'inotify', fallback=False)
                if not self.checkdir(basedir):
                    logger.warning('Plugin %s disabled' % name)
                    continue
                if not os.path.isabs(okdir):
                    okdir = os.path.join(basedir, okdir)
                if not self.checkdir(okdir):
                    logger.warning('Plugin %s disabled' % name)
                    continue
                if not os.path.isabs(kodir):
                    kodir = os.path.join(basedir, kodir)
                if not self.checkdir(kodir):
                    logger.warning('Plugin %s disabled' % name)
                    continue
            except configparser.NoOptionError as e:
                logger.warning('Plugin %s disabled: %s' % (name, str(e)))
                continue
            logger.info('Loading plugin %s' % name)
            loaded_class = getattr(loaded_mod, name)
            loaded_class.clientid = clientid
            loaded_class.name = name
            loaded_class.basedir = basedir
            loaded_class.okdir = okdir
            loaded_class.kodir = kodir
            loaded_class.pattern = pattern
            loaded_class.inotify = inotify
            self.__plugins.append(loaded_class())
        if not self.__plugins:
            logger.critical('No plugins enabled!')

    def run(self):
        schedule.clear()
        schedule.every(config.interval).seconds.do(self.periodic)
        schedule.every().day.do(self.update)
        self.periodic()
        while True:
            schedule.run_pending()
            time.sleep(1)

    def periodic(self):
        for thread in self.__threads:
            self.__threads[thread].join()
            self.__threads[thread].stop()
        logger.debug('Processing threads running')
        self.__threads = {}
        for plugin in self.__plugins:
            self.__threads[plugin] = JThread(plugin)
            self.__threads[plugin].start()
        for plugin in self.__plugins:
            self.__threads[plugin].join()
        logger.debug('Processing threads ended')

    def checkdir(self,directory):
        if not os.path.exists(directory):
            logger.warning('Directory %s does not exist' % directory)
            return False
        if not os.access(directory, os.R_OK | os.W_OK):
            logger.warning('Directory %s is not readable and writable by uid/gid %d:%d' % (directory, os.getuid(), os.getgid()))
            return False
        return True

    def update(self, force=False):
        update = JWebUpdate()
        if update.update(force=force):
            logger.info('%s updated, exiting and waiting systemd restart...' % __name__)
            for thread in self.__threads:
                self.__threads[thread].join()
            os._exit(0)

class JThread(threading.Thread, pyinotify.ProcessEvent):

    def __init__(self, plugin):
        threading.Thread.__init__(self)
        self.plugin = plugin
        self.name = plugin.name
        self.wm = pyinotify.WatchManager()
        self.notifier = pyinotify.ThreadedNotifier(self.wm, self)
        self.notifier.name = plugin.name
        if plugin.inotify:
            logger.debug('Watching directory "%s" for %s' % (plugin.basedir, plugin.pattern))
            self.notifier.start()
            self.wm.add_watch(plugin.basedir, pyinotify.IN_CLOSE_WRITE)

    def run(self):
        logger.debug('Plugin %s thread started' % self.plugin.name)
        infiles = glob.glob(os.path.join(self.plugin.basedir, self.plugin.pattern))
        for infile in infiles:
            self.process_file(infile)
        logger.debug('Plugin %s thread ended' % self.plugin.name)

    def process_file(self, infile):
        okdir = self.plugin.okdir
        kodir = self.plugin.kodir
        logger.info('Processing "%s"' % infile)
        with open(infile, 'r') as f:
            buf = f.read()
        f.close()
        try:
            data = self.plugin.parse(buf)
        except (IndexError, ValueError, AttributeError) as e:
            logger.error('Cannot parse "%s", moving into "%s" (%s)' % (infile, kodir, str(e)))
            os.rename(infile, os.path.join(kodir, os.path.basename(infile)))
            return
        if rest.post(data):
            logger.info('Moving "%s" into "%s"' % (infile, okdir))
            os.rename(infile, os.path.join(okdir, os.path.basename(infile)))
        else:
            logger.error('Cannot send "%s" data to server, moving into "%s"' % (infile, kodir))
            os.rename(infile, os.path.join(kodir, os.path.basename(infile)))

    def process_IN_CLOSE_WRITE(self, event):
        if not event.dir and event.path == self.plugin.basedir:
            infile = os.path.join(event.path, event.name)
            if fnmatch.filter([infile], self.plugin.pattern):
                logger.info('New or modified file detected: "%s"' % infile)
                self.process_file(infile)

    def stop(self):
        if self.notifier.ident:
            logger.debug('Unwatching directory "%s" for %s' % (self.plugin.basedir, self.plugin.pattern))
            self.notifier.stop()

class JWebUpdate():

    def __init__(self):
        self.api = 'https://api.github.com/repos'
        self.repo = {'channel': 'releases', 'import': 'jackal', 'repository': 'myna-project/Jackal', 'version': __version__}

    def __unzip(self, buffer, dest):
        if not dest:
            return 0
        q = queue.Queue()
        try:
            zip=zipfile.ZipFile(buffer)
        except zipfile.BadZipFile as e:
            logger.error('Downloaded archive corrupted, cannot update!')
            return 0
        if zip.testzip():
            logger.error('Downloaded archive corrupted, cannot update!')
            return 0
        logger.info('Downloaded archive test OK')

        rootdir = zip.filelist[0].filename
        # extract only missing or differing files, only of destination directory
        for f in zip.infolist():
            filename = f.filename.replace(rootdir, '')
            if not filename or not filename.startswith(dest):
                continue
            f.filename = filename
            if f.is_dir():
                if not os.path.isdir(filename):
                    logger.debug('Extracting %s' % filename)
                    q.put(zip.extract(f))
            if not f.is_dir():
                if not os.path.isfile(filename):
                    logger.debug('Extracting %s' % filename)
                    q.put(zip.extract(f))
                else:
                    crc32 = zlib.crc32(open(filename,"rb").read())
                    if crc32 != f.CRC:
                        logger.debug('Overwriting %s' % filename)
                        q.put(zip.extract(f))

        # cleanup destination directory of possible aliens/older files
        filelist = glob.glob('%s/**' % dest, recursive=True)
        namelist = [os.path.relpath(x) for x in zip.namelist()]
        for f in filelist:
            if not os.path.relpath(f) in namelist:
                logger.debug('Removing %s' % f)
                if os.path.isdir(f):
                    q.put(shutil.rmtree(f, ignore_errors=True))
                if os.path.isfile(f):
                    q.put(os.unlink(f))
        return q.qsize()

    def update(self, force=False):
        repo = self.repo
        client = requests.session()
        try:
            response = client.get('%s/%s/%s' % (self.api, repo['repository'], repo['channel']))
        except (ConnectionError, ConnectTimeout, ReadTimeout) as e:
            logger.error('Cannot check Github repository: %s' % str(e))
            return False
        gitresp = json.loads(response.text)
        if response.status_code == 404:
            logger.error('Repository %s not found, cannot update!' % repo['repository'])
            return False
        if response.status_code != 200:
            logger.error('Cannot check Github repository: %s' % gitresp['message'])
            return False
        if not gitresp:
            logger.error('Repository %s empty, cannot update!' % repo['repository'])
            return False
        gitlast = gitresp[0]
        logger.debug('Repository %s (online version: %s installed version: %s)' % (repo['repository'], gitlast['name'], repo['version']))
        if (gitlast['name'] != repo['version']) or force:
            logger.info('Online repository %s newer than local %s: updating...' % (repo['repository'], __name__))
            url = gitlast['zipball_url']
            logger.debug('Downloading %s' % url)
            try:
                response = client.get(url)
            except (ConnectionError, ConnectTimeout, ReadTimeout) as e:
                logger.error('Cannot download %s %s' % (url, str(e)))
                return False
            if response.status_code != 200:
                logger.error('Cannot download %s %s' % (url, response.text.splitlines()))
                return False
            buffer = io.BytesIO(response.content)
            logger.debug("Downloaded %s size: %d bytes" % (gitlast['zipball_url'], buffer.getbuffer().nbytes))
            if self.__unzip(buffer, repo['import']):
                logger.info('%s up to date' % __name__)
                return True
        else:
            logger.info('%s already up to date' % __name__)
        return False

# Signals handlers

def terminate(signum=None, frame=None):
    logger.info("Exiting.")
    os._exit(0)

signal.signal(signal.SIGHUP, terminate)
signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)

# Init main app

def main(argv=None):
    global config, logger, rest

    # Logging setup
    logging.setLoggerClass(JLogger)
    logger = logging.getLogger(__name__)
    logger.info('%s starting up as %s' % (__title__, __name__))

    # Check and read config file
    config = JConfig()
    config.read('%s/%s.ini' % (os.getcwd(), __name__))

    # Logging level setup
    logger.setup()

    # Init REST APIs
    rest = JRest()

    app = JApp()
    app.run()

if __name__== "__main__":
    main()
