#!poetry run fetch
# vim: set ts=4 sw=4 expandtab:
__version__ = '0.1.0'

import tqdm
import influxdb
import argparse
import logging
import logging.config
import traceback
import os
import yaml
import re
import termcolor
import threading
import yaml
from pool import Pools
from farms import Farms


class ColoredFormatter(logging.Formatter):  # {{{
    COLORS = {"WARNING": "yellow", "INFO": "cyan", "CRITICAL": "white", "ERROR": "red"}
    COLORS_ATTRS = {
        "CRITICAL": "on_red",
    }

    def __init__(self, use_color=True):
        # main formatter:
        logformat = (
            "%(asctime)s %(threadName)14s.%(funcName)-15s %(levelname)-8s %(message)s"
        )
        logdatefmt = "%H:%M:%S %d/%m/%Y"
        logging.Formatter.__init__(self, logformat, logdatefmt)

        # for thread-less scripts :
        logformat = (
            "%(asctime)s %(module)14s.%(funcName)-15s %(levelname)-8s %(message)s"
        )
        self.mainthread_formatter = logging.Formatter(logformat, logdatefmt)

        self.use_color = use_color

    def format(self, record):
        if self.use_color and record.levelname in self.COLORS:
            if record.levelname in self.COLORS_ATTRS:
                record.msg = "%s" % termcolor.colored(
                    record.msg,
                    self.COLORS[record.levelname],
                    self.COLORS_ATTRS[record.levelname],
                )
            else:
                record.msg = "%s" % termcolor.colored(
                    record.msg, self.COLORS[record.levelname]
                )
        if threading.currentThread().getName() == "MainThread":
            return self.mainthread_formatter.format(record)
        else:
            return logging.Formatter.format(self, record)
# }}}




class Fetch:
    def __init__(self, config):
        logging.info("🦄 Starting ...")
        self.miners = config['miners']
        self.idb = influxdb.InfluxDBClient(
            host=config['general']["idb"]["host"],
            port=config['general']["idb"]["port"],
            database=config['general']["idb"]["database"],
            gzip=False,
            timeout=600)

    def fetchall(self):
        for customer in self.miners:
            logging.info("🧢 Fetching %s ...", customer)
            self.fetch(customer, self.miners[customer])

    def fetch(self, customer, config):
        workers = {}
        prices = {}
        for poolname in config['pools']:
            poolclass = getattr(Pools, config['pools'][poolname]['pool'].capitalize())
            pool = poolclass(self.idb, config['pools'][poolname]['pool'], customer, config['pools'][poolname]['wallet'], config['pools'][poolname]['coin'])
            pool.fetch_globals()

            # Share prices between nanopool and :
            if not prices:
                prices = pool.prices
            if prices and not pool.prices:
                pool.prices = prices

            pool.fetch()

            workers.update(pool.workers)
        if 'hiveos' in config:
            for token in config['hiveos']:
                hiveos = Farms.HiveOs(self.idb, customer, token)
                hiveos.fetch()
        if 'workers' in config:
            static_workers = Farms.StaticWorkers(self.idb, customer, config['workers'], workers)
            static_workers.fetch()



def main():
    try:
        parser = argparse.ArgumentParser(
            description='Mithril : mining stats fetcher',
            formatter_class=argparse.RawDescriptionHelpFormatter
        )
        parser.add_argument('--debug', action='store_true', help='DEBUG', default=True)
        args = parser.parse_args()

        with open(os.path.abspath(os.path.dirname(__file__) + '/../logging.yaml'), 'r') as f:
            logconfig = yaml.load(f, Loader=yaml.FullLoader)
            logging.config.dictConfig(logconfig)
        log = logging.getLogger('logger')
        loghandler = logging.StreamHandler()
        use_color = False
        if 'TERM' in os.environ and ( re.search("term", os.environ["TERM"]) or os.environ["TERM"] in ('screen',) ):
            use_color = True
            loghandler.setFormatter(ColoredFormatter(use_color))
            logging.root.handlers.pop()
            logging.root.addHandler(loghandler)

        if args.debug:
            logging.root.setLevel(logging.DEBUG)
            logging.getLogger('logger').setLevel(logging.DEBUG)
            logging.getLogger('dblogger').setLevel(logging.DEBUG)

        with open('config.yaml') as ycfg:
            config = yaml.load(ycfg, Loader=yaml.FullLoader)

        f = Fetch(config)
        f.fetchall()


    except SystemExit:
        print("Stopping Program")
    except Exception:
        print(traceback.format_exc())


if __name__ == "__main__":
    main()

