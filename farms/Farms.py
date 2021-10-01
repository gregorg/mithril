import logging
import json
import urllib3
import datetime
from pprint import pprint

farms = {
    "hiveos": "https://api2.hiveos.farm/api/v2"
}


class Farm:
    DEFAULT_POWER_PRICE = 0.15
    def __init__(self, idb, customer):
        self.idb = idb
        self.customer = customer
        self.points = []
        self.workers = []
        self.hashrate = 0
        self.power = 0
        self.avg_power_price = 0
        self.currency = '€'
        self.total_power_costs = 0




class HiveOs(Farm):
    def __init__(self, idb, customer, token):
        super().__init__(idb, customer)
        self.token = token
        self.url = None
        self.farms = {}
        self.set_url()

    def fetch(self):
        if self.query("/auth/check") == False:
            logging.warning("HiveOs auth check failed")
            return False

        farms = self.json("/farms")
        for farm in farms:
            self.farms[farm['name']] = {}
            self.power += farm['stats']['power_draw']
            try:
                farm['power_price']
            except KeyError:
                farm['power_price'] = self.DEFAULT_POWER_PRICE
                logging.warning("%s farm doesn't have power price set., set it: https://the.hiveos.farm/farms/%s/settings", farm['name'], farm['id'])
            
            self.avg_power_price += farm['power_price']

            try:
                if farm['power_price_currency'] == 'DOL':
                    self.currency = '$'
            except KeyError: pass
            for k in ('power_price', 'power_price_currency', 'workers_count', 'rigs_count'):
                try:
                    self.farms[farm['name']][k] = farm[k]
                except KeyError:
                    pass

            try:
                hr = 0
                for hrc in farm['hashrates_by_coin']:
                    hr += hrc['hashrate']
                    #self.farms[farm['name']]['avg_efficiency'][hrc['coin']] = hrc['hashrate'] / farm['stats']['power_draw']
                    self.hashrate += hrc['hashrate']
                self.farms[farm['name']]['avg_efficiency'] = hr / farm['stats']['power_draw']
            except KeyError:
                pass

            try:
                self.farms[farm['name']]['power_draw'] = farm['stats']['power_draw']
                self.farms[farm['name']]['gpus'] = farm['stats']['gpus_total']
                self.farms[farm['name']]['gpus_online'] = farm['stats']['gpus_online']
                self.farms[farm['name']]['gpus_offline'] = farm['stats']['gpus_offline']
                try:
                    if farm['stats']['power_cost'] > 0:
                        self.farms[farm['name']]['power_cost_per_hour'] = farm['stats']['power_cost']
                except KeyError: pass
                self.farms[farm['name']]['power_cost_per_month'] = farm['stats']['power_draw'] / 1000 * farm['power_price'] * 24 * 30
                self.total_power_costs += self.farms[farm['name']]['power_cost_per_month']
            except KeyError:
                logging.warning("key error", exc_info=True)
                pass
            workers = self.json("/farms/%s/workers"%farm['id'])
            for worker in workers:
                try:
                    gpus = len(worker['gpu_stats'])
                    hashrate = int(sum([a['hash'] for a in worker['gpu_stats']]) / 1000)
                    power = int(sum([a['power'] for a in worker['gpu_stats']]))
                    efficiency = int(hashrate / power * 1000)
                except KeyError:
                    continue
                self.workers.append({
                    "name": worker['name'],
                    "gpus": gpus,
                    "hms": hashrate,
                    "power": power,
                    "efficiency": efficiency
                })
                self.points.append({
                    "measurement": "workers",
                    "tags": {
                        "farm": farm['name'],
                        "name": worker['name']
                    },
                    "fields": {
                        "gpus": gpus,
                        "hms": hashrate,
                        "power": power,
                        "efficiency": efficiency
                    }
                })
            self.points.append({
                "measurement": "farms",
                "tags": {
                    "farm": farm['name']
                },
                "fields": self.farms[farm['name']]
                })

        # Python divizion ???
        self.avg_power_price = (self.avg_power_price * 100) / len(farms) / 100

        logging.info("%s: %dMH/s for %dW, avg power price is %.02f%s", self.customer, self.hashrate / 1000, self.power, self.avg_power_price, self.currency)
        logging.info("Efficiency is %dkH/W, Power costs %d%s", self.hashrate/self.power, self.total_power_costs, self.currency)
        self.points.append({
            "measurement": "customers",
            "tags": {
                "currency": self.currency,
                "farm_type": "hiveos"
            },
            "fields": {
                "avg_power_price": self.avg_power_price,
                "hashrate": int(self.hashrate/1000),
                "power": self.power,
                "efficiency": self.hashrate/self.power,
                "total_power_costs": self.total_power_costs
            }
        })
        self.enrich_points()
        self.idb.write_points(self.points, time_precision='h', retention_policy='autogen')

    def enrich_points(self):
        tags = {
            "customer": self.customer,
        }
        for point in self.points:
            try:
                point["tags"].update(tags)
            except KeyError:
                point["tags"] = tags

    def query(self, uri):
        try:
            http = urllib3.PoolManager()
            resp = http.request(
                    'GET',
                    self.url + uri,
                    headers = {
                        "Authorization": "Bearer %s"%self.token,
                        "Accept": "application/json"
                    }
            )
            if resp.status in (200, 204):
                return resp.data
            else:
                return False
        except:
            logging.warning("Unable to query: %s", self.url, exc_info=True)
            raise


    def set_url(self):
        self.url = farms["hiveos"]
        logging.debug("%s / %s / %s", self.customer, self.token, self.url)

    def json(self, uri):
        try:
            data = json.loads(self.query(uri))
            return data['data']
        except:
            logging.warning("Unable to decode query: %s", self.url, exc_info=True)
            return {}









class StaticWorkers(Farm):
    def __init__(self, idb, customer, config, workers):
        super().__init__(idb, customer)
        self.config = config
        self.pool_workers = workers

    def fetch(self):
        farm = "static"
        for worker in self.config:
            gpus = 1
            hashrate = None
            power = None
            efficiency = None
            try:
                gpus = self.config[worker]['gpus']
            except KeyError: pass

            try:
                hashrate = self.pool_workers[worker]
                logging.warning("Found %s for %s", hashrate, worker)
            except KeyError:
                try:
                    hashrate = self.config[worker]['hashrate']
                except KeyError:
                    hashrate = 0
            self.hashrate += hashrate

            try:
                power = self.config[worker]['power']
                self.power += power
                self.avg_power_price = self.config[worker]['power_price']
            except KeyError: pass
            try:
                efficiency = int(hashrate / power * 1000)
            except: pass
            self.workers.append({
                "name": worker,
                "gpus": gpus,
                "hms": hashrate,
                "power": power,
                "efficiency": efficiency
            })
            self.points.append({
                "measurement": "workers",
                "tags": {
                    "customer": self.customer,
                    "name": worker,
                    "farm": farm
                },
                "fields": {
                    "gpus": gpus,
                    "hms": hashrate,
                    "power": power,
                    "efficiency": efficiency
                }
            })

        self.total_power_costs = self.avg_power_price * self.power
        self.points.append({
            "measurement": "customers",
            "tags": {
                "customer": self.customer,
                "farm_type": "static",
                "currency": self.currency
            },
            "fields": {
                "avg_power_price": self.avg_power_price,
                "hashrate": int(self.hashrate/1000),
                "power": self.power,
                "efficiency": self.hashrate/self.power,
                "total_power_costs": self.total_power_costs
            }
        })
        self.idb.write_points(self.points, time_precision='h', retention_policy='autogen')

