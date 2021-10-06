import logging
import json
import urllib3
import datetime
from pprint import pprint

pools = {
    "nanopool": "https://api.nanopool.org/v1/$COIN",
    "ethermine": "https://api.ethermine.org"
}

class Pool:
    def __init__(self, idb, pool, customer, wallet, coin):
        self.idb = idb
        self.pool = pool
        self.customer = customer
        self.wallet = wallet
        self.coin = coin
        self.url = None
        self.pool = None
        self.points = []
        self.global_points = []
        self.workers = {}
        self.prices = {}
        self.payments_data = []

    def fetch(self):
        self.idb.write_points(self.global_points, time_precision='h', retention_policy='autogen')
        self.payments()
        self.account()
        self.hashrate()
        self.earnings()
        self.pool_effiency()
        self.enrich_points()
        self.idb.write_points(self.points, time_precision='h', retention_policy='autogen')

    def fetch_globals(self):
        pass

    def enrich_points(self):
        tags = {
            "customer": self.customer,
            "wallet": self.wallet,
            "coin": self.coin,
            "pool": self.pool
        }
        for point in self.points:
            try:
                point["tags"].update(tags)
            except KeyError:
                point["tags"] = tags

    def total_payments(self):
        total_payments = sum(self.payments_data)
        fields = {
            "amount": total_payments,
            "count": len(self.payments_data),
        }
        if total_payments > 0:
            for price in self.prices:
                fields[price] = total_payments * self.prices[price]
            self.points.append({
                "measurement": "agg_payments",
                "fields": fields
            })

    def query(self, uri):
        try:
            http = urllib3.PoolManager()
            resp = http.request('GET', self.url + uri)
            if resp.status == 200:
                return resp.data
            else:
                return False
        except:
            logging.warning("Unable to query: %s", self.url, exc_info=True)
            raise

    def pool_effiency(self):
        pass

    def payments(self):
        pass

    def account(self):
        pass

    def hashrate(self):
        pass

    def earnings(self):
        pass



class Nanopool(Pool):
    def __init__(self, idb, pool, customer, wallet, coin):
        super().__init__(idb, pool, customer, wallet, coin)
        self.pool = "nanopool"
        self.set_url()

    def set_url(self):
        logging.debug("Nanopool %s / %s / %s", self.customer, self.wallet, self.coin)
        self.url = pools["nanopool"].replace("$COIN", self.coin)

    def json(self, uri):
        try:
            data = json.loads(self.query(uri))
            if data['status']:
                return data['data']
        except:
            logging.warning("Unable to decode query: %s", self.url, exc_info=True)
            return {}

    def fetch_globals(self):
        prices = self.json("/prices")
        # InfluxDB type casting:
        for price in prices:
            price_label = price.replace('price_', '')
            self.prices[price_label] = float(prices[price])

        self.global_points.append({
            "measurement": "prices",
            "fields": self.prices
        })

    def payments(self):
        count = 0
        for p in self.json("/payments/%s"%self.wallet):
            if p['confirmed']:
                count += 1
                #logging.debug("Payment at %s : %f", p['date'], p['amount'])
                self.payments_data.append(p['amount'])
                self.points.append({
                    "measurement": "payments",
                    "time": datetime.datetime.fromtimestamp(p['date']),
                    "fields": {
                        "amount": p['amount']
                    }
                })
        logging.debug("%d payments", count)
        self.total_payments()


    def account(self):
        us = self.json("/usersettings/%s"%self.wallet)
        ac = self.json("/user/%s"%self.wallet)
        self.points.append({
            "measurement": "account",
            "fields": {
                'balance':		float(ac['balance']),
                'payout':		float(us['payout']),
            }
        })

        for w in ac['workers']:
            self.workers[w['id']] = int(float(w['hashrate']))
            self.points.append({
                "measurement": "pool_workers",
                "tags": {"worker": w['id']},
                "fields": {
                    'hashrate':		int(float(w['hashrate'])),
                    'avghashrate':	int(float(w['h1'])),
                }
            })


    
    def hashrate(self):
        data = self.json("/reportedhashrate/%s"%self.wallet)
        ac = self.json("/user/%s"%self.wallet)
        self.hr = int(float(data))
        self.points.append({
            "measurement": "hashrate",
            "fields": {
                "reported":     self.hr,
                'calculated':	int(float(ac['hashrate'])),
                'avg':	        int(float(ac['avgHashrate']['h1'])),
            }
        })
    
    def earnings(self):
        data = self.json("/approximated_earnings/%s"%self.hr)
        if data is None:
            return False
        self.points.append({
            "measurement": "earnings",
            "fields": {
                "month_dollars": float(data['month']['dollars']),
                "month_euros": float(data['month']['euros']),
                "day_dollars": float(data['day']['dollars']),
                "day_euros": float(data['day']['euros']),
            }
        })






class Ethermine(Pool):
    def __init__(self, idb, pool, customer, wallet, coin):
        super().__init__(idb, pool, customer, wallet, coin)
        self.pool = "ethermine"
        self.set_url()

    def set_url(self):
        logging.debug("Ethermine %s / %s / %s", self.customer, self.wallet, self.coin)
        self.url = pools[self.pool]

    def json(self, uri):
        try:
            data = json.loads(self.query(uri))
            if data['status'] == 'OK':
                return data['data']
        except:
            logging.warning("Unable to decode query: %s", self.url, exc_info=True)
            return {}

    def payments(self):
        count = 0
        for p in self.json("/miner/%s/payouts"%self.wallet):
            count += 1
            logging.debug("Payment at %s : %f", p['paidOn'], p['amount'])
            logging.warning("TODO: payments 🔜 %s"%p)
            #self.payments_data.append(p['amount'])
            ## 2021-09-23T07:31:28.000Z
            #self.points.append({
            #    "measurement": "payments",
            #    "time": datetime.datetime.strptime(p['time'], '%Y-%m-%dT%H:%M:%S.%fZ'),
            #    "fields": {
            #        "amount": p['amount']
            #    }
            #})
        logging.debug("%d payments from Ethermine", count)
        self.total_payments()


    def account(self):
        self.stats = self.json("/miner/%s/currentStats"%self.wallet)
        us = self.json("/miner/%s/settings"%self.wallet)
        self.hr = int(self.stats['reportedHashrate']/1000000)
        self.points.append({
            "measurement": "account",
            "fields": {
                'balance':		self.stats['unpaid']/1000000000000000000,
                'payout':		us['minPayout']/1000000000000000000,
            }
        })

        workers = self.json("/miner/%s/workers"%self.wallet)
        for w in workers:
            # On ASICs, reportedHashrate is 0:
            if w['reportedHashrate'] == 0:
                self.workers[w['worker']] = int(w['currentHashrate']/1000000)
            else:
                self.workers[w['worker']] = int(w['reportedHashrate']/1000000)
            self.points.append({
                "measurement": "pool_workers",
                "tags": {"worker": w['worker']},
                "fields": {
                    'hashrate':		int(w['reportedHashrate']/1000000),
                    'avghashrate':	int(w['currentHashrate'] /1000000),
                }
            })


    
    def hashrate(self):
        self.points.append({
            "measurement": "hashrate",
            "fields": {
                "reported":     self.hr,
                'calculated':	int(self.stats['currentHashrate']/1000000),
                'avg':	        int(self.stats['averageHashrate']/1000000),
            }
        })
    
    def earnings(self):
        dd = self.stats['usdPerMin'] * 60 * 24
        md = dd * 30
        me = md / self.prices['usd'] * self.prices['eur']
        de = me / 30
        self.points.append({
            "measurement": "earnings",
            "fields": {
                "month_dollars": md,
                "month_euros": me,
                "day_dollars": dd,
                "day_euros": de
            }
        })
