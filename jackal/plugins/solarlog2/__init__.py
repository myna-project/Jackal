import csv
import datetime
from dateutil import tz

class solarlog2():

    def __init__(self):
        self.clientid = getattr(self, 'clientid', '-1')

    def parse(self, buf):
        lines = csv.reader(buf.splitlines(), delimiter=';')
        lines = list(lines)
        requests = []
        method = 'POST'
        names = ['Pac', 'DaySum', 'Status', 'Error', 'Pdc1', 'Pdc2', 'Udc1', 'Udc2', 'Temp']
        l = len(names) + 1
        for row in lines[1:]:
            (date, time) = row[0:2]
            datestring = '%s %s' % (date, time)
            dt = datetime.datetime.strptime(datestring, '%d/%m/%y %H:%M:%S')
            ts = dt.replace(tzinfo=tz.gettz()).isoformat()
            row = [int(x) for x in row[2:]]
            for offset in range(0, len(row), l):
                wr = int(row[offset:offset + l][0])
                measures = []
                for idx, name in enumerate(names):
                    value = row[offset:offset + l][idx + 1]
                    # Wh -> KWh
                    if name == 'DaySum':
                        value *= 0.001
                    measures.append({'measure_id': name, 'value': value})
                json = {'client_id': self.clientid, 'at': ts, 'device_id': wr, 'measures': measures}
                requests.append(json)
        return requests, method
