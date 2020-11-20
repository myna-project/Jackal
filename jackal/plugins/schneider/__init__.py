import csv
import datetime
from dateutil import tz

class schneider():

    def __init__(self):
       self.clientid = getattr(self, 'clientid', '-1')

    def parse(self, buf):
        lines = csv.reader(buf.splitlines(), delimiter=';')
        lines = list(lines)
        requests = []
        method = 'POST'
        (gwname, gwns, gwip, gwmac, devname, devid, devtyp, devtypname, time, cron) = lines[1]
        bulks = lines[4][3:]
        for row in lines[7:]:
            (err, diff, dt) = row[0:3]
            row = [float(x.replace(",", ".")) for x in row[3:]]
            dt = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
            dt = dt.replace(tzinfo=tz.tzoffset(None, int(diff)*60))
            ts = dt.isoformat()
            measures = []
            measures.append({'measure_id': 'Errore', 'value': int(err)})
            for id in range(0, len(row)):
                measures.append({'measure_id': bulks[id], 'value': row[id]})
            json = {'client_id': self.clientid, 'at': ts, 'device_id': int(devid), 'measures': measures}
            requests.append(json)
        return requests, method
