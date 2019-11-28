import csv
from datetime import datetime, timedelta
from tzlocal import get_localzone

class deval():

    def __init__(self):
        self.clientid = getattr(self, 'clientid', '-1')

    def parse(self, buf):
        lines = csv.reader(buf.splitlines(), delimiter=';')
        lines = list(lines)
        requests = []
        for row in lines:
            (pod, num, measure_id, date, time) = row[0:5]
            dt = datetime.strptime('%s %s' % (date, time), '%d.%m.%y %H:%M')
            td = timedelta()
            tz = get_localzone()
            dst = tz.localize(dt).dst()
            self.__validate(date, time, len(row[5:]), dst)
            for col in row[5:]:
                try:
                    ts = tz.normalize(tz.localize(dt) + td).isoformat()
                    measures = []
                    measures.append({'measure_id': measure_id, 'value': float(col)})
                    json = {'client_id': self.clientid, 'at': ts, 'device_id': pod, 'measures': measures}
                    requests.append(json)
                except ValueError:
                    continue
                finally:
                    td += timedelta(minutes=15)
        return requests

    def __validate(self, date, time, length, dst):
        if time not in ('00:15', '02:15'):
            raise ValueError('Invalid format: unexpected first time value: %s date %s' % (time, date))
        if (not dst and length in [12]):
            raise ValueError('Invalid format: %d values on date %s, expected %s' % (length, date, '12'))
        elif (dst and length in [88, 92]):
            raise ValueError('Invalid format: %d values on date %s, expected %s' % (length, date, '88 or 92'))
        elif length not in [12, 88, 92, 96]:
            raise ValueError('Invalid format: %d values on date %s, expected %s' % (length, date, '96'))
