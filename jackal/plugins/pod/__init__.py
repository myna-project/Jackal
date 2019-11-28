from datetime import datetime, timedelta
from lxml import etree, objectify
from tzlocal import get_localzone
import re


class pod():

    def __init__(self):
       self.clientid = getattr(self, 'clientid', '-1')

    def parse(self, buf):
        # delete xml header
        buf = re.sub(r'^[^\n]*\n', '', buf)
        # replaces decimal separator
        buf = re.sub(r'(\d+),(\d+)', '\\1.\\2', buf)
        try:
            tree = objectify.fromstring(buf)
        except etree.XMLSyntaxError as e:
            raise ValueError(e)

        Pod = str(tree.DatiPod.Pod)
        MeseAnno = str(tree.DatiPod.MeseAnno)
        PotDisp = float(tree.DatiPod.DatiPdp.PotDisp)
        PotMax = float(tree.DatiPod.Curva.PotMax)
        TipoDato = tree.DatiPod.Curva.TipoDato
        if TipoDato == 'S':
            return []

        # e.tag -> Ea or Er
        # e.text -> day of month
        # e.attrib -> quarter of an hour datas
        eas = {}
        ers = {}
        for e in tree.DatiPod.Curva.getchildren():
            if e.tag == 'Ea':
                eas[e.text] = eas.setdefault(e.text, {})
                eas[e.text][e.attrib['Dst']] = e.attrib
                eas[e.text][e.attrib['Dst']].pop('Dst',None)
            if e.tag == 'Er':
                ers[e.text] = ers.setdefault(e.text, {})
                ers[e.text][e.attrib['Dst']] = e.attrib
                ers[e.text][e.attrib['Dst']].pop('Dst',None)

        requests = []

        tz = get_localzone()

        # first measure for PotDisp and PotMax
        dt = datetime.strptime('01/%s' % MeseAnno, '%d/%m/%Y')
        ts = tz.normalize(tz.localize(dt)).isoformat()

        measures = []
        measures.append({'measure_id': 'PotDisp', 'value': PotDisp})
        measures.append({'measure_id': 'PotMax', 'value': PotMax})
        json = {'client_id': self.clientid, 'at': ts, 'device_id': Pod, 'measures': measures}
        requests.append(json)

        for day in eas:
            dt = datetime.strptime('%s/%s' % (day, MeseAnno), '%d/%m/%Y')
            td = timedelta()
            for dst in eas[day]:
                self.__validate(dst, day, len(eas[day][dst]), 'Ea')
                self.__validate(dst, day, len(ers[day][dst]), 'Er')
                for e in eas[day][dst]:
                    ts = tz.normalize(tz.localize(dt) + td).isoformat()
                    ea = eas[day][dst][e]
                    er = ers[day][dst][e]
                    measures = []
                    measures.append({'measure_id': 'Ea', 'value': float(ea)})
                    measures.append({'measure_id': 'Er', 'value': float(er)})
                    json = {'client_id': self.clientid, 'at': ts, 'device_id': Pod, 'measures': measures}
                    requests.append(json)
                    td += timedelta(minutes=15)

        return requests

    def __validate(self, dst, day, length, measure):
        if (dst == '0' and length != 96):
            raise ValueError('Invalid format: %d %s values instead of %d for Dst %s day %s' % (length, measure, 96, dst, day))
        if (dst == '1' and length != 92):
            raise ValueError('Invalid format: %d %s values instead of %d for Dst %s day %s' % (length, measure, 96, dst, day))
        if (dst == '2' and length != 12):
            raise ValueError('Invalid format: %d %s values instead of %d for Dst %s day %s' % (length, measure, 96, dst, day))
        if (dst == '3' and length != 88):
            raise ValueError('Invalid format: %d %s values instead of %d for Dst %s day %s' % (length, measure, 96, dst, day))
        if dst not in ['0','1','2','3']:
            raise ValueError('Invalid format: unexpected Dst value: %s for %s day %s' % (dst, measure, day))
