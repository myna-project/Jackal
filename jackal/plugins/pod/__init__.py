from datetime import datetime, timedelta
from lxml import etree, objectify
from tzlocal import get_localzone
import re
import os

class pod():

    def __init__(self):
        self.clientid = getattr(self, 'clientid', '-1')
        self.pods = []
        # PODs list filter
        if self.filtr and os.path.isfile(self.filtr):
            with open(self.filtr, 'r') as f:
                buf = f.read()
            f.close()
            self.pods = list(filter(None, buf.splitlines()))

    def parse(self, buf):
        # delete xml header
        buf = re.sub(r'<\?xml.*\?>', '', buf)
        # replaces decimal separator
        buf = re.sub(r'(\d+),(\d+)', '\\1.\\2', buf)
        try:
            parser = objectify.makeparser(huge_tree=True, recover=True)
            tree = objectify.fromstring(buf, parser=parser)
        except etree.XMLSyntaxError as e:
            raise ValueError(e)

        parsed = []
        requests = []
        for child in tree.iterchildren():
            measures = []
            method = 'POST'
            if child.tag == 'IdentificativiFlusso':
                continue
            Pod = str(child.Pod)
            if self.pods and Pod not in self.pods or Pod in parsed:
                continue
            parsed.append(Pod)
            if hasattr(child, 'DataMisura'):
                DataMisura = str(child.DataMisura)
                dt = datetime.strptime(DataMisura, '%d/%m/%Y')
            if hasattr(child, 'MeseAnno'):
                MeseAnno = str(child.MeseAnno)
                dt = datetime.strptime('01/%s' % MeseAnno, '%d/%m/%Y')
            tz = get_localzone()
            ts = tz.normalize(tz.localize(dt)).isoformat()

            if hasattr(child, 'Motivazione'):
                Motivazione = int(child.Motivazione)
                if Motivazione == 1:
                    method = 'POST'
                if Motivazione == 2:
                    method = 'PUT'
                if Motivazione == 3:
                    method = 'DELETE'

            if hasattr(child, 'DatiPdp'):
                self.__append(measures, 'PotDisp', self.__attr(child.DatiPdp, 'PotDisp', 'float'))
                self.__append(measures, 'Tensione', self.__attr(child.DatiPdp, 'Tensione', 'float'))

            if hasattr(child, 'Consumo'):
                self.__append(measures, 'EaM', self.__attr(child.Consumo, 'EaM', 'float'))
                self.__append(measures, 'DataInizioPeriodo', self.__attr(child.Consumo, 'DataInizioPeriodo', 'date'))

            eaer = []
            if hasattr(child, 'Curva'):
                if self.__attr(child.Curva, 'TipoDato', 'str') == 'S':
                    continue
                if self.__attr(child.Curva, 'Validato', 'str') == 'N':
                    continue
                eaer = self.__eaer(child.Curva, dt, Pod)

            if hasattr(child, 'Misura'):
                if self.__attr(child.Misura, 'TipoDato', 'str') == 'S':
                    continue
                if self.__attr(child.Misura, 'Validato', 'str') == 'N':
                    continue
                self.__append(measures, 'PotMax', self.__attr(child.Misura, 'PotMax', 'float'))
                self.__append(measures, 'EaF1', self.__attr(child.Misura, 'EaF1', 'float'))
                self.__append(measures, 'EaF2', self.__attr(child.Misura, 'EaF2', 'float'))
                self.__append(measures, 'EaF3', self.__attr(child.Misura, 'EaF3', 'float'))
                self.__append(measures, 'ErF1', self.__attr(child.Misura, 'ErF1', 'float'))
                self.__append(measures, 'ErF2', self.__attr(child.Misura, 'ErF2', 'float'))
                self.__append(measures, 'ErF3', self.__attr(child.Misura, 'ErF3', 'float'))
                self.__append(measures, 'PotF1', self.__attr(child.Misura, 'PotF1', 'float'))
                self.__append(measures, 'PotF2', self.__attr(child.Misura, 'PotF2', 'float'))
                self.__append(measures, 'PotF3', self.__attr(child.Misura, 'PotF3', 'float'))
                eaer = self.__eaer(child.Misura, dt, Pod)

            json = {'client_id': self.clientid, 'at': ts, 'device_id': Pod, 'measures': measures}
            requests.append(json)
            requests += eaer
        return (requests, method)


    def __append(self, lst, name, value):
        lst.append({'measure_id': name, 'value': value}) if value is not None else None

    def __eaer(self, tree, dt, pod):
        requests = []
        # e.tag -> Ea or Er
        # e.text -> day of month
        # e.attrib -> quarter of an hour datas
        eas = {}
        ers = {}
        for child in tree.getchildren():
            if child.tag == 'Ea':
                day = child.text
                dst = '0'
                if 'Dst' in child.attrib:
                    dst = child.attrib['Dst']
                eas[day] = eas.setdefault(day, {})
                eas[day][dst] = child.attrib
                eas[day][dst].pop('Dst', None)
            if child.tag == 'Er':
                day = child.text
                dst = '0'
                if 'Dst' in child.attrib:
                    dst = child.attrib['Dst']
                ers[child.text] = ers.setdefault(child.text, {})
                ers[child.text][dst] = child.attrib
                ers[child.text][dst].pop('Dst', None)

        tz = get_localzone()
        for day in eas:
            dt = datetime(dt.year, dt.month, int(day))
            for dst in eas[day]:
                self.__validate(dst, day, len(eas[day][dst]), 'Ea')
                self.__validate(dst, day, len(ers[day][dst]), 'Er')
                for e in eas[day][dst]:
                    td = (int(e[1:]) - 1) * timedelta(minutes=15)
                    ts = tz.normalize(tz.localize(dt + td, is_dst = bool(dst != '3'))).isoformat()
                    ea = eas[day][dst][e]
                    er = ers[day][dst][e]
                    measures = []
                    measures.append({'measure_id': 'Ea', 'value': float(ea)})
                    measures.append({'measure_id': 'Er', 'value': float(er)})
                    json = {'client_id': self.clientid, 'at': ts, 'device_id': pod, 'measures': measures}
                    requests.append(json)
        return requests


    def __attr(self, tree, name, typ):
        if hasattr(tree, name):
            value = getattr(tree, name)
            if typ == 'str':
                value = str(value)
            if typ == 'int':
                value = int(value)
            if typ == 'float':
                value = float(value)
            if typ == 'date':
                tz = get_localzone()
                value = str(value)
                if re.search('[0-3][0-9]/[0-1][0-9]/\d{4}', value):
                    pass
                elif re.search('[0-1][0-9]/\d{4}', value):
                    value = '01/%s' % value
                else:
                    raise ValueError('Invalid format for attribute %s: %s' % (name, value))
                dt = datetime.strptime(value, '%d/%m/%Y')
                value = tz.normalize(tz.localize(dt)).isoformat()
            return value

    #    Dst: 0=no DST     - potenziali 96 quarti d'ora;
    #         1=Inizio DST - potenziali 92 quarti d'ora;
    #         2=Fine DST   - Prima parte curva;
    #         3=Fine DST   - Seconda parte curva

    def __validate(self, dst, day, length, measure):
#        if (dst == '0' and length != 96):
#            raise ValueError('Invalid format: %d %s values instead of %d for Dst %s day %s' % (length, measure, 96, dst, day))
        if (dst == '1' and length != 92):
            raise ValueError('Invalid format: %d %s values instead of %d for Dst %s day %s' % (length, measure, 96, dst, day))
        if (dst == '2' and length != 12):
            raise ValueError('Invalid format: %d %s values instead of %d for Dst %s day %s' % (length, measure, 96, dst, day))
        if (dst == '3' and length != 88):
            raise ValueError('Invalid format: %d %s values instead of %d for Dst %s day %s' % (length, measure, 96, dst, day))
        if dst not in ['0','1','2','3']:
            raise ValueError('Invalid format: unexpected Dst value: %s for %s day %s' % (dst, measure, day))

