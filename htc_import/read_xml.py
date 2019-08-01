from xml.etree import ElementTree
from transaction import Transaction
import os
from os import listdir
from os.path import join, isfile
import datetime



class ReadXML:
    def __init__ (self, cfg):
        self.cfg = cfg
        self.tags = ['Count', 'Object', 'Report', 'Properties', 'Metrics']

    def read (self):
        dic_trans = dict()
        for file in os.listdir(self.cfg.folders()):
            path = os.path.join(self.cfg.folders(), file)
            if os.path.isdir(path):
                continue
            else:
                name = self.cfg.folders()
                with open(join(name, file)) as fp:
                    if file.endswith('.xml'):
                        trans = []
                        tree = ElementTree.parse(fp)
                        for node in tree.findall('.//Count'):
                            tran = Transaction()
                            tran.start_time = node.attrib.get('StartTime')
                            tran.end_time = node.attrib.get('EndTime')
                            tran.in_count = node.attrib.get('Enters')
                            tran.out_count = node.attrib.get('Exits')
                            tran.status = node.attrib.get('Status')
                            tran.file_name = file
                            trans.append(tran)
                        for node in tree.iter():
                            if node.tag == 'Metrics':
                                for tran in trans:
                                    tran.site_code = node.attrib.get('SiteId')
                                    tran.site_name = node.attrib.get('Sitename')
                                    tran.device_id = node.attrib.get('DeviceId')
                                    tran.device_name = node.attrib.get('Devicename')
                            elif node.tag == 'MacAddress':
                                for tran in trans:
                                    tran.mac_address = node.text
                            elif node.tag == 'IpAddress':
                                for tran in trans:
                                    tran.ip_address = node.text
                            elif node.tag == 'HostName':
                                for tran in trans:
                                    tran.host_name = node.text
                            elif node.tag == 'HttpPort':
                                for tran in trans:
                                    tran.http_port = node.text
                            elif node.tag == 'HttpsPort':
                                for tran in trans:
                                    tran.https_port = node.text
                            elif node.tag == 'TimezoneName':
                                for tran in trans:
                                    tran.timezone_name = node.text
                            elif node.tag == 'SerialNumber':
                                for tran in trans:
                                    tran.serial_number = node.text
                            elif node.tag == 'SwRelease':
                                for tran in trans:
                                    tran.software_version = node.text
                            elif node.tag == 'Report':
                                for tran in trans:
                                    tran.transaction_date = datetime.datetime.strptime(node.attrib.get('Date'),
                                                                                       '%Y-%m-%d')
                                    tran.week = tran.transaction_date.isocalendar()[1]
                                    tran.day = tran.transaction_date.weekday()

                            elif node.tag == 'Object':
                                for tran in trans:
                                    tran.name = node.attrib.get('Name')
                        dic_trans[str(file)] = trans
        return dic_trans
