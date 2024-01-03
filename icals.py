import requests
import pandas as pd
import requests
import datetime as dt
from http import HTTPStatus

Week_days = {
    'MO': 0,
    'TU': 1,
    'WE': 2,
    'TH': 3,
    'FR': 4,
    'SA': 5,
    'SU': 6
}

# скорее всего, надо будет сделать словарь,
# ключь - это название атрибута события,
# занчения - вкл/выкл (True/False),
# парсить/не парсить
Attributes_list = [
    'DTEND',
    'RRULE',
    'SEQUENCE',
    'STATUS',
    'SUMMARY',
    'DESCRIPTION'
]

def txt_to_list(cal):
    replacements = {'\\n': "", '\r\n': "\n", ';': ':'}

    for old, new in replacements.items():
        cal = cal.replace(old, new)

    # Разбиваем строки на элементы словаря, что бы перебрать их в цикле
    cal_row = cal.split('\n')

    return cal_row

def event_field_parce(row, is_it_event, dictionary):
    '''
    Разбирает информацию о событии по соответсвующим полям.
    is_it_event - если True, то анализируемые строкии относятся к событиям
    '''
    colon_position = row.find(':')
    field_type = row[:colon_position]
    field_value = row[colon_position + 1:]

    if any(attribute in row for attribute in Attributes_list):
        if is_it_event:
            dictionary[f'{field_type}'] = f'{field_value}'
    # Нужен RETURN !!!!!





# ------------------------ The Class is starts here ----------------------
class Calendar:

    row = None # Text from file
    list = [] # temp attr

    prodid = None
    version = None
    calscale = 'GREGORIAN'
    method = 'PUBLISH'
    cal_timezone = None
    name = None # X-WR-CALNAME

    events = []

    def get(self, path):
        '''

        :param path:
        :return:
        '''

        response = requests.get(path)
        if not response.status_code == HTTPStatus.OK:
            print('Response code:', response.status_code())
            return
        else:
            self.row = response.text

        list = txt_to_list(self.row)
        self.list = [i.split(":") for i in list]

    def parce_list(self,):
        is_it_event = False # for events parcenig

        for i in self.list:
            if not is_it_event:
                match i[0]:
                    case 'PRODID':
                        self.prodid = i[1]
                    case 'VERSION':
                        self.version = i[1]
                    case 'CALSCALE':
                        self.calscale = i[1]
                    case 'METHOD':
                        self.calscale = i[1]
                    case 'X-WR-TIMEZONE':
                        self.cal_timezone = i[1]
                    case 'X-WR-CALNAME':
                        self.name = i[1]
                    case 'BEGIN':
                        if i[1] == 'VEVENT':
                            is_it_event = True
                            event = {}
            elif i[0] == 'END':
                self.events.append(event)
                is_it_event = False
            else:
                event[i[0]] = i[1]




