import requests
import pandas as pd
import requests
from datetime import datetime

def event_field_parce(row, is_it_event, dictionary):
    '''
    Разбирает информацию о событии по соответсвующим полям.
    is_it_event - если True, то анализируемые строкии относятся к событиям
    '''
    colon_position = row.find(':')
    field_type     = row[:colon_position]
    field_value    = row[colon_position+1:]

    # Это в временный колхоз
    if  (
            ('DTSTART'	in row) or
            ('DTEND'	in row) or
            ('RRULE'	in row) or
            # ('DTSTAMP'	in row) or
            ('SEQUENCE'	in row) or
            ('STATUS'	in row) or
            ('SUMMARY'	in row)
    ):

        if is_it_event:
            dictionary[f'{field_type}'] = f'{field_value}'


def get_cal(url):
    '''
    :param url: - ссылка на файл календаря
    :return: - список собыйтий с полями ввиде словаря
    '''

    # После того, как я поствали тройные кавычки и нажал Enter, IDE сама добавила в комментарий строки :param url: и :return:
    # Это какие-то стандартыоформления функции?

    response = requests.get(url)
    if response.status_code == 200:

        content = response.text

        # стоит ли эту часть переписать в цикл по словарю, 'что заменить':'на что заменить' ?
        content = content.replace('\\n', "")
        content = content.replace('\r\n', "\n")
        content = content.replace(';', ':')
        content = content.replace(';', ':')

        # Разбиваем строки на элементы словаря, что бы перебрать их в цикле
        cont_row = content.split('\n')

        events = []

        # Создадим признак, который будет говорить о том, что сейчас мы проходимся по строкам события.
        # Событие начинается со строки с текстом BEGIN:VEVENT'
        # И заканчивается строкой с тестом END:VEVENT
        is_it_event = False

        for row in cont_row:

            if row == 'BEGIN:VEVENT':
                is_it_event = True
                event = {}

            elif  row == 'END:VEVENT':
                events.append(event)
                is_it_event = False

            elif is_it_event:
                event_field_parce(row, is_it_event, event)

        return events
    else:
        print('Response code:', response.status_code() )

def open_cals_file(path_to_file):
    cals_dict = {}
    with open(path_to_file, 'r', encoding='utf-8' ) as cals_txt:
        for line in cals_txt:
            key, value = line.split('$')
            cals_dict[key] = value

        for cal, url in cals_dict.items():
            cals_dict[cal] = url.replace('\n', '')

    return cals_dict

def create_schedule(cal, st, end):

    # Переводим словарь в DataFram, так будет проще работать
    df = pd.DataFrame(cal)
    df.columns = map(str.lower, df.columns)

    # Если строка поле 'dtstart'содержат VALUE, то это не события, а задачи.
    # А задачи нам пока не нужны
    df = df[~df['dtstart'].str.contains('VALUE')]

    for col in ['dtstart', 'dtend']:
        # Т.к. некоторые даты имеют вид: 20150610T145500Z, а некоторые: TZID=Asia/Vladivostok:20150609T180000 или VALUE=DATE:20150614
        # Приведём всё к одному виду, отрезав лишнее (лишнее то, что до ':')
        df[f'{col}'] = df[f'{col}'].apply(lambda x:  x.split(':')[1] if len(x.split(':'))>1 else x )
        df[f'{col}'] = df[f'{col}'].str.replace('Z', '')
        df[f'{col}'] = pd.to_datetime(df[f'{col}'], format='%Y%m%dT%H%M%S')

    df['duration'] = (df['dtend'] - df['dtstart']).apply(lambda x: x.seconds / 3600)
    return df[ (df['dtstart'] >= st) & (df['dtend'] <= end) ]

