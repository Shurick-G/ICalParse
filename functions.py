import requests
import pandas as pd
import requests
import datetime as dt

Week_days = {
    'MO':0,
    'TU':1,
    'WE':2,
    'TH':3,
    'FR':4,
    'SA':5,
    'SU':6
}

def event_field_parce(row, is_it_event, dictionary):
    '''
    Разбирает информацию о событии по соответсвующим полям.
    is_it_event - если True, то анализируемые строкии относятся к событиям
    '''
    colon_position = row.find(':')
    field_type = row[:colon_position]
    field_value = row[colon_position+1:]

    # Это в временный колхоз
    if (
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

    response = requests.get(url)
    if response.status_code == 200:

        content = response.text

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

            elif row == 'END:VEVENT':
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

def dict_to_df(cal):

    # Переводим словарь в DataFrame, так будет проще работать
    df = pd.DataFrame(cal)
    # Понижаем символы в названии полей DataFrame'а
    df.columns = map(str.lower, df.columns)

    # Если строка поле 'dtstart'содержат VALUE, то это событие запланировано не на конкретное время, а на весь день
    # На данный момент они нам не нужны
    df = df[~df['dtstart'].str.contains('VALUE')]

    # Преобразования дат в читаемый формат
    for col in ['dtstart', 'dtend']:

        # Т.к. некоторые даты имеют вид: 20150610T145500Z, а некоторые: TZID=Asia/Vladivostok:20150609T180000
        # Приведём всё к одному виду, убрав лишнее (лишнее то, что до ':')
        df[f'{col}'] = df[f'{col}'].apply(lambda x:  x.split(':')[1] if len(x.split(':')) > 1 else x)

        df[f'{col}'] = df[f'{col}'].str.replace('Z', '')

        # Создадим столбцы со временем начала и окончания мероприятия
        # Это пригодиться при создании событий по правилам. Оставим в текстовом формате
        df[f'{col}_time'] = df[f'{col}'].apply(lambda x:  x.split('T')[1])

        df[f'{col}'] = pd.to_datetime(df[f'{col}'], format='%Y%m%dT%H%M%S')

    # Создадим поле только с датой для дальнейшего матчинга с "таймлайном"
    df['st_date'] = df['dtstart'].apply(lambda x: x.date())
    df['duration'] = (df['dtend'] - df['dtstart']).apply(lambda x: x.seconds / 3600)

    return df


def create_schedule(cal, st, end): # wor - without rules

    df = dict_to_df(cal)

    # Создаём пустой DataFrame c DatatimeIndex
    timeline = pd.date_range(st, end)
    bord = pd.DataFrame(timeline, columns=['timeline'])
    bord['dayofweek'] = bord['timeline'].dt.dayofweek
    bord['month'] = bord['timeline'].dt.month
    bord['day'] = bord['timeline'].dt.day

    # return df[ (df['dtstart'] >= st) & (df['dtend'] <= end) ]
    return bord
    # return df


def rule_to_dict(rule):
    # Это функция будет подставляться к каждой ячейки столбца с помощью lambda функции

    rule_dict = {}              # dict to return
    rule_list = rule.split(':') # Разобьём текст на список правил

    for i in rule_list:
        key, value = i.split('=')
        rule_dict[key] = value

    return rule_dict


def make_row_by_day(e, temp_bord, day):

    rule_bord = temp_bord.query(f" dayofweek == {day}")

    for field in e.index:
        rule_bord[field] = e[field]

    return rule_bord

def make_row_by_working_day(e, temp_bord):
    rule_bord = temp_bord.query(f" dayofweek in (0, 1, 2, 3, 4)")

    for field in e.index:
        rule_bord[field] = e[field]

    return rule_bord


def events_by_rules(df, st, end):
    # select events containing rules
    df = df[~df['rrule'].isna()]
    # parse string rule to dict
    df['rrule'] = df['rrule'].apply(lambda x: rule_to_dict(x))

    # Создаём пустой DataFrame c DatatimeIndex
    timeline = pd.date_range(st, end)
    bord = pd.DataFrame(timeline, columns=['timeline'])
    bord['dayofweek'] = bord['timeline'].dt.dayofweek
    bord['month'] = bord['timeline'].dt.month
    bord['day'] = bord['timeline'].dt.day

    # for e in df.iterrows():
    #     if e
    # !!!!! Не забудь пределать df.loc[4]. См. две закомментированные строки выше
    e = df.iloc[4, :] # event
    rule = e['rrule']

    temp_st = dt.datetime.combine(e['st_date'], dt.time.min) # Both datetime.time() and datetime.time.min represent midnight (00:00:00).

    # creating a Data Frame, in which we will add events created by the rule
    rules_events = pd.DataFrame()

    # Проверяем есть ли правило "UNTIL"
    if 'UNTIL' in rule:
        until = rule['UNTIL']
        until = until.replace('Z', '')
        until = dt.datetime.strptime(until, '%Y%m%dT%H%M%S')

        # temp_bord = temp_bord[
        #     (temp_bord['timeline'] >= temp_st)
        #     &
        #     (temp_bord['timeline'] < until)
        #     ]

        bord = bord[
            (bord['timeline'] >= temp_st)
            &
            (bord['timeline'] < until)
            ]

    # Need comment
    match rule['FREQ']:
        case 'DAILY':
            for field in e.index:
                bord[field] = e[field]

        case 'WEEKLY':
            # check whether this rule is the rule of the working week
            if 'WKST' in rule.keys():
                row = make_row_by_working_day(e, bord)
                bord = pd.concat([rules_events, row], axis=0)
            else:
                rule['BYDAY'] = rule['BYDAY'].split(',')

                if type( rule['BYDAY'] ) is list:
                    for day in rule['BYDAY']:

                        row = make_row_by_day(e, bord, Week_days[day])
                        rules_events = pd.concat([rules_events, row], axis=0)

                    return rules_events

        case 'MONTHLY':






    # Replace values in 'dtstart' and 'dtend' columns

    # Delete extr columns

    return bord


#%%

#%%
