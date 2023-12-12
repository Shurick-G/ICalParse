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

Attributes_list = ['DTSTART', 'DTEND', 'RRULE', 'SEQUENCE', 'STATUS', 'SUMMARY', 'DESCRIPTION']

def open_cals_file(path_to_file):
    cals_dict = {}
    with open(path_to_file, 'r', encoding='utf-8') as cals_txt:
        for line in cals_txt:
            key, value = line.split('$')
            cals_dict[key] = value.replace('\n', '')

    return cals_dict

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


def get_cal(url):  # Вот тут было замечание про возвтрат функции
    '''
    :param url: - ссылка на файл календаря
    :return: - список собыйтий с полями ввиде словаря
    '''

    response = requests.get(url)
    if not response.status_code == HTTPStatus.OK:
        print('Response code:', response.status_code())
        return
    else:
        return response.text


def parse_cal(cal):
    replacements = {'\\n': "", '\r\n': "\n", ';': ':'}

    for old, new in replacements.items():
        cal = cal.replace(old, new)

    # Разбиваем строки на элементы словаря, что бы перебрать их в цикле
    cal_row = cal.split('\n')

    events = []

    # Создадим признак, который будет говорить о том, что сейчас мы проходимся по строкам события.
    # Событие начинается со строки с текстом BEGIN:VEVENT
    # И заканчивается строкой с тестом END:VEVENT
    is_it_event = False

    for row in cal_row:

        if row == 'BEGIN:VEVENT':
            is_it_event = True
            event = {}

        elif row == 'END:VEVENT':
            events.append(event)
            is_it_event = False

        elif is_it_event:
            event_field_parce(row, is_it_event, event)

    return events


def dict_to_df(cal):
    # if cal is empty, return empty DataFrame
    if not cal:
        return pd.DataFrame()

    # Переводим словарь в DataFrame, так будет проще работать
    df = pd.DataFrame(cal)
    # Понижаем символы в названии полей DataFrame'а
    df.columns = [str.lower(column) for column in df.columns]

    # Если строка поле 'dtstart'содержат VALUE, то это событие запланировано не на конкретное время, а на весь день
    # На данный момент они нам не нужны
    df = df[~df['dtstart'].str.contains('VALUE')]

    # Преобразования дат в читаемый формат
    for col in ['dtstart', 'dtend']:
        # Т.к. некоторые даты имеют вид: 20150610T145500Z, а некоторые: TZID=Asia/Vladivostok:20150609T180000
        # Приведём всё к одному виду, убрав лишнее (лишнее то, что до ':')
        df[f'{col}'] = df[f'{col}'].apply(lambda x: x.split(':')[1] if len(x.split(':')) > 1 else x)

        df[f'{col}'] = df[f'{col}'].str.replace('Z', '')

        # Создадим столбцы со временем начала и окончания мероприятия
        # Это пригодиться при создании событий по правилам. Оставим в текстовом формате
        df[f'{col}_time'] = df[f'{col}'].apply(lambda x: x.split('T')[1])

        df[f'{col}'] = pd.to_datetime(df[f'{col}'], format='%Y%m%dT%H%M%S')

    # Создадим поле только с датой для дальнейшего матчинга с "таймлайном"
    df['st_date'] = df['dtstart'].apply(lambda x: x.date())
    df['duration'] = (df['dtend'] - df['dtstart']).apply(lambda x: x.seconds / 3600)

    return df


def make_board(st, end):
    # Создаём пустой DataFrame c DatatimeIndex

    # создаем board с начала месяца, т.к. в правиле MONTHLY BYDAY придётся считать номера дней недели в месяце. Пример: FREQ=MONTHLY: BYDAY=4MO
    st = dt.date.fromisoformat(st).replace(day=1)

    # Т.к. нам ещё придётся проверять день на то, последний ли он в месеце, добавим на борд ещё неделю
    end = dt.date.fromisoformat(end)
    end += dt.timedelta(weeks=1)

    timeline = pd.date_range(st, end)
    bord = pd.DataFrame(timeline, columns=['timeline'])
    bord['dayofweek'] = bord['timeline'].dt.dayofweek
    bord['month'] = bord['timeline'].dt.month
    bord['day'] = bord['timeline'].dt.day

    return bord


def prepare_cal_to_events_by_rules(df, st, end):
    # select events containing rules
    df = df[~df['rrule'].isna()]
    # parse string rule to dict
    df['rrule'] = df['rrule'].apply(lambda x: dict(item.split('=') for item in x.split(':')))
    # creating a board (DataFrame) for the schedule
    board = make_board(st, end)

    return df, board


def events_to_board(e, board_of_event):
    for field in e.index:
        board_of_event[field] = e[field]

    return board_of_event


def rule_weekly(e, rule, board):
    rule['BYDAY'] = rule['BYDAY'].split(',')
    days = tuple(Week_days[day] for day in rule['BYDAY'])
    board_of_event = board.query(f" dayofweek in {days}")

    return events_to_board(e, board_of_event)


def make_day_in_month(board: pd.DataFrame):
    day_in_month = pd.Series(index=board.index)
    month_number = 0
    for index, row in board.iterrows():

        if month_number != row['month']:  # If a new month starts, update the day counter
            day_count = {0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1, 6: 1}
            month_number = row['month']

        day_of_the_week = row['dayofweek']
        day_in_month[index] = day_count[day_of_the_week]
        day_count[day_of_the_week] += 1

    board = pd.concat([board, day_in_month], axis=1).rename(columns={0: 'day_in_month'})
    board['day_in_month'] = board['day_in_month'].astype(int)

    board['last_in_month'] = False
    board['nex_days_month'] = board['month'].shift(-7)
    board.loc[board['month'] != board['nex_days_month'], 'last_in_month'] = True

    return board


def rule_monthly(e, rule, board):
    if 'BYDAY' in rule.keys():
        board = make_day_in_month(board)

        if len(rule['BYDAY']) < 4:
            day_in_month = int(rule['BYDAY'][0])
            days = (Week_days[rule['BYDAY'][1:]],)
            board_of_event = board.query(f"dayofweek in {days} and day_in_month == {day_in_month}")

        else:  # Если условие в календаре "последний день месяца" (FREQ=MONTHLY:BYDAY=-1MO)
            if int(rule['BYDAY'][:2]) == -1:
                days = (Week_days[rule['BYDAY'][2:]],)
                board_of_event = board.query(f"dayofweek in {days} and last_in_month == True")

    if 'BYMONTHDAY' in rule.keys():
        days = (int(rule['BYMONTHDAY']),)
        board_of_event = board.query(f"day in {days}")

        board_of_event = events_to_board(e, board_of_event)

    return events_to_board(e, board_of_event)


def claer_schedule(schedule):
    #  creating the date and time of the start of the event
    schedule['dtstart'] = pd.to_datetime(
        schedule['timeline'].astype('str') + ' ' + schedule['dtstart_time'],
        format='%Y-%m-%d %H%M%S'
    )

    #  creating the date and time of the end of the event
    schedule['dtend'] = pd.to_datetime(
        schedule['timeline'].astype('str') + ' ' + schedule['dtend_time'],
        format='%Y-%m-%d %H%M%S'
    )

    return schedule


def drop_extra_cols(schedule):
    return schedule.drop(
        [
            'timeline',
            'dayofweek',
            'month',
            'day',
            'dtstart_time',
            'dtend_time',
            'st_date',
            'rrule',
            'sequence'

        ],
        axis='columns'
    )


def events_by_rules(df, st, end):
    df, board = prepare_cal_to_events_by_rules(df, st, end)

    rules_schedule = pd.DataFrame()
    for index, e in df.iterrows():
        rule = e['rrule']
        # Since recurring events should not be created before the date of the main event, we will cut boar table
        e_st = dt.datetime.combine(e['st_date'],
                                   dt.time.min)  # Both datetime.time() and datetime.time.min represent midnight (00:00:00).
        board_of_event = board[board['timeline'] >= e_st]

        # Need comment
        match rule['FREQ']:
            case 'DAILY':
                board_of_event = events_to_board(e, board_of_event)

            case 'WEEKLY':
                board_of_event = rule_weekly(e, rule, board)

            case 'MONTHLY':
                board_of_event = rule_monthly(e, rule, board)

            # case 'YEARLY':

        # Checking whether the event has a rule "UNTIL"
        if 'UNTIL' in rule:
            until = rule['UNTIL']
            until = until.replace('Z', '')
            until = dt.datetime.strptime(until, '%Y%m%dT%H%M%S')

            board_of_event = board_of_event[board_of_event['timeline'] <= until]

        rules_schedule = pd.concat([rules_schedule, board_of_event], axis=0, ignore_index=True)
        # rules_schedule = rules_schedule.sort_values('timeline').reset_index(drop=True)

    # Replace values in 'dtstart' and 'dtend' columns

    # Delete extr columns

    # !!! Перед выводом результата обреж DataFrame по st дату. Т.к. в функции make_board мы приводили ей к началу месяца
    rules_schedule = rules_schedule.query(f"'{st}' <= timeline <= '{end}' ")

    # Preparing the schedule for output
    return claer_schedule(rules_schedule)


def create_schedule(cal, st, end):
    # if cal is empty, return empty DataFrame
    if not cal:
        return pd.DataFrame()

    df = dict_to_df(cal)

    if 'rrule' in df.columns:
        schedule_without_rules = df[df['rrule'].isna()]
        schedule_by_rules = events_by_rules(df, st, end)

        schedule = pd.concat([schedule_without_rules, schedule_by_rules], axis=0, ignore_index=True)

        # Preparing the schedule for output
        return drop_extra_cols( schedule )

    df = df[
        df['dtstart'] >= st
        &
        df['end'] <= end
    ]
    return drop_extra_cols(df)
