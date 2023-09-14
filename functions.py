import requests

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