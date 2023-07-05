from datetime import datetime


def convert_float(value):
    if value is not None and value != '':
        return float(value)
    else:
        return 0.00


def convert_bolean(value):
    #0 if item['cancelado'] is not True or item['cancelado'] == 0 else 1
    if value is not True or value == 0:
        return 0
    else:
        return 1


def convert_str(value):
    #str(item['comboId']) if item['comboId'] is not None else ""
    if value is not None:
        return str(value)
    else:
        return ""


def convert_datetime_with_seconds(timestamp):
    if timestamp is not None and timestamp != '':
        dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    else:
        return ''


def convert_datetime_with_timezone(datetime_str):
    try:
        datetime_obj = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S%z")
        return datetime_obj.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


'''def convert_datetime(data):
    if data is not None:
        return datetime.strptime(data, "%Y%m%d").strftime("%Y-%m-%d")'''

def convert_datetime(data):
    if data is not None:
        try:
            date_obj = datetime.strptime(data, "%Y-%m-%d")
            return date_obj.strftime("%Y-%m-%d")
        except ValueError:
            try:
                date_obj = datetime.strptime(data, "%Y%m%d")
                return date_obj.strftime("%Y-%m-%d")
            except ValueError:
                return None
    return None


def convert_seconds(value):
    if value is not None:
        return datetime.strptime(value, "%H:%M").strftime("%H:%M:%S")





