import argparse
from datetime import datetime, timedelta, timezone
import requests
from functools import wraps
import json
import asyncio
from aioch import Client


parser = argparse.ArgumentParser(description="Arguments")
parser.add_argument('--init', action='store_true', help="Create/Recreate DB table")
parser.add_argument('-c', type=int, default=3600*24*3, help="Export with txn_created_at last N seconds (default: 3 days)")
parser.add_argument('-u', type=int, default=90, help="Export with txn_updated_at last N seconds (default: 90 sec)")
arg = parser.parse_args()

host_addr = '192.168.xxx.xxx'
port_num = 9000
user_name = 'xxx'
user_passwd = 'xxxxx'
name_base = 'xxx'
House_Cluster_name = 'HouseCluster'
TABLE_NAME_REPL = "transaction_repl1"
TABLE_NAME_DIST = "transaction_dist1"
TABLE_NAME_SVC = "service_table1"
PSP_EXPORT_1C_BASE_URL = "https://xxxxxxx/api"


def log_to_json(log_entry):
        try:
            with open('log.json', 'r', encoding='utf-8') as file:
                data = json.load(file)

        except (json.JSONDecodeError, FileNotFoundError):
            data = []
        new_entry = {str(key).replace("'", '"'): str(log_entry[key]).replace("'", '"') for key in log_entry}
        data.append(new_entry)
            # file.seek(0)
        with open('log.json', 'w', encoding='utf-8') as file:
            file.write(json.dumps(data, ensure_ascii=False, indent=4))


def logs_decorator(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            result = func(*args, **kwargs)
            # log_entry = {
            #     "Date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            #     "level": "INFO",
            #     "message": f"Function {func.__name__} executed successfully",
            #     "result": result,
            #     'args': args,
            #     'kwargs': kwargs
            # }
            # log_to_json(log_entry)
            return result
        except Exception as e:
            log_entry = {
                "Date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "message": f"Function {func.__name__} executed ERROR",
                "Type error": e
            }
            log_to_json(log_entry)
            raise
    return wrapper


INIT_QUERY_Replicate = f"""
CREATE TABLE {TABLE_NAME_REPL} ON CLUSTER {House_Cluster_name} (
    txn_id UInt64,
    project_id Nullable(UInt32),
    mst_id Nullable(String),
    merch_id Nullable(String),
    mst_name Nullable(String),
    merch_name Nullable(String),
    txn_amount_src Nullable(Decimal(10, 5)),
    txn_currency_src Nullable(String),
    txn_amount Nullable(Decimal(10, 5)),
    txn_currency Nullable(String),
    txn_type_id Nullable(String),
    pay_method_id Nullable(String),
    txn_status_id Nullable(String),
    txn_error_code Nullable(String),
    chn_id Nullable(UInt32),
    gtw_id Nullable(UInt32),
    gtw_desc Nullable(String),
    mst_order_id Nullable(String),
    order_id Nullable(String),
    order_desc Nullable(String),
    mst_txn_id Nullable(String),
    gtw_txn_id Nullable(String),
    pan Nullable(String),
    cardinfo_pay_system Nullable(String),
    ip Nullable(String),
    rrn Nullable(String),
    email Nullable(String),
    full_name Nullable(String),
    auth_code Nullable(String),
    ps_error_code Nullable(String),
    ps_error_message Nullable(String),
    cardinfo_issuer_name Nullable(String),
    cardinfo_country Nullable(String),
    card_issuer_country Nullable(String),
    eu_country_card Nullable(Boolean),
    decline_commission Nullable(Boolean),
    txn_authorized_at Nullable(DateTime),
    txn_confirmed_at  Nullable(DateTime),
    txn_reconciled_at Nullable(DateTime),
    txn_settled_at    Nullable(DateTime),
    txn_updated_at    DateTime,
    txn_created_at    DateTime,
    txn_cms_updated_at Nullable(DateTime),
    txn_cms_created_at Nullable(DateTime),
)ENGINE = ReplicatedReplacingMergeTree(\'/clickhouse/tables/{{shard}}/{TABLE_NAME_REPL}\', \'{{replica}}\', txn_id)
PARTITION BY toYYYYMM(txn_created_at)
ORDER BY cityHash64(concat(toString(txn_id), '|', toString(txn_updated_at)));
"""

INIT_QUERY_Distributed = f"""
CREATE TABLE {TABLE_NAME_DIST} ON CLUSTER {House_Cluster_name} (
    txn_id UInt64,
    project_id Nullable(UInt32),
    mst_id Nullable(String),
    merch_id Nullable(String),
    mst_name Nullable(String),
    merch_name Nullable(String),
    txn_amount_src Nullable(Decimal(10, 5)),
    txn_currency_src Nullable(String),
    txn_amount Nullable(Decimal(10, 5)),
    txn_currency Nullable(String),
    txn_type_id Nullable(String),
    pay_method_id Nullable(String),
    txn_status_id Nullable(String),
    txn_error_code Nullable(String),
    chn_id Nullable(UInt32),
    gtw_id Nullable(UInt32),
    gtw_desc Nullable(String),
    mst_order_id Nullable(String),
    order_id Nullable(String),
    order_desc Nullable(String),
    mst_txn_id Nullable(String),
    gtw_txn_id Nullable(String),
    pan Nullable(String),
    cardinfo_pay_system Nullable(String),
    ip Nullable(String),
    rrn Nullable(String),
    email Nullable(String),
    full_name Nullable(String),
    auth_code Nullable(String),
    ps_error_code Nullable(String),
    ps_error_message Nullable(String),
    cardinfo_issuer_name Nullable(String),
    cardinfo_country Nullable(String),
    card_issuer_country Nullable(String),
    eu_country_card Nullable(Boolean),
    decline_commission Nullable(Boolean),
    txn_authorized_at Nullable(DateTime),
    txn_confirmed_at  Nullable(DateTime),
    txn_reconciled_at Nullable(DateTime),
    txn_settled_at    Nullable(DateTime),
    txn_updated_at    DateTime,
    txn_created_at    DateTime,
    txn_cms_updated_at Nullable(DateTime),
    txn_cms_created_at Nullable(DateTime),
)ENGINE = Distributed({House_Cluster_name}, {name_base}, {TABLE_NAME_REPL}, (cityHash64(concat(toString(txn_id), '|', toString(txn_updated_at)))));
"""

INIT_QUERY_SVC = f"""
CREATE TABLE {TABLE_NAME_SVC} ON CLUSTER {House_Cluster_name} (
    row_count Nullable(UInt32),
    status Boolean,
    date DateTime,
)ENGINE = ReplicatedMergeTree(\'/clickhouse/tables/{{shard}}/{TABLE_NAME_SVC}\', \'{{replica}}\')
PARTITION BY date
ORDER BY date;
"""

columns = [
    'txn_id',
    'project_id',
    'mst_id',
    'merch_id',
    'mst_name',
    'merch_name',
    'txn_amount_src',
    'txn_currency_src',
    'txn_amount',
    'txn_currency',
    'txn_type_id',
    'pay_method_id',
    'txn_status_id',
    'txn_error_code',
    'chn_id',
    'gtw_id',
    'gtw_desc',
    'mst_order_id',
    'order_id',
    'order_desc',
    'mst_txn_id',
    'gtw_txn_id',
    'pan',
    'cardinfo_pay_system',
    'ip',
    'rrn',
    'email',
    'full_name',
    'auth_code',
    'ps_error_code',
    'ps_error_message',
    'cardinfo_issuer_name',
    'cardinfo_country',
    'card_issuer_country',
    'eu_country_card',
    'decline_commission',
    'txn_authorized_at',
    'txn_confirmed_at',
    'txn_reconciled_at',
    'txn_settled_at',
    'txn_updated_at',
    'txn_created_at',
    'txn_cms_updated_at',
    'txn_cms_created_at'
]
columns_str = ', '.join(columns)

columns_svc = [
    'row_count',
    'status',
    'date'
]
columns_svc_str = ', '.join(columns_svc)


def check_str(value):
    if value is None:
        return None
    return str(value)


def check_int(value):
    if value is None:
        return None
    elif isinstance(value, int) or str(value).isdigit():
        return int(value)



def check_float(value):
    if value is None:
        return None
    elif isinstance(value, float) or isinstance(value, str):
        return float(value)
    else:
        return None


def check_bool(value):
    if isinstance(value, bool):
        return value
    else:
        return None

@logs_decorator
def required_timestamp(timestamp):
    if timestamp is None:
        return datetime.strptime('2100-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    return dt


@logs_decorator
def format_timestamp(timestamp):
    if timestamp is None:
        return None
    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
    return dt


# Подготовка данных к записи в кликхаус
@logs_decorator
def preparing_data(api_answer):
    dict_list = []
    for record in api_answer:
        dict_tmp = {}
        dict_tmp.update({'txn_id': check_int(record.get('txn_id')),
                         'project_id': check_int(record.get('project_id')),
                         'mst_id': check_str(record.get('mst_id')),
                         'merch_id': check_str(record.get('merch_id')),
                         'mst_name': check_str(record.get('mst_name')),
                         'merch_name': check_str(record.get('merch_name')),
                         'txn_amount_src': check_float(record.get('txn_amount_src')),
                         'txn_currency_src': check_str(record.get('txn_currency_src')),
                         'txn_amount': check_float(record.get('txn_amount')),
                         'txn_currency': check_str(record.get('txn_currency')),
                         'txn_type_id': check_str(record.get('txn_type_id')),
                         'pay_method_id': check_str(record.get('pay_method_id')),
                         'txn_status_id': check_str(record.get('txn_status_id')),
                         'txn_error_code': check_str(record.get('txn_error_code')),
                         'chn_id': check_int(record.get('chn_id')),
                         'gtw_id': check_int(record.get('gtw_id')),
                         'gtw_desc': check_str(record.get('gtw_desc')),
                         'mst_order_id': check_str(record.get('mst_order_id')),
                         'order_id': check_str(record.get('order_id')),
                         'order_desc': check_str(record.get('order_desc')),
                         'mst_txn_id': check_str(record.get('mst_txn_id')),
                         'gtw_txn_id': check_str(record.get('gtw_txn_id')),
                         'pan': str(record.get('card_first_6', '')) + '******' + str(record.get('card_last_4', '')),
                         'cardinfo_pay_system': check_str(record.get('cardinfo_pay_system')),
                         'ip': check_str(record.get('ip')),
                         'rrn': check_str(record.get('rrn')),
                         'email': check_str(record.get('email')),
                         'full_name': check_str(record.get('full_name')),
                         'auth_code': check_str(record.get('auth_code')),
                         'ps_error_code': check_str(record.get('ps_error_code')),
                         'ps_error_message': check_str(record.get('ps_error_message')),
                         'cardinfo_issuer_name': check_str(record.get('cardinfo_issuer_name')),
                         'cardinfo_country': check_str(record.get('cardinfo_country')),
                         'card_issuer_country': check_str(record.get('card_issuer_country')),
                         'eu_country_card': check_bool(record.get('eu_country_card')),
                         'decline_commission': check_bool(record.get('decline_commission')),
                         'txn_authorized_at': format_timestamp(record.get('txn_authorized_at')),
                         'txn_confirmed_at': format_timestamp(record.get('txn_confirmed_at')),
                         'txn_reconciled_at': format_timestamp(record.get('txn_reconciled_at')),
                         'txn_settled_at': format_timestamp(record.get('txn_settled_at')),
                         'txn_updated_at': format_timestamp(record.get('txn_updated_at')),
                         'txn_created_at': required_timestamp(record.get('txn_created_at')),
                         'txn_cms_updated_at': format_timestamp(record.get('txn_cms_updated_at')),
                         'txn_cms_created_at': format_timestamp(record.get('txn_cms_created_at'))
        })
        dict_list.append(dict_tmp)
    return dict_list

@logs_decorator
async def connect_to_db():
    client = Client(host=host_addr, port=port_num, user=user_name, password=user_passwd, database=name_base)
    return client


# Создание таблиц в БД
@logs_decorator
async def init_database(client):
    try:
        tables = await client.execute("SHOW TABLES")
        table_repl = TABLE_NAME_REPL in [table[0] for table in tables]
        table_dist = TABLE_NAME_DIST in [table[0] for table in tables]
        table_svc = TABLE_NAME_SVC in [table[0] for table in tables]
        if not table_repl:
            await client.execute(INIT_QUERY_Replicate)
        if not table_dist:
            await client.execute(INIT_QUERY_Distributed)
        if not table_svc:
            await client.execute(INIT_QUERY_SVC)
    finally:
        await client.execute(f'TRUNCATE TABLE IF EXISTS {TABLE_NAME_REPL} ON CLUSTER {House_Cluster_name}')
        await client.execute(f'TRUNCATE TABLE IF EXISTS {TABLE_NAME_DIST} ON CLUSTER {House_Cluster_name}')
        await client.execute(f'TRUNCATE TABLE IF EXISTS {TABLE_NAME_SVC} ON CLUSTER {House_Cluster_name}')
        await client.disconnect()

@logs_decorator
async def insert_to_db(client, data):
    result = await client.execute(f'INSERT INTO {TABLE_NAME_REPL} ({columns_str}) VALUES', data)
    return True, result


@logs_decorator
async def service_record(client, status, count_row):
    now = datetime.now(timezone.utc)
    dict_tmp = {'row_count': count_row, 'status': status, 'date': now}
    await client.execute(
        f'INSERT INTO {TABLE_NAME_SVC} ({columns_svc_str}) VALUES', [dict_tmp], types_check=True)

@logs_decorator
async def service_func(client):
    values_list = await client.execute(f"SELECT * FROM {TABLE_NAME_SVC} ORDER BY date DESC LIMIT 5000")
    index = 0
    count_false = 0
    now = datetime.now(timezone.utc)
    date_from = now - timedelta(seconds=arg.c)
    updated_from = now - timedelta(seconds=arg.u)
    if not values_list:
        return date_from, updated_from
    elif values_list[0][1] is True:
        difference = now - values_list[0][2].replace(tzinfo=timezone.utc)
        difference_in_seconds = difference.total_seconds()
        if int(difference_in_seconds) > 90:
            date_from = values_list[0][2]
            updated_from = values_list[0][2]
            return date_from, updated_from
        else:
            return date_from, updated_from
    elif values_list[0][1] is False and values_list[1][1] is True:
        date_from = values_list[1][2]
        updated_from = values_list[1][2]
        return date_from, updated_from
    elif values_list[0][1] is False and values_list[1][1] is False:
        for i in range(len(values_list)):
            if values_list[i][1] is False:
                count_false += 1
                if count_false >= 2:
                    index = i
            else:
                break
        date_from = values_list[index + 1][2]
        updated_from = values_list[index + 1][2]
        return date_from, updated_from

@logs_decorator
async def async_main():
    client = await connect_to_db()
    if arg.init:
        await init_database(client)
        return
    requests.packages.urllib3.disable_warnings()
    status, count_row = False, 0
    try:
        date_from, updated_from = await service_func(client)
        # date_from = '2024-10-18 08:00:04'
        # updated_from = '2024-10-18 08:00:04'
        # date_to = '2024-10-18 08:01:00'
        # updated_to = '2024-10-18 08:01:01'
        params = {
            "txn_cms_created_at": "any",
            "date_from": date_from.strftime('%Y-%m-%d %H:%M:%S'),
            "updated_from": updated_from.strftime('%Y-%m-%d %H:%M:%S')
            # "date_to": date_to,
            # "updated_to": updated_to
        }
        response = requests.get(PSP_EXPORT_1C_BASE_URL, params=params, verify=False)
        records = response.json()
        # with open ('./txn.json', 'w', encoding='utf-8') as f:
        #     f.write(json.dumps(records, ensure_ascii=False, indent=4))
        count = len(records)
        print('Кол-во транзакций ', count)
        print(params)
        data = preparing_data(records)
        status, count_row = await insert_to_db(client, data)
    finally:
        await service_record(client, status, count_row)
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(async_main())

