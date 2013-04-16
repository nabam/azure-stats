#!/usr/bin/env python

import argparse
import json

from datetime import datetime, timedelta
from azure.storage import *

default_config_path = "azure.json"
retries = 4

def parse_config(config_path):
    config_file = open(config_path)
    config = json.load(config_file)
    config_file.close

    return config

def get_capacity(table_service, time):
    for i in xrange(1, retries):
        day = time.strftime("%Y%m%dT0000")
        stats = table_service.query_entities(table_name='$MetricsCapacityBlob', filter="RowKey eq 'data' and PartitionKey eq '"+day+"'", top=1)

        if stats != []:
            print "Time: %s, capacity: %sGB, objects: %s, containers: %s" % (stats[0].PartitionKey, stats[0].Capacity/1024/1024/1024, stats[0].ObjectCount, stats[0].ContainerCount)
            break
        else:
            time = time - timedelta(hours=1)

def get_metrics(table_service, time, transactions, items, service_type='blob'):
    if service_type == 'blob':
        table = '$MetricsTransactionsBlob'
    elif service_type == 'table':
        table = '$MetricsTransactionsTable'

    for i in xrange(1, retries):
        hour = time.strftime("%Y%m%dT%H00")
        stats = table_service.query_entities(table_name=table, filter="PartitionKey eq '"+hour+"'")

        if stats != []:
            for stat in stats:
                for transaction in transactions:
                    for item in items:
                        if "user;"+transaction == stat.RowKey:
                            print "Time: %s, TransactionType: %s, %s: %s" % (stat.PartitionKey, stat.RowKey, item, getattr(stat,item))
            break
        else:
            time = time - timedelta(hours=1)

def run():
    usefull = False

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="config path")
    parser.add_argument("-C", "--capacity", help="show capacity", action='store_true')
    parser.add_argument("-T", "--blob_transactions", nargs="+", help="blob transactions")
    parser.add_argument("-t", "--table_transactions", nargs="+", help="table transactions")
    parser.add_argument("-i", "--items", nargs="+", help="items")

    parser.set_defaults(add_help=True)

    args = parser.parse_args()

    if args.config:
        config_path = args.config
    else:
        config_path = default_config_path

    config = parse_config(config_path)

    table_service = TableService(account_name=config['account'], account_key=config['accessKey'])

    time = datetime.utcnow()

    if args.capacity:
        print("Capacity:")
        get_capacity(table_service, time)
        usefull = True

    if args.blob_transactions and args.items:
        print("Blobs:")
        transactions = args.blob_transactions
        items = args.items
        get_metrics(table_service, time, transactions, items, "blob")
        usefull = True

    if args.table_transactions and args.items:
        print("Tables:")
        transactions = args.table_transactions
        items = args.items
        get_metrics(table_service, time, transactions, items, "table")
        usefull = True

    if usefull == False:
        parser.print_help()

if __name__ == '__main__':
    run()
