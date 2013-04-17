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
    result = []
    for i in xrange(1, retries):
        day = time.strftime("%Y%m%dT0000")
        stats = table_service.query_entities(table_name='$MetricsCapacityBlob', filter="RowKey eq 'data' and PartitionKey eq '"+day+"'", top=1)

        if stats != []:
            result.append({"item": "capacity", "value": stats[0].Capacity, "time": stats[0].PartitionKey})
            result.append({"item": "objects", "value": stats[0].ObjectCount, "time": stats[0].PartitionKey})
            result.append({"item": "containers", "value": stats[0].ContainerCount, "time": stats[0].PartitionKey})
            break
        else:
            time = time - timedelta(hours=1)
    return result

def get_metrics(table_service, time, transactions, items, service_type='blob'):
    result = []
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
			    result.append({"item": transaction+"."+item, "value":getattr(stat,item), "time":stats[0].PartitionKey})
            break
        else:
            time = time - timedelta(hours=1)
    return result

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
        usefull = True
        capacity = get_capacity(table_service, time)
        print("Capacity:")
        print "Time: %s, capacity: %sGB, objects: %s, containers: %s" % (capacity[0]["time"], capacity[0]["value"]/1024/1024/1024, capacity[1]["value"], capacity[2]["value"])

    if args.blob_transactions and args.items:
	usefull = True
        transactions = args.blob_transactions
        items = args.items
        blob_metrics = get_metrics(table_service, time, transactions, items, "blob")
        print("Blobs:")
	for metric in blob_metrics:
	    print "Time: %s, Item: %s, Value: %s" % (metric["time"], metric["item"], metric["value"])

    if args.table_transactions and args.items:
        usefull = True
        transactions = args.table_transactions
        items = args.items
        table_metrics = get_metrics(table_service, time, transactions, items, "table")
        print("Tables:")
	for metric in table_metrics:
	    print "Time: %s, Item: %s, Value: %s" % (metric["time"], metric["item"], metric["value"])

    if usefull == False:
        parser.print_help()

if __name__ == '__main__':
    run()
