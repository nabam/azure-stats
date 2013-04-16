azure-stats
===========

Usage
-----

<pre>./get_stats.py -h
usage: get_stats.py [-h] [-c CONFIG] [-C]
                    [-T BLOB_TRANSACTIONS [BLOB_TRANSACTIONS ...]]
                    [-t TABLE_TRANSACTIONS [TABLE_TRANSACTIONS ...]]
                    [-i ITEMS [ITEMS ...]]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        config path
  -C, --capacity        show capacity
  -T BLOB_TRANSACTIONS [BLOB_TRANSACTIONS ...], --blob_transactions BLOB_TRANSACTIONS [BLOB_TRANSACTIONS ...]
                        blob transactions
  -t TABLE_TRANSACTIONS [TABLE_TRANSACTIONS ...], --table_transactions TABLE_TRANSACTIONS [TABLE_TRANSACTIONS ...]
                        table transactions
  -i ITEMS [ITEMS ...], --items ITEMS [ITEMS ...]
                        items</pre>

Usage example
-------------

<pre>./get_stats.py -T PutBlockList PutBlock GetBlob All -i AverageServerLatency AverageE2ELatency Availability -t All MergeEntity DeleteEntity QueryEntity -C</pre>
