"""
python rawdata_to_rule_table.py --rawdata acl1_2000_2 --ruletable acl1_rule_table_2
"""



import pandas as pd
import numpy as np
import functools
import sys
from rawdata_to_df import raw_to_df
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
  '--rawdata',
  type=str,
 )
parser.add_argument(
   '--ruletable',
  type=str
)
args = parser.parse_args()





# ACL_FIELD = {
#     'ETH_TYPE': [],
#     'IP_PROTO': [],
#     'IPV4_SRC': [],
#     'IPV4_DST': [],
#     'IPV6_SRC': [],
#     'IPV6_DST': [],
#     'PORT_SRC': [],
#     'PORT_DST': [],
#     }

ACL_FIELD = {
    'IP_PROTO': [],
    'IPV4_SRC': [],
    'IPV4_DST': [],
    'PORT_SRC': [],
    'PORT_DST': [],
    'FLAG' : []
    }

df = raw_to_df(args.rawdata)



# print(df)

df.to_csv(args.ruletable,index = False)
