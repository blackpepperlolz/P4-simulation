import pandas as pd
import numpy as np
import functools
import sys
from rawdata_to_df import raw_to_df
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
  '--analysis',
  type=str,
 )
parser.add_argument(
   '--trans',
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
#     'MATCH_TIMES':[]
#     }


ACL_FIELD = {
    'IP_PROTO': [],
    'IPV4_SRC': [],
    'IPV4_DST': [],
    'PORT_SRC': [],
    'PORT_DST': [],
    'FLAG':[],
    'MATCH_TIMES':[]
    }

# df = raw_to_df(args.i)



# print(df)
# rule_table_analyed = pd.read_csv(args.analysis,dtype ={'IPV6_SRC':str,'IPV6_DST':str,'IP_PROTO':str,'PORT_SRC':str,'PORT_DST':str})
rule_table_analyed = pd.read_csv(args.analysis,dtype ={'IPV4_SRC':str,'IPV4_DST':str,'IP_PROTO':str,'PORT_SRC':str,'PORT_DST':str,'FLAG':str})
# df.to_csv("rule_table",index = False)
# rule_table_analyed = df.copy()
acl_length = len(rule_table_analyed)
zero_matrix = np.repeat(0,acl_length)

def eth_type_f(cell):
    if cell == '':
        return 0
    else:
        return 1

def ipv4_src_f(cell):
    if cell == '0.0.0.0/0':
        return 0
    else:
        return 1

def ipv4_dst_f(cell):
    if cell == '0.0.0.0/0':
        return 0
    else:
        return 1
def ipv6_src_f(cell):
    if cell == "0":
        return 0
    else:
        return 1

def ipv6_dst_f(cell):
    if cell == "0":
        return 0
    else:
        return 1

def ip_proto_f(cell):
    if cell == '0x00/0x00':
        return 0
    else:
        return 1

def port_src_f(cell):
    if cell == '0:65535':
        return 0
    else:
        return 1

def port_dst_f(cell):
    if cell == '0:65535':
        return 0
    else:
        return 1

def flag_dst_f(cell):
    if cell == '0x0000/0x0000':
        return 0
    else:
        return 1


# rule_table_analyed["ETH_TYPE"] = rule_table_analyed["ETH_TYPE"].map(eth_type_f)
rule_table_analyed["IP_PROTO"] = rule_table_analyed["IP_PROTO"].map(ip_proto_f)
rule_table_analyed["IPV4_SRC"] = rule_table_analyed["IPV4_SRC"].map(ipv4_src_f)
rule_table_analyed["IPV4_DST"] = rule_table_analyed["IPV4_DST"].map(ipv4_dst_f)
# rule_table_analyed["IPV6_SRC"] = rule_table_analyed["IPV6_SRC"].map(ipv6_src_f)
# rule_table_analyed["IPV6_DST"] = rule_table_analyed["IPV6_DST"].map(ipv6_dst_f)
rule_table_analyed["PORT_SRC"] = rule_table_analyed["PORT_SRC"].map(port_src_f)
rule_table_analyed["PORT_DST"] = rule_table_analyed["PORT_DST"].map(port_dst_f)
rule_table_analyed["PORT_DST"] = rule_table_analyed["PORT_DST"].map(port_dst_f)
rule_table_analyed["FLAG"] = rule_table_analyed["FLAG"].map(flag_dst_f)

rule_table_analyed.insert(loc = 0, column='EST_SRC', value=zero_matrix)
rule_table_analyed.insert(loc = 0, column='EST_DST', value=zero_matrix)
rule_table_analyed.insert(loc = 0, column='IN_PORT', value=zero_matrix)
# print(tf_table)
"""
"""

BIT_MAP = {
    'IN_PORT':32,
    'EST_DST': 48,
    'EST_SRC': 48,
    'ETH_TYPE': 16,
    'IP_PROTO': 8,
    'IPV4_SRC': 32,
    'IPV4_DST': 32,
    'IPV6_SRC': 128,
    'IPV6_DST': 128,
    'PORT_SRC': 16,
    'PORT_DST': 16,
    'FLAG': 16,
    }
COLUMN_NAMES = BIT_MAP.keys()
# print(COLUMN_NAMES)


# IN_PORT = pd.DataFrame([[1,0,0,0,0,0,0,0,0,0,0]], columns=COLUMN_NAMES)
IN_PORT = pd.DataFrame({
    'IN_PORT': [1],
    'EST_DST': [0],
    'EST_SRC': [0],
    'ETH_TYPE': [0],
    'IP_PROTO': [0],
    'IPV4_SRC': [0],
    'IPV4_DST': [0],
    'IPV6_SRC': [0],
    'IPV6_DST': [0],
    'PORT_SRC': [0],
    'PORT_DST': [0],
})
EST_DST = pd.DataFrame({
    'IN_PORT': [0],
    'EST_DST': [1],
    'EST_SRC': [0],
    'ETH_TYPE': [0],
    'IP_PROTO': [0],
    'IPV4_SRC': [0],
    'IPV4_DST': [0],
    'IPV6_SRC': [0],
    'IPV6_DST': [0],
    'PORT_SRC': [0],
    'PORT_DST': [0],
})
IPV4_DST = pd.DataFrame({
    'IN_PORT': [0],
    'EST_DST': [0],
    'EST_SRC': [0],
    'ETH_TYPE': [0],
    'IP_PROTO': [0],
    'IPV4_SRC': [0],
    'IPV4_DST': [1],
    'IPV6_SRC': [0],
    'IPV6_DST': [0],
    'PORT_SRC': [0],
    'PORT_DST': [0],
})
IP_PROTO = pd.DataFrame({
    'IN_PORT': [0],
    'EST_DST': [0],
    'EST_SRC': [0],
    'ETH_TYPE': [0],
    'IP_PROTO': [1],
    'IPV4_SRC': [0],
    'IPV4_DST': [0],
    'IPV6_SRC': [0],
    'IPV6_DST': [0],
    'PORT_SRC': [0],
    'PORT_DST': [0],
})
# print(IN_PORT)
# EST_DST = pd.DataFrame([[0,1,0,0,0,0,0,0,0,0,0]], columns=COLUMN_NAMES)
# IPV4_DST = pd.DataFrame([[0,0,0,0,0,0,1,0,0,0,0]], columns=COLUMN_NAMES)


# num_entry = 100
# for i in range(num_entry):
#     entry_select = np.random.randint(3)
#     noise = np.random.randint(100)
#     if entry_select == 0:
#         tf_table = tf_table.append(IN_PORT, ignore_index=True)
#     if entry_select == 1:
#         tf_table = tf_table.append(EST_DST, ignore_index=True)
#     if entry_select == 2:
#         tf_table = tf_table.append(IPV4_DST, ignore_index=True)
#     if noise == 0 :
#         # noise_pd = pd.DataFrame(np.random.randint(low=0, high=2, size=(1, 13)),columns = COLUMN_NAMES)
#         tf_table = tf_table.append(IP_PROTO, ignore_index=True)




rule_table_analyed.to_csv(args.trans,index = False)

# print (tf_table)





# print(df)
