import pandas as pd
import numpy as np
import functools
import sys



ACL_FIELD = {
    'ETH_TYPE': [],
    'IP_PROTO': [],
    'IPV4_SRC': [],
    'IPV4_DST': [],
    'IPV6_SRC': [],
    'IPV6_DST': [],
    'PORT_SRC': [],
    'PORT_DST': [],
    }


with open("test100", "rw+") as f:
    for line in f:
        temp = line.split("\t")
        IP_SRC = temp[0][1:]
        IP_DST = temp[1]
        PORT_SRC = temp[2].replace(" ","")
        PORT_DST = temp[3].replace(" ","")
        IP_PROTO = temp[4]
        if ":" not in IP_SRC :
            ACL_FIELD["ETH_TYPE"].append("0x0800")
            ACL_FIELD["IPV4_SRC"].append(IP_SRC)
        else:
            ACL_FIELD["ETH_TYPE"].append("0x86DD")
            ACL_FIELD["IPV6_SRC"].append(IP_SRC)
        if ":" not in IP_DST :
            # ACL_FIELD["ETH_TYPE"].append("0x0800")
            ACL_FIELD["IPV4_DST"].append(IP_DST)
        else:
            # ACL_FIELD["ETH_TYPE"].append("0x86DD")
            ACL_FIELD["IPV6_DST"].append(IP_DST)

        # temp
        ACL_FIELD["IPV6_SRC"].append("0")
        ACL_FIELD["IPV6_DST"].append("0")
        # temp
        ACL_FIELD["PORT_SRC"].append(PORT_SRC)
        ACL_FIELD["PORT_DST"].append(PORT_DST)
        ACL_FIELD["IP_PROTO"].append(IP_PROTO)

    # print (ACL_FIELD)


df=pd.DataFrame(ACL_FIELD)
tf_table = df.copy()
acl_length = len(tf_table)
zero_matrix = np.repeat(0,acl_length)

def eth_type_f(cell):
    # print(cell)
    # return "XXXX"
    if cell == '':
        return 0
    else:
        return 1

def ipv4_src_f(cell):
    # print(cell)
    # return "XXXX"
    if cell == '':
        return 0
    else:
        return 1

def ipv4_dst_f(cell):
    # print(cell)
    # return "XXXX"
    if cell == '':
        return 0
    else:
        return 1
def ipv6_src_f(cell):
    # print(cell)
    # return "XXXX"
    if cell == '':
        return 0
    else:
        return 1

def ipv6_dst_f(cell):
    # print(cell)
    # return "XXXX"
    if cell == '':
        return 0
    else:
        return 1

def ip_proto_f(cell):
    # print(cell)
    # return "XXXX"
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

tf_table["ETH_TYPE"] = tf_table["ETH_TYPE"].map(eth_type_f)
tf_table["IPV4_SRC"] = tf_table["IPV4_SRC"].map(ipv4_src_f)
tf_table["IPV4_DST"] = tf_table["IPV4_DST"].map(ipv4_dst_f)
tf_table["IPV6_SRC"] = tf_table["IPV6_SRC"].map(ipv6_src_f)
tf_table["IPV6_DST"] = tf_table["IPV6_DST"].map(ipv6_dst_f)
tf_table["IP_PROTO"] = tf_table["IP_PROTO"].map(ip_proto_f)
tf_table["PORT_SRC"] = tf_table["PORT_SRC"].map(port_src_f)
tf_table["PORT_DST"] = tf_table["PORT_DST"].map(port_dst_f)

tf_table.insert(loc = 0, column='EST_SRC', value=zero_matrix)
tf_table.insert(loc = 0, column='EST_DST', value=zero_matrix)
tf_table.insert(loc = 0, column='IN_PORT', value=zero_matrix)

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
    }
COLUMN_NAMES = BIT_MAP.keys()


IN_PORT = pd.DataFrame([[1,0,0,0,0,0,0,0,0,0,0]], columns=COLUMN_NAMES)
EST_DST = pd.DataFrame([[0,1,0,0,0,0,0,0,0,0,0]], columns=COLUMN_NAMES)
IPV4_DST = pd.DataFrame([[0,0,0,0,0,0,1,0,0,0,0]], columns=COLUMN_NAMES)


num_entry = 100
for i in range(num_entry):
    entry_select = np.random.randint(3)
    noise = np.random.randint(10)
    if entry_select == 0:
        tf_table = tf_table.append(IN_PORT, ignore_index=True)
    if entry_select == 1:
        tf_table = tf_table.append(EST_DST, ignore_index=True)
    if entry_select == 2:
        tf_table = tf_table.append(IPV4_DST, ignore_index=True)
    if noise == 10 :
        # noise_pd = pd.DataFrame(np.random.randint(low=0, high=2, size=(1, 13)),columns = COLUMN_NAMES)
        tf_table = tf_table.append(IP_PROTO, ignore_index=True)




tf_table.to_csv("tf_table")

print (tf_table)





# print(df)
