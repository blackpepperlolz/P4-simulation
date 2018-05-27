import pandas as pd
import numpy as np
import functools
import sys



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
    'FLAG':[]
    }

def raw_to_tuple(line):
    temp = line.split("\t")
    IP_SRC = temp[0][1:]
    IP_DST = temp[1]
    PORT_SRC = temp[2].replace(" ","")
    PORT_DST = temp[3].replace(" ","")
    IP_PROTO = temp[4]
    FLAG = temp[5]

    if ":" not in IP_SRC :
        _ETH_TYPE = "0x0800"
        _IPV4_SRC = IP_SRC
    else:
        _ETH_TYPE = "0x86DD"
        _IPV6_SRC = IP_SRC
    if ":" not in IP_DST :
        # ETH_TYPE ="0x0800"
        _IPV4_DST = IP_DST
    else:
        # ETH_TYPE ="0x86DD"
        _IPV6_DST = IP_DST

    # temp
    # _IPV6_SRC = "0"
    # _IPV6_DST = "0"
    # temp
    _PORT_SRC = PORT_SRC
    _PORT_DST = PORT_DST
    _IP_PROTO = IP_PROTO
    _FLAG = FLAG

    # print ( _ETH_TYPE, _IP_PROTO,_IPV4_SRC,_IPV4_DST,_IPV6_SRC ,_IPV6_DST,_PORT_SRC,_PORT_DST,"=======********============")
    return _IP_PROTO,_IPV4_SRC,_IPV4_DST,_PORT_SRC,_PORT_DST,_FLAG





def raw_to_df(file_path,field_map = ACL_FIELD):
    with open(file_path, "r") as f:
        for line in f:
            IP_PROTO,IPV4_SRC,IPV4_DST,PORT_SRC,PORT_DST,FLAG = raw_to_tuple(line)
            # field_map["ETH_TYPE"].append(ETH_TYPE)
            field_map["IPV4_SRC"].append(IPV4_SRC)
            field_map["IPV4_DST"].append(IPV4_DST)
            # field_map["IPV6_SRC"].append(IPV6_SRC)
            # field_map["IPV6_DST"].append(IPV6_DST)
            field_map["PORT_SRC"].append(PORT_SRC)
            field_map["PORT_DST"].append(PORT_DST)
            field_map["IP_PROTO"].append(IP_PROTO)
            field_map["FLAG"].append(FLAG)

            # temp = line.split("\t")
            # IP_SRC = temp[0][1:]
            # IP_DST = temp[1]
            # PORT_SRC = temp[2].replace(" ","")
            # PORT_DST = temp[3].replace(" ","")
            # IP_PROTO = temp[4]
            # if ":" not in IP_SRC :
            #     field_map["ETH_TYPE"].append("0x0800")
            #     field_map["IPV4_SRC"].append(IP_SRC)
            # else:
            #     field_map["ETH_TYPE"].append("0x86DD")
            #     field_map["IPV6_SRC"].append(IP_SRC)
            # if ":" not in IP_DST :
            #     # field_map["ETH_TYPE"].append("0x0800")
            #     field_map["IPV4_DST"].append(IP_DST)
            # else:
            #     # field_map["ETH_TYPE"].append("0x86DD")
            #     field_map["IPV6_DST"].append(IP_DST)
            #
            # # temp
            # field_map["IPV6_SRC"].append("0")
            # field_map["IPV6_DST"].append("0")
            # # temp
            # field_map["PORT_SRC"].append(PORT_SRC)
            # field_map["PORT_DST"].append(PORT_DST)
            # field_map["IP_PROTO"].append(IP_PROTO)

        # print (field)


    df=pd.DataFrame(ACL_FIELD)

    return df
