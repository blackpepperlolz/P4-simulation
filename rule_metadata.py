import pandas as pd
import numpy as np
import functools
import sys
from rawdata_to_df import raw_to_tuple
from random import randint
import time
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
parser.add_argument(
   '--analysis',
  type=str
)


args = parser.parse_args()




# ETH_TYPE = "0"
# IP_PROTO = "0"
# IPV4_SRC = "0"
# IPV4_DST = "0"
# IPV6_SRC = "0"
# IPV6_DST = "0"
# PORT_SRC = "0"
# PORT_DST = "0"


IP_PROTO = "0"
IPV4_SRC = "0"
IPV4_DST = "0"
PORT_SRC = "0"
PORT_DST = "0"
FLAG = "0"

def match_time_update(line):
    # ETH_TYPE, IP_PROTO,IPV4_SRC,IPV4_DST,IPV6_SRC,IPV6_DST,PORT_SRC,PORT_DST = raw_to_tuple(line)
    IP_PROTO,IPV4_SRC,IPV4_DST,PORT_SRC,PORT_DST,FLAG = raw_to_tuple(line)

    # mask = ((rule_table_time['ETH_TYPE'] == ETH_TYPE) &
    # (rule_table_time['IPV4_SRC'] == IPV4_SRC) &
    # (rule_table_time['IPV4_DST'] == IPV4_DST) &
    # (rule_table_time['IPV6_SRC'] == IPV6_SRC) &
    # (rule_table_time['IPV6_DST'] == IPV6_DST) &
    # (rule_table_time['IP_PROTO'] == IP_PROTO) &
    # (rule_table_time['PORT_SRC'] == PORT_SRC) &
    # (rule_table_time['PORT_DST'] == PORT_DST))

    mask = ((rule_table_time['FLAG'] == FLAG) &
    (rule_table_time['IPV4_SRC'] == IPV4_SRC) &
    (rule_table_time['IPV4_DST'] == IPV4_DST) &
    (rule_table_time['IP_PROTO'] == IP_PROTO) &
    (rule_table_time['PORT_SRC'] == PORT_SRC) &
    (rule_table_time['PORT_DST'] == PORT_DST))
    filtered_df =rule_table_time[mask]
    if len(filtered_df) == 1:
        rule_table_time.ix[filtered_df.index.values[0], 'MATCH_TIMES'] = rule_table_time['MATCH_TIMES'][filtered_df.index.values[0]]+ 1
        # rule_table['MATCH_TIMES'][filtered_df.index.values[0]] = rule_table['MATCH_TIMES'][filtered_df.index.values[0]]+ 1






# rule_table_time = pd.read_csv(args.ruletable,dtype ={'IPV6_SRC':str,'IPV6_DST':str,'IP_PROTO':str,'PORT_SRC':str,'PORT_DST':str})
rule_table_time = pd.read_csv(args.ruletable,dtype ={'IPV4_SRC':str,'IPV4_DST':str,'IP_PROTO':str,'PORT_SRC':str,'PORT_DST':str,'FLAG':str})

rule_leng = len(rule_table_time)
zero_matrix = np.repeat(0,rule_leng)
rule_table_time.insert(loc = len(rule_table_time.columns), column='MATCH_TIMES', value = zero_matrix)
# print(rule_table_time)

num_lines = sum(1 for line in open(args.rawdata))
# print (num_lines)

# print (random_num,'%%%%%%%%%%%%%%%%')
# randint(0,num_lines)
start_time = time.time()

for x in range(10000):
    random_num  = randint(0,num_lines-1)
    with open(args.rawdata) as f:
        for i, line in enumerate(f):
            if i == random_num:
                # print(line,"************************")
                match_time_update(line)
                break





elapsed_time = time.time() - start_time



rule_table_time.to_csv(args.analysis,index = False)

# print(rule_table_time)
