# coding: utf-8

# In[23]:


import pandas as pd
import numpy as np
import functools
import sys
import argparse
import math

parser = argparse.ArgumentParser()

parser.add_argument(
   '--trans',
  type=str
)

parser.add_argument(
   '--sizecase',
   dest = 'case',
  type=str
)

args = parser.parse_args()
# In[24]:

"""
Fields bit
"""
# BIT_MAP = {
#     'IN_PORT':32,
#     'EST_DST': 48,
#     'EST_SRC': 48,
#     'ETH_TYPE': 16,
#     'IP_PROTO': 8,
#     'IPV4_SRC': 32,
#     'IPV4_DST': 32,
#     'IPV6_SRC': 128,
#     'IPV6_DST': 128,
#     'PORT_SRC': 16,
#     'PORT_DST': 16,
#     'FLAG' : 8,
#     'MATCH_TIMES':0
# }

BIT_MAP = {
    'IN_PORT':0,
    'EST_DST': 0,
    'EST_SRC': 0,
    'IP_PROTO': 8,
    'IPV4_SRC': 32,
    'IPV4_DST': 32,
    'PORT_SRC': 16,
    'PORT_DST': 16,
    'FLAG' : 8,
    'MATCH_TIMES':0
    }


METATADA = ['MATCH_TIMES']


COLUMN_NAMES = BIT_MAP.keys()
HIGH_DIM = 1
MIDIUM_DIM = 2
LOW_DIM = 3
RANDOM = 4
TEST_CASE = 5
TIME = 1000 # miniseconds
THRESHOLD = 26 # weighting threshold
ENTRY_BITS = 112
CASE = 1
MEMORY_SIZE = 336000

"""
Genterating the original table
"""




def find_used(entry_row):
    used_set = []
    for column in COLUMN_NAMES:
        if column not in METATADA:
            value = entry_row[column]
            if (value == 1):
                used_set.append(column)
    sorted(used_set)
    return used_set


# In[26]:
def reduce_df_column(raw_series):
    mask = (raw_series == 1)
    mask[-1] = True
    used_fields = raw_series[mask]
    # used_fields = raw_series[
    #     raw_series['IN_PORT'] == 1 |
    #     raw_series['EST_DST'] == 1 |
    #     raw_series['EST_SRC'] == 1 |
    #     raw_series['ETH_TYPE'] == 1 |
    #     raw_series['IP_PROTO'] == 1 |
    #     raw_series['IPV4_SRC'] == 1 |
    #     raw_series['IPV4_DST'] == 1 |
    #     raw_series['IPV6_SRC'] == 1 |
    #     raw_series['IPV6_DST'] == 1 |
    #     raw_series['PORT_SRC'] == 1 |
    #     raw_series['PORT_DST'] == 1
    # ]
    # print (used_fields)
    return used_fields.to_frame().T

def sort_info(raw_info, by_key):

    sorted_info = {}
    sorted_list = sorted(
        raw_info.items(),
        key=(lambda x: x[1][by_key]),
        reverse=True
    )
    # print (sorted_list,'\n')
    weight = len(entryTableMap)
    for item_value in sorted_list:
        item_value[1].weight_cal(weight)
        sorted_info[item_value[0]] = item_value[1]
        weight = weight - 1

        # print(item_value[0], item_value[1]['total_unused_bit_count'])


    # print (sorted_info,'\n')
    return sorted_info

"""
Initial
"""
class EntryTable:
    def __init__(self,used_fields,is_fixed = False,unused_bit_count = 0):
        self.df = pd.DataFrame(columns = used_fields)
        self.dict= {'total_unused_bit_count': 0,'reduced_df_length': 0,'matchtimes':0,'weight':0,'used_bit_count':0}
        self.key = ','.join(used_fields)
        # self.used_bit_count = 0
        self.unused_bit_count = unused_bit_count
        self.total_used_bit_count = 0
        # self.total_unused_bit_count = 0
        # self.reduced_df_length = 0
        # self.weight = 0
        self.frequency = 0
        self.size = 0
        self.matchtimes = 0

    def append(self, add_entry):
        # print (add_entry['MATCH_TIMES'])
        self.dict['matchtimes'] = self.dict['matchtimes']+ int(add_entry['MATCH_TIMES'])
        self.df = self.df.append(add_entry, ignore_index=True)

    def update(self,time,entry_bits):
        length = len(self.df)
        self.dict['reduced_df_length'] = length
        self.dict['used_bit_count'] = entry_bits - self.unused_bit_count
        self.total_used_bit_count = length * self.dict['used_bit_count']
        self.dict['total_unused_bit_count'] = length * self.unused_bit_count
        # self.frequency = self.matchtimes/time
        # self.matchtimes =

    def unused_count(self,bit_map,unused_fields):
        self.unused_bit_count = functools.reduce((lambda acc, field: acc + bit_map[field]), unused_fields, 0)


    def weight_cal(self,weight_val):
        self.dict['weight'] = self.dict['weight']  + weight_val

    def __getitem__(self,key):
        return self.dict[key]
    def __setitem__(self,key,value):
        self.dict[key] = value

    def __repr__(self):
        # return self.df.to_string()
        return ("%s\n"  %(self.key)
                +" weight : %s\n" %(self.dict['weight'] )
                +" numbers of entry :　%s\n" % (self.dict['reduced_df_length'])
                +" used bits in an entry :　%s\n"%(self.dict['used_bit_count'])
                +" total used bits :　%s\n" % (self.total_used_bit_count)
                +" unused bits in an entry :　%s\n"%(self.unused_bit_count)
                +" total unused bits :　%s\n" % (self.dict['total_unused_bit_count'])
                +" frequency : %s\n" %(self.frequency)
                +" matchtimes : %s\n" %(self.dict['matchtimes'])
                + "===============\n")

        # return self.key + '\n'

    def __str__(self):
        return self.key + '\n'




# open_table_test = entry_generator(TEST_CASE,1000)
open_table = pd.read_csv(args.trans)

total_rules = len(open_table)

# print(open_table)
entryTableMap = {
'IN_PORT':EntryTable(['IN_PORT'],unused_bit_count = 0),
'EST_DST':EntryTable(['EST_DST'],unused_bit_count = 0),
'IPV4_DST':EntryTable(['IPV4_DST'],unused_bit_count = 0),
}

for idx, row in open_table.iterrows():
    used_fields = find_used(row)
    used_fields_key = ','.join(used_fields)



    add_entry = reduce_df_column(row)


    if used_fields_key in entryTableMap:
        entryTableMap[used_fields_key].append(add_entry)

    else:
        unused_fields = list(row[row == 0].axes[0])
        # print(unused_fields)
        # unsed_bit_count = functools.reduce((lambda acc, field: acc + BIT_MAP[field]), unused_fields, 0)
        entryTable = EntryTable(used_fields)
        entryTable.unused_count(BIT_MAP,unused_fields)
        entryTable.append(add_entry)
        entryTableMap[used_fields_key] = entryTable


for key in entryTableMap:
    entryTableMap[key].update(TIME,ENTRY_BITS)





sorted_info_by_total = sort_info(entryTableMap, 'total_unused_bit_count')
sorted_info_by_length = sort_info(entryTableMap, 'reduced_df_length')
sorted_info_by_matchtimes = sort_info(entryTableMap, 'matchtimes')

# print(sorted_info_by_total, '\n\n============\n\n')
# print(sorted_info_by_length, '\n\n============\n\n')

# for key in info:
#     info[key]['frequency'] = info[key]['reduced_TF_df_length']/TIME

# weight = len(entryTableMap)
# for key in sorted_info_by_total:
#     entryTableMap[key].weight_cal(weight)
#     weight = weight - 1
#     print(key,'=======',entryTableMap[key]['total_unused_bit_count'],'========',weight)
#
#
# weight = len(entryTableMap)
# for key in sorted_info_by_length:
#     entryTableMap[key].weight_cal(weight)
#     weight = weight - 1
#     print(key,'**********',entryTableMap[key]['reduced_df_length'],'**********',weight)
#
# weight = len(entryTableMap)
# for key in sorted_info_by_matchtimes:
#     entryTableMap[key].weight_cal(weight)
#     weight = weight - 1
#     print(key,'@@@@@@@@@@@@@@@@@@',entryTableMap[key]['matchtimes'],'@@@@@@@@@@@@@@@',weight)
"""
Print result
"""
print(entryTableMap)

# print(info,'\n\n============\n\n')

"""
Decide Size and table
"""
# origanized_table = {}
# original_table = {}
# origanied_size = 0
# for key in info:
#     if info[key]['weight']>= THRESHOLD:
#         origanized_table[key] = info[key]
#     if info[key]['weight'] < THRESHOLD:
#         original_table[key] = info[key]
#
# for key in origanized_table:
#     origanized_table[key]['size'] = origanized_table[key]['total_used_bit_count']*(1+origanized_table[key]['frequency'])
#     origanied_size = origanied_size + origanized_table[key]['size']
#
# for key in original_table:
#     original_table[key]['size'] = ENTRY_BITS*len(open_table)-origanied_size

def decide_table_size(case, entry,memory_size,table_counts,total_rules):
    if case == 'equal':
        return math.floor(memory_size/table_counts)
    if case == 'mine':
        return entry['used_bit_count']*entry['reduced_df_length']*(1+(float(entry['reduced_df_length'])/total_rules))






schemas = []
weights = []
tablesizes = []

filteredEntryTableMap = {}
for key in entryTableMap :
    # print entryTableMap
    if entryTableMap[key]['weight'] > THRESHOLD :
        schemas.append(key.replace(',','$'))
        weights.append(entryTableMap[key]['weight'])
        filteredEntryTableMap[key] = entryTableMap[key]


table_counts = len(schemas)+1
memory_size = MEMORY_SIZE
for key in entryTableMap :
    # print entryTableMap
    if entryTableMap[key]['weight'] > THRESHOLD :
        tablesizes.append(decide_table_size(args.case,entryTableMap[key],memory_size,table_counts,total_rules))


special_table_size = memory_size - np.sum(tablesizes)
schemas.append('IP_PROTO$IPV4_SRC$IPV4_DST$PORT_SRC$PORT_DST$FLAG')
weights.append(0)

if args.case == 'equal' :
    tablesizes.append(math.floor(memory_size/table_counts))
if args.case == 'mine' :
    tablesizes.append(special_table_size)


df_org = pd.DataFrame({
    'schema': schemas,
    'weights': weights,
    'tablesize': tablesizes,
})

# print (total_rules)
#
# print(df_org)

df_org.to_csv('org_table.csv')

sys.exit()
