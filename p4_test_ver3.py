# coding: utf-8

# In[23]:


import pandas as pd
import numpy as np
import functools
import sys
# In[24]:

"""
Fields bit
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
HIGH_DIM = 1
MIDIUM_DIM = 2
LOW_DIM = 3
RANDOM = 4
TEST_CASE = 5
TIME = 1000 # miniseconds
THRESHOLD = 1 # weighting threshold
ENTRY_BITS = 504


"""
Genterating the original table
"""

# IN_PORT = pd.DataFrame([[1,0,0,0,0,0,0,0,0,0,0]], columns=COLUMN_NAMES)
# EST_DST = pd.DataFrame([[0,1,0,0,0,0,0,0,0,0,0]], columns=COLUMN_NAMES)
# EST_SRC = pd.DataFrame([[0,0,1,0,0,0,0,0,0,0,0]], columns=COLUMN_NAMES)
# EST_DST_EST_SRC= pd.DataFrame([[0,1,1,0,0,0,0,0,0,0,0]], columns=COLUMN_NAMES)
# # IP_DST = pd.DataFrame([[0,0,0,0,0,0,1,0,0,0,0]], columns=COLUMN_NAMES)
# IPV4_SRC = pd.DataFrame([[0,0,0,0,0,1,0,0,0,0,0]], columns=COLUMN_NAMES)
# IPV4_DST = pd.DataFrame([[0,0,0,0,0,0,1,0,0,0,0]], columns=COLUMN_NAMES)
# ETH_TYPE_IPV4_SRC = pd.DataFrame([[0,0,0,1,0,1,0,0,0,0,0]], columns=COLUMN_NAMES)
# ETH_TYPE_IPV4_DST = pd.DataFrame([[0,0,0,1,0,0,1,0,0,0,0]], columns=COLUMN_NAMES)
# ETH_TYPE_IPV4_SRC_IPV4_DST = pd.DataFrame([[0,0,0,1,0,1,1,0,0,0,0]], columns=COLUMN_NAMES)
# IPV6_SRC = pd.DataFrame([[0,0,0,1,0,0,0,1,0,0,0]], columns=COLUMN_NAMES)
# IPV6_DST = pd.DataFrame([[0,0,0,1,0,0,0,0,1,0,0]], columns=COLUMN_NAMES)
# ETH_TYPE_IPV6_SRC_IPV6_DST = pd.DataFrame([[0,0,0,1,0,0,0,1,1,0,0]], columns=COLUMN_NAMES)
# PORT_SRC = pd.DataFrame([[0,0,0,0,1,0,0,0,0,1,0]], columns=COLUMN_NAMES)
# PORT_DST = pd.DataFrame([[0,0,0,0,1,0,0,0,0,0,1]], columns=COLUMN_NAMES)
# PORT_SRC_PORT_DST = pd.DataFrame([[0,0,0,0,1,0,0,0,0,1,1]], columns=COLUMN_NAMES)
# IP_PROTO = pd.DataFrame([[0,0,0,0,1,0,0,0,0,0,0]], columns=COLUMN_NAMES)
# UDP_SRC = pd.DataFrame([[0,0,0,0,0,0,0,0,0,0,0,0]], columns=COLUMN_NAMES)
# UDP_DST = pd.DataFrame([[0,0,0,0,0,0,0,0,0,0,0,0]], columns=COLUMN_NAMES)
# def entry_generator(case_selection, num_entry = 30):
#     """
#     Test
#     """
#     if case_selection == TEST_CASE:
#         """
#         high test case
#         """
#         open_table = pd.DataFrame(columns = COLUMN_NAMES)
#         for i in range(num_entry):
#             entry_select = np.random.randint(9)
#             noise = np.random.randint(100)
#             if entry_select == 0:
#                 open_table = open_table.append(IN_PORT, ignore_index=True)
#             if entry_select == 1:
#                 open_table = open_table.append(EST_DST, ignore_index=True)
#             if entry_select == 2:
#                 open_table = open_table.append(IPV4_DST, ignore_index=True)
#             if entry_select == 3:
#                 open_table = open_table.append(ETH_TYPE_IPV4_SRC_IPV4_DST, ignore_index=True)
#             if entry_select == 4:
#                 open_table = open_table.append(ETH_TYPE_IPV4_SRC_IPV4_DST, ignore_index=True)
#             if entry_select == 5:
#                 open_table = open_table.append(ETH_TYPE_IPV4_SRC_IPV4_DST, ignore_index=True)
#             if entry_select == 6 :
#                 open_table = open_table.append(ETH_TYPE_IPV6_SRC_IPV6_DST, ignore_index=True)
#             if entry_select == 7  :
#                 open_table = open_table.append(PORT_SRC_PORT_DST, ignore_index=True)
#             if entry_select == 8  :
#                 open_table = open_table.append(PORT_SRC_PORT_DST, ignore_index=True)
#         #     if entry_select == 6 :
#         #         open_table = open_table.append(MAC_IPV6_TCP, ignore_index=True)
#         #     if entry_select == 7:
#         #         open_table = open_table.append(MAC_IPV6_UDP, ignore_index=True)
#             if noise == 10 :
#                 # noise_pd = pd.DataFrame(np.random.randint(low=0, high=2, size=(1, 13)),columns = COLUMN_NAMES)
#                 open_table = open_table.append(IP_PROTO, ignore_index=True)
#         return open_table



def find_used(entry_row):
    used_set = []
    for column in COLUMN_NAMES:
        value = entry_row[column]
        if (value == 1):
            used_set.append(column)
    sorted(used_set)
    return used_set


# In[26]:
def reduce_df_column(raw_series):
    used_fields = raw_series[raw_series == 1]
    return used_fields.to_frame().T

def sort_info(raw_info, by_key):

    sorted_info = {}
    sorted_list = sorted(
        raw_info.items(),
        key=(lambda x: x[1][by_key]),
        reverse=True
    )
    for item_value in sorted_list:
        sorted_info[item_value[0]] = item_value[1]
    return sorted_info

"""
Initial
"""
class EntryTable:
    def __init__(self,used_fields,is_fixed = False,unused_bit_count = 0):
        self.df = pd.DataFrame(columns = used_fields)
        self.dict= {'total_unused_bit_count': 0,'reduced_df_length': 0,}
        self.key = ','.join(used_fields)
        self.used_bit_count = 0
        self.unused_bit_count = unused_bit_count
        self.total_used_bit_count = 0
        # self.total_unused_bit_count = 0
        # self.reduced_df_length = 0
        self.weight = 0
        self.frequency = 0
        self.size = 0

    def append(self, add_entry):
        self.df = self.df.append(add_entry, ignore_index=True)

    def update(self,time,entry_bits):
        length = len(self.df)
        self.dict['reduced_df_length'] = length
        self.used_bit_count = entry_bits - self.unused_bit_count
        self.total_used_bit_count = length * self.used_bit_count
        self.dict['total_unused_bit_count'] = length * self.unused_bit_count
        self.frequency = self.dict['reduced_df_length']/time

    def unused_count(self,bit_map,unused_fields):
        self.unused_bit_count = functools.reduce((lambda acc, field: acc + bit_map[field]), unused_fields, 0)


    def weight_cal(self,weight_val):
        self.weight = self.weight + weight_val

    def __getitem__(self,key):
        return self.dict[key]
    def __setitem__(self,key,value):
        self.dict[key] = value

    def __repr__(self):
        # return self.df.to_string()
        return ("%s\n"  %(self.key)
                +" weight : %s\n" %(self.weight)
                +" numbers of entry :　%s\n" % (self.dict['reduced_df_length'])
                +" used bits in an entry :　%s\n"%(self.used_bit_count)
                +" total used bits :　%s\n" % (self.total_used_bit_count)
                +" unused bits in an entry :　%s\n"%(self.unused_bit_count)
                +" total unused bits :　%s\n" % (self.dict['total_unused_bit_count'])
                +" frequency : %s\n" %(self.frequency)
                + "===============\n")

        # return self.key + '\n'

    def __str__(self):
        return self.key + '\n'




# open_table_test = entry_generator(TEST_CASE,1000)
open_table = pd.read_csv("tf_table")
print(open_table)
entryTableMap = {
'IN_PORT':EntryTable(['IN_PORT'],unused_bit_count = ENTRY_BITS-32),
'EST_DST':EntryTable(['EST_DST'],unused_bit_count = ENTRY_BITS-48),
'IPV4_DST':EntryTable(['IPV4_DST'],unused_bit_count = ENTRY_BITS-32),
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

# print(sorted_info_by_total, '\n\n============\n\n')
# print(sorted_info_by_length, '\n\n============\n\n')

# for key in info:
#     info[key]['frequency'] = info[key]['reduced_TF_df_length']/TIME
weight = len(entryTableMap)
for key in sorted_info_by_total:
    entryTableMap[key].weight_cal(weight)
    weight = weight - 1

weight = len(entryTableMap)
for key in sorted_info_by_length:
    entryTableMap[key].weight_cal(weight)
    weight = weight - 1

print(entryTableMap)
sys.exit()

# print(info,'\n\n============\n\n')

"""
Decide Size and table
"""
origanied_table = {}
original_table = {}
origanied_size = 0
for key in info:
    if info[key]['weight']>= THRESHOLD:
        origanied_table[key] = info[key]
    if info[key]['weight'] < THRESHOLD:
        original_table[key] = info[key]

for key in origanied_table:
    origanied_table[key]['size'] = origanied_table[key]['total_used_bit_count']*(1+origanied_table[key]['frequency'])
    origanied_size = origanied_size + origanied_table[key]['size']

for key in original_table:
    original_table[key]['size'] = ENTRY_BITS*len(open_table)-origanied_size




print(origanied_table,'\n\n============\n\n')
print(original_table,'\n\n============\n\n')
