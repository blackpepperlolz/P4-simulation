
# coding: utf-8

# In[23]:


import pandas as pd
import numpy as np
import functools

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
    'TCP_SRC': 16,
    'TCP_DST': 16,
    'UDP_SRC': 16,
    'UDP_DST': 16,
}
COLUMN_NAMES = BIT_MAP.keys()
HIGH_DIM = 1
MIDIUM_DIM = 2
LOW_DIM = 3
RANDOM = 4

"""
Genterating the original table
"""
"""
High Dimension
"""
def entry_generator(case_selection, num_entry = 30):
    if case_selection == HIGH_DIM:
        MAC_IPV4_TCP = pd.DataFrame([[1,1,1,1,1,1,1,0,0,1,1,0,0]], columns=COLUMN_NAMES)
        MAC_IPV4_UCP = pd.DataFrame([[1,1,1,1,1,1,1,0,0,0,0,1,1]], columns=COLUMN_NAMES)
        MAC_IPV6_TCP = pd.DataFrame([[1,1,1,1,1,0,0,1,1,1,1,0,0]], columns=COLUMN_NAMES)
        MAC_IPV6_UDP = pd.DataFrame([[1,1,1,1,1,0,0,1,1,0,0,1,1]], columns=COLUMN_NAMES)
        open_table = pd.DataFrame(columns = COLUMN_NAMES)
        for i in range(num_entry):
            entry_select = np.random.randint(4)
            if entry_select == 0:
                open_table = open_table.append(MAC_IPV4_TCP, ignore_index=True)
            if entry_select == 1:
                open_table = open_table.append(MAC_IPV4_UCP, ignore_index=True)
            if entry_select == 2:
                open_table = open_table.append(MAC_IPV6_TCP, ignore_index=True)
            if entry_select == 3:
                open_table = open_table.append(MAC_IPV6_UDP, ignore_index=True)
        return open_table

    """
    Midium Dimension
    """
    if case_selection == MIDIUM_DIM:
        MAC_IPV4 = pd.DataFrame([[1,1,1,1,1,1,1,0,0,0,0,0,0]], columns=COLUMN_NAMES)
        MAC_IPV6 = pd.DataFrame([[1,1,1,1,1,0,0,1,1,0,0,0,0]], columns=COLUMN_NAMES)
        open_table = pd.DataFrame(columns = COLUMN_NAMES)
        for i in range(num_entry):
            entry_select = np.random.randint(2)
            if entry_select == 0:
                open_table = open_table.append(MAC_IPV4, ignore_index=True)
            if entry_select == 1:
                open_table = open_table.append(MAC_IPV6, ignore_index=True)
        return open_table

    """
    Low Dimension
    """
    if case_selection == LOW_DIM:
        MAC = pd.DataFrame([[0,1,1,0,0,0,0,0,0,0,0,0,0]], columns=COLUMN_NAMES)
        IPV4 = pd.DataFrame([[0,0,0,1,0,1,1,0,0,0,0,0,0]], columns=COLUMN_NAMES)
        IPV6 = pd.DataFrame([[0,0,0,1,0,0,0,1,1,0,0,0,0]], columns=COLUMN_NAMES)
        TCP = pd.DataFrame([[0,0,0,0,1,0,0,0,0,1,1,0,0]], columns=COLUMN_NAMES)
        UDP = pd.DataFrame([[0,0,0,0,1,0,0,0,0,0,0,1,1]], columns=COLUMN_NAMES)
        open_table = pd.DataFrame(columns = COLUMN_NAMES)
        for i in range(num_entry):
            entry_select = np.random.randint(5)
            if entry_select == 0:
                open_table = open_table.append(MAC, ignore_index=True)
            if entry_select == 1:
                open_table = open_table.append(IPV4, ignore_index=True)
            if entry_select == 2:
                open_table = open_table.append(IPV6, ignore_index=True)
            if entry_select == 3:
                open_table = open_table.append(TCP, ignore_index=True)
            if entry_select == 4:
                open_table = open_table.append(UDP, ignore_index=True)
        return open_table

    """
    Random
    """
    if case_selection == RANDOM:
        open_table = pd.DataFrame(np.random.randint(low=0, high=2, size=(num_entry, 13)),columns = COLUMN_NAMES)
        return open_table



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
info = {}
open_table = entry_generator(HIGH_DIM)

for idx, row in open_table.iterrows():
    used_fields = find_used(row)
    used_fields_key = ','.join(used_fields)

    add_entry = reduce_df_column(row)
    if used_fields_key in info:
        info[used_fields_key]['reduced_TF_df'] = info[used_fields_key]['reduced_TF_df'].append(add_entry, ignore_index=True)
    else:
        unused_fields = list(row[row == 0].axes[0])
        unsed_bit_count = functools.reduce((lambda acc, field: acc + BIT_MAP[field]), unused_fields, 0)
        new_df = pd.DataFrame(columns = used_fields)
        new_df = new_df.append(add_entry, ignore_index=True)
        info[used_fields_key] = {
            'reduced_TF_df': new_df,
            'unsed_bit_count': unsed_bit_count,
            'total_unsed_bit_count': -1,
            'reduced_TF_df_length': -1,
            'weight': -1,
        }

for key in info:
    length = len(info[key]['reduced_TF_df'])
    info[key]['reduced_TF_df_length'] = length
    info[key]['total_unsed_bit_count'] = length * info[key]['unsed_bit_count']

sorted_info_by_total = sort_info(info, 'total_unsed_bit_count')
sorted_info_by_length = sort_info(info, 'reduced_TF_df_length')

print(sorted_info_by_total, '\n\n============\n\n')
print(sorted_info_by_length, '\n\n============\n\n')

weight = len(info)
for key in sorted_info_by_total:
    info[key]['weight'] = weight
    weight = weight - 1

print(info, '\n\n============\n\n')
