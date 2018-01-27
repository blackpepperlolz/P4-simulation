
# coding: utf-8

# In[23]:


import pandas as pd
import numpy as np


# In[24]:

"""
Fields bit
"""
bits = ['32','48','48','16','8','32','32','128','128','16','16','16','16']
column_names = ['IN_PORT','EST_DST','EST_SRC','ETH_TYPE','IP_PROTO','IPv4_SRC','IPv4_DST','IPv6_SRC','IPv6_DST','TCP_SRC','TCP_DST','UDP_SRC','UDP_DST']
open_field_bits =  pd.DataFrame(columns = column_names)
open_field_bits.loc[0] = bits

"""
Genterating the original table
"""
case_selection = 2
num_entry = 30

"""
High Dimension
"""
if case_selection == 1:
    MAC_IP4_TCP = pd.DataFrame([[1,1,1,1,1,1,1,0,0,1,1,0,0]], columns=column_names)
    MAC_IP4_UCP = pd.DataFrame([[1,1,1,1,1,1,1,0,0,0,0,1,1]], columns=column_names)
    MAC_IP6_TCP = pd.DataFrame([[1,1,1,1,1,0,0,1,1,1,1,0,0]], columns=column_names)
    MAC_IP6_UDP = pd.DataFrame([[1,1,1,1,1,0,0,1,1,0,0,1,1]], columns=column_names)
    open_table = pd.DataFrame(columns = column_names)
    for i in range(num_entry):
        entry_select = np.random.randint(4)
        if entry_select == 0:
            open_table = open_table.append(MAC_IP4_TCP, ignore_index=True)
        if entry_select == 1:
            open_table = open_table.append(MAC_IP4_UCP, ignore_index=True)
        if entry_select == 2:
            open_table = open_table.append(MAC_IP6_TCP, ignore_index=True)
        if entry_select == 3:
            open_table = open_table.append(MAC_IP6_UCP, ignore_index=True)

"""
Midium Dimension
"""

if case_selection == 2:
    MAC_IP4 = pd.DataFrame([[1,1,1,1,1,1,1,0,0,0,0,0,0]], columns=column_names)
    MAC_IP6 = pd.DataFrame([[1,1,1,1,1,0,0,1,1,0,0,0,0]], columns=column_names)
    open_table = pd.DataFrame(columns = column_names)
    for i in range(num_entry):
        entry_select = np.random.randint(2)
        if entry_select == 0:
            open_table = open_table.append(MAC_IP4, ignore_index=True)
        if entry_select == 1:
            open_table = open_table.append(MAC_IP6, ignore_index=True)

"""
Low Dimension
"""
if case_selection == 3:
    MAC = pd.DataFrame([[0,1,1,0,0,0,0,0,0,0,0,0,0]], columns=column_names)
    IP4 = pd.DataFrame([[0,0,0,1,0,1,1,0,0,0,0,0,0]], columns=column_names)
    IP6 = pd.DataFrame([[0,0,0,1,0,0,0,1,1,0,0,0,0]], columns=column_names)
    TCP = pd.DataFrame([[0,0,0,0,1,0,0,0,0,1,1,0,0]], columns=column_names)
    UDP = pd.DataFrame([[0,0,0,0,1,0,0,0,0,0,0,1,1]], columns=column_names)
    open_table = pd.DataFrame(columns = column_names)
    for i in range(num_entry):
        entry_select = np.random.randint(5)
        if entry_select == 0:
            open_table = open_table.append(MAC, ignore_index=True)
        if entry_select == 1:
            open_table = open_table.append(IP4, ignore_index=True)
        if entry_select == 2:
            open_table = open_table.append(IP6, ignore_index=True)
        if entry_select == 3:
            open_table = open_table.append(TCP, ignore_index=True)
        if entry_select == 4:
            open_table = open_table.append(UDP, ignore_index=True)

"""
Random
"""
if case_selection == 4:
    open_table = pd.DataFrame(np.random.randint(low=0, high=2, size=(num_entry, 13)),columns = column_names)


print (open_table)

def find_used(entry_row):
    used_set = []
    for column in column_names:
        value = entry_row[column]
        if (value == 1):
            used_set.append(column)
    sorted(used_set)
    return used_set


# In[26]:
def reduce_df_column(raw_series):
    filtered = raw_series[raw_series == 1]
    return filtered.to_frame().T

dfs = {}


for idx, row in open_table.iterrows():
    used_fields = find_used(row)
    used_fields_key = ','.join(used_fields)

    add_entry = reduce_df_column(row)
    if used_fields_key in dfs:
        dfs[used_fields_key] = dfs[used_fields_key].append(add_entry, ignore_index=True)
    else:
        new_df = pd.DataFrame(columns = used_fields)
        new_df = new_df.append(add_entry, ignore_index=True)
        dfs[used_fields_key] = new_df
    # print(dfs[used_fields_key])

print (dfs)
