
# coding: utf-8

# In[23]:


import pandas as pd
import numpy as np


# In[24]:


bits = ['32','48','48','16','8','32','32','128','128','16','16','16','16']
column_names = ['IN_PORT','EST_DST','EST_SRC','ETH_TYPE','IP_PROTO','IPv4_SRC','IPv4_DST','IPv6_SRC','IPv6_DST','TCP_SRC','TCP_DST','UDP_SRC','UDP_DST']
open_field_bits =  pd.DataFrame(columns = column_names)
open_field_bits.loc[0] = bits

open_table = pd.DataFrame(np.random.randint(low=0, high=2, size=(10000, 13)),columns = ['IN_PORT','EST_DST','EST_SRC','ETH_TYPE','IP_PROTO','IPv4_SRC','IPv4_DST','IPv6_SRC','IPv6_DST','TCP_SRC','TCP_DST','UDP_SRC','UDP_DST'])


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
    print(dfs[used_fields_key])

print (dfs)
