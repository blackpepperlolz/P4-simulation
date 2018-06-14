"""
python simulation.py --orgtable org_table.csv --packetset acl1_rule_table_2
"""
import pandas as pd
import numpy as np
import functools
import sys
import argparse
import math
import time


parser = argparse.ArgumentParser()

parser.add_argument(
   '--orgtable',
   dest = 'org_t',
  type=str
)

parser.add_argument(
   '--packetset',
   dest = 'packetset',
  type=str
)

args = parser.parse_args()





def import_df_org(filepath):
    entryTableMap = {}
    statisticMap = {}

    df_org = pd.read_csv(filepath,index_col = 0)

    for index, row in df_org.iterrows():
        keys = row['schema'].split('$')
        id = ','.join(keys)
        keys += ['MATCH_TIMES','TIMER']
        new_df = pd.DataFrame(columns=keys)

        entryTableMap[id] = new_df
        statisticMap[id] = {
            'tablesize': row['tablesize'],
            'currentusedspace': 0,
            'oneEntrySize': row['oneEntrySize'],
            'overflow_count': 0,
        }

    return entryTableMap, statisticMap

def import_packet_header(filepath):
     df_header = pd.read_csv(filepath,index_col = False)

     return df_header


def sample_packet(header_set):

    header = header_set.sample(n=1,replace=True)
    return header

def to_controller(header,tablemap):
    fields_to_keep = []

    if not header['IPV4_DST'].values[0] == '0.0.0.0/0':
        fields_to_keep.append('IPV4_DST')

    if not header['IPV4_SRC'].values[0] == '0.0.0.0/0':
        fields_to_keep.append('IPV4_SRC')

    if not header['IP_PROTO'].values[0] == '0x00/0x00':
        fields_to_keep.append('IP_PROTO')

    if not header['PORT_SRC'].values[0] == '0:65535':
        fields_to_keep.append('PORT_SRC')

    if not header['PORT_DST'].values[0] == '0:65535':
        fields_to_keep.append('PORT_DST')

    if not header['FLAG'].values[0] == '0x0000/0x0000':
        fields_to_keep.append('FLAG')

    # print (fields_to_keep,'@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@')
    new_header = header.loc[:, fields_to_keep]
    fields_to_keep.sort()
    # print (fields_to_keep,'=====================')
    new_header_fields = ','.join(fields_to_keep)
    # print (new_header_fields,'=====================')

    if new_header_fields in tablemap:
        # if ('IPV4_DST' in fields_to_keep) and ('IPV4_SRC' in fields_to_keep) and ('PORT_DST' in fields_to_keep):
        #     print('\n')
        #     print('++++++++++++++++++++++++++++++++')
        #     print(fields_to_keep)
        #     print(new_header)

        # if new_header_fields == 'IPV4_DST,IPV4_SRC,PORT_DST':
        #     print('++++++++++++++++++++++++++++++++', new_header)
        return new_header_fields , new_header
    else:
        keys = header.columns
        zippedReturnValue = zip(keys, header.values[0])
        zippedReturnValue.sort(key=lambda pair: pair[0])
        columns, values = [x[0] for x in zippedReturnValue], [x[1] for x in zippedReturnValue]
        new_header = pd.DataFrame([values], columns=columns)
        return ','.join(columns), new_header



def match(tablemap,statisticMap,header):
    is_matched = False

    for id in tablemap:
        df = tablemap[id]
        columns = id.split(',')
        # table_fields = df.columns.values
        # print(table_fields)
        reduced_header = header.loc[:,columns]
        # print(reduced_header)

        mask_of_all_columns = np.repeat(True, len(df))
        for column in columns:
            mask_of_current_column = (df[column] == header[column].values[0])
            mask_of_all_columns = mask_of_all_columns & mask_of_current_column

        matched = df[mask_of_all_columns]
        is_matched = (len(matched) > 0)

        if is_matched:
            # print("****************")
            idx = list(mask_of_all_columns).index(True)
            # try:
            # tablemap[id].loc[idx, 'MATCH_TIMES'] = tablemap[id]['MATCH_TIMES'].iloc[idx] + 1
            tablemap[id]['MATCH_TIMES'].iloc[idx] += 1
            # except:
            #     print('\n\nEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE\n\n')
                # print(id)
                # print(idx)
                # print(tablemap[id].iloc[idx])
                # print(tablemap[id]['MATCH_TIMES'].iloc[idx])
            tablemap[id]['TIMER'].iloc[idx] = time.time()
            break

    if not is_matched:
        if statisticMap[id]['tablesize'] <= statisticMap[id]['currentusedspace']+statisticMap[id]['oneEntrySize']:
            index = tablemap[id]['TIMER'].idxmin()
            tablemap[id].drop(index, inplace=True)
            statisticMap[id]['overflow_count']  = statisticMap[id]['overflow_count'] +1
            # print(statisticMap[id]['overflow_count'])
            statisticMap[id]['currentusedspace']  = statisticMap[id]['currentusedspace'] -statisticMap[id]['oneEntrySize']
            # print(statisticMap[id]['overflow_count'])


        id,new_header = to_controller(header,tablemap)
        # new_value = list(new_header.values[0])
        # new_value += [1, time.time() ]
        new_header['MATCH_TIMES'] = [1]
        new_header['TIMER'] = [time.time()]
        statisticMap[id]['currentusedspace']  = statisticMap[id]['currentusedspace'] +statisticMap[id]['oneEntrySize']
        tablemap[id] = tablemap[id].append(new_header)
        # print(tablemap[id])

def statistic(statisticMap,packetcount):
    sum_memorysize = 0
    sum_usedspace = 0
    sum_overflowcount = 0
    row_count_map = {}
    # ids = []
    # used_row_counts = []

    for id in statisticMap:
        sum_memorysize = sum_memorysize + statisticMap[id]['tablesize']
        sum_usedspace = sum_usedspace + statisticMap[id]['currentusedspace']
        sum_overflowcount = sum_overflowcount + statisticMap[id]['overflow_count']

        row_count_map[id] = [statisticMap[id]['currentusedspace'] / statisticMap[id]['oneEntrySize']]
        # ids.append(id)
        # used_row_counts.append(statisticMap[id]['currentusedspace'] / statisticMap[id]['oneEntrySize'])

    # print (statisticMap)
    # print(used_row_counts)
    # sum_entrycount = float(sum_usedspace)/ statisticMap[id]['oneEntrySize']
    return float(sum_usedspace)/sum_memorysize, float(sum_overflowcount)/packetcount, pd.DataFrame(row_count_map)
    # print(records)
    # return float(sum_usedspace)/sum_memorysize, float(sum_overflowcount)/packetcount


def main():

    # print (entryTableMap)
    header_set = import_packet_header(args.packetset)
    PACKETCOUNT = 10000
    ITERATION = 10
    records = np.empty((0, 2))
    used_entry_count = pd.DataFrame()
    for _ in range(ITERATION):
        entryTableMap, statisticMap = import_df_org(args.org_t)
        for _ in range(PACKETCOUNT):
            header = sample_packet(header_set)
            match(entryTableMap,statisticMap,header)


        a,b,used_row_count = statistic(statisticMap,PACKETCOUNT)
        # print(used_row_count)
        used_entry_count = used_entry_count.append(used_row_count)
        # print(used_entry_count)
        records = np.concatenate((records, [[a, b]]), axis=0)
    print(records)
    print(used_entry_count)
    print(np.mean(records, axis=0))
    # for id in entryTableMap:
    #     print('\n\n')
    #     print(id)
    #     print('============')
    #     print (entryTableMap[id])
    # # print (header_set)
    # # print (header)
    # print (statisticMap)
main()
