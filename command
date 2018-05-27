python rawdata_to_rule_table.py --rawdata acl1_2000 --ruletable acl1_rule_table

python rule_metadata.py --rawdata acl1_2000 --ruletable acl1_rule_table --analysis acl1_analysis

python format_trans.py --analysis acl1_analysis --trans acl1_trans

python p4_test_ver3.py --trans acl1_trans_2 --sizecase equal
