from synop import synop

report = "AAXX 01031 28877 /1598 70603 10026 21007 39840 40241 52009 70282 87500 555 20026 31002 69902 7990/="
report = report.replace("=", " ")

syn = synop(report)
sec_0 = syn.decoded['section_0']
sec_1 = syn.decoded['section_1']
sec_3 = syn.decoded['section_3']
sec_5 = syn.decoded['section_5']


#get all synop variables as a dict
variables = ['station_id', 't_air', 'dewp', 'p_baro', 'p_slv', 'precip', 't_min', 't_max']
syn_dict = syn.to_dict(variables)
