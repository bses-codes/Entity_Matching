import pandas as pd
from function import *

# retrieve the index of dataframes to begin with
df_lengths = retrieve()

# read the data
final_df = pd.read_csv('../datasets/final_table.csv', index_col=0)

layout_1 = pd.read_csv('../datasets/layout/ABC_layout_1.csv')
layout_1_c = layout_1.copy().loc[df_lengths['lay1']:]

layout_2 = pd.read_csv('../datasets/layout/PQR_layout_2.csv')
layout_2_c = layout_2.copy().loc[df_lengths['lay2']:]

layout_3 = pd.read_csv('../datasets/layout/XYZ_layout_3.csv')
layout_3_c = layout_3.copy().loc[df_lengths['lay3']:]

layout_4 = pd.read_csv('../datasets/layout/KLM_layout_4.csv')
layout_4_c = layout_4.copy().loc[df_lengths['lay4']:]

layout_5 = pd.read_csv('../datasets/layout/DOTM_layout_5.csv')
layout_5_c = layout_5.copy().loc[df_lengths['lay5']:]

store(layout_1, layout_2, layout_3, layout_4, layout_5)

dict_layout = {
    'Customer Code': layout_1_c,
    'Phone Number': layout_2_c,
    'votersID': layout_3_c,
    'Electricity Bill ID': layout_4_c,
    'License Number': layout_5_c
}

new_final_df = add_new_values(final_df, dict_layout)
new_final_df.to_csv('../datasets/final_table.csv', index=False)
