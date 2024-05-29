from datetime import datetime
from function import *

# load datasets
layout_1, layout_2, layout_3, layout_4, layout_5 = read_data()

# store lengths of dataframes
store(layout_1, layout_2, layout_3, layout_4, layout_5)

# created combined column for each layout
df_lay1 = combine_columns(layout_1.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
df_lay2 = combine_columns(layout_2.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
df_lay3 = combine_columns(layout_3.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
df_lay4 = combine_columns(layout_4.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
df_lay5 = combine_columns(layout_5.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')

# merge layout 1 and layout 2
c_df1 = combine(layout_1, layout_2, df_lay1, df_lay2, 'Customer Code', 'Mobile Number')
c_df1 = merge_columns(c_df1.copy())

# merge layout 3
df_c_df1 = combine_columns(c_df1.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
c_df2 = combine(c_df1, layout_3, df_c_df1, df_lay3, 'Customer Code', 'votersID')
c_df2 = merge_columns(c_df2.copy())

# merge layout 4
df_c_df2 = combine_columns(c_df2.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
c_df3 = combine(c_df2, layout_4, df_c_df2, df_lay4, 'Customer Code', 'Electricity Bill ID')
c_df3 = merge_columns(c_df3.copy())

# merge layout 5
df_c_df3 = combine_columns(c_df3.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
c_df4 = combine(c_df3, layout_5, df_c_df3, df_lay5, 'Customer Code', 'License Number')
c_df4 = merge_columns(c_df4.copy())

# create final table
c_df4['Updated Date'] = datetime.today().strftime('%Y-%m-%d')

# create unique universal id for each record
final_df = generate_unique_id(c_df4.copy())

# save to a csv file
final_df.to_csv('../datasets/final_table.csv', index=False)
