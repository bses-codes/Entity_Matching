import csv
import string
import random
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors

# read data
def read_data():
    layout_1 = pd.read_csv('../datasets/layout/ABC_layout_1.csv')
    layout_2 = pd.read_csv('../datasets/layout/PQR_layout_2.csv')
    layout_3 = pd.read_csv('../datasets/layout/XYZ_layout_3.csv')
    layout_4 = pd.read_csv('../datasets/layout/KLM_layout_4.csv')
    layout_5 = pd.read_csv('../datasets/layout/DOTM_layout_5.csv')
    return layout_1, layout_2, layout_3, layout_4, layout_5

# retrieve length of datasets
def retrieve():
    lengths_from_file = {}
    with open('../datasets/dataframe_lengths.csv', mode='r') as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header row
        for row in reader:
            dataframe_name, length = row
            lengths_from_file[dataframe_name] = int(length)
    return lengths_from_file


# store length of datasets
def store(lay1, lay2, lay3, lay4, lay5):
    dataframes = [lay1, lay2, lay3, lay4, lay5]
    lengths = {f'lay{i + 1}': len(dataframe) for i, dataframe in enumerate(dataframes)}
    with open('../datasets/dataframe_lengths.csv', mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['DataFrame', 'Length'])
        for name, length in lengths.items():
            writer.writerow([name, length])


# merge duplicate columns
def merge_columns(df):
    # List columns ending with _x and _y
    merge_cols = [col[:-2] for col in df.columns if col.endswith('_x')]

    for col in merge_cols:
        col_x = f'{col}_x'
        col_y = f'{col}_y'
        df[col] = df[col_x].combine_first(df[col_y])

    # Drop _x and _y columns
    drop_cols = [col for col in df.columns if col.endswith('_x') or col.endswith('_y')]
    df.drop(drop_cols, axis=1, inplace=True)

    return df


# transform selected columns and combine them into a single column
def combine_columns(df, column_names, new_column_name):
    if not all(col in df.columns for col in column_names):
        raise ValueError("Some column names do not exist in the DataFrame")

    def transform_date(date):
        if pd.isna(date):
            return ''
        f_date = date.replace('-', '')
        return f_date

    def transform_address(address):
        if pd.isna(address):
            return ''
        address = address.replace(',', '').replace(' ', '').replace('Nepal', '')
        return address

    def transform_name(name):
        if pd.isna(name):
            return ''
        f_name = name.replace(' ', '')
        return f_name

    df.columns = df.columns.str.lower()
    columns = [item.lower() for item in column_names]
    for col in columns:
        if 'date' in col:
            df[col] = df[col].apply(transform_date)
        elif 'address' in col:
            df[col] = df[col].apply(transform_address)
        elif 'name' in col:
            df[col] = df[col].apply(transform_name)
    df[new_column_name] = df[columns].astype(str).agg(' '.join, axis=1)
    return df


# perform entity matching and merge the dataframes
def combine(odf, odf1, df, df1, Pid1, Pid2):
    vectorizer = TfidfVectorizer()
    df_tfidf_matrix = vectorizer.fit_transform(df['combined'])
    df1_tfidf_matrix = vectorizer.transform(df1['combined'])
    knn = NearestNeighbors(metric='cosine', algorithm='brute')
    knn.fit(df_tfidf_matrix)
    distances, indices = knn.kneighbors(df1_tfidf_matrix, n_neighbors=1)

    matches = []
    for i in range(len(df1)):
        match_id = df.iloc[indices[i][0]][Pid1.lower()]
        lookup_id = df1.iloc[i][Pid2.lower()]
        distance = distances[i][0]
        if distance < 0.5:
            matches.append((lookup_id, match_id, distance))
        else:
            matches.append((lookup_id, None, distance))

    matches_df = pd.DataFrame(matches, columns=['lookup_id', 'matched_id', 'distance'])

    odf1 = odf1.merge(matches_df[['matched_id', 'lookup_id']], left_on=Pid2, right_on='lookup_id', how='left')
    odf1.drop(columns=['lookup_id'], inplace=True)

    merged_df = odf1.merge(odf, left_on='matched_id', right_on=Pid1, how='outer')
    merged_df.drop(columns=['matched_id'], inplace=True)

    return merged_df


# generate unique id for each entry in dataframe
def generate_unique_id(df):
    if 'uid' not in df.columns:
        df['uid'] = ''

    for i in range(len(df)):
        if df.loc[i, 'uid'] == '' or pd.isna(df.loc[i, 'uid']):
            chars = string.ascii_uppercase + string.digits
            uid = ''.join(random.choice(chars) for _ in range(10))
            while uid in df['uid'].unique():
                uid = ''.join(random.choice(chars) for _ in range(10))
            df.loc[i, 'uid'] = uid

    return df


# Add new records to existing final_df
def add_new_values(final_df, list_of_layouts_with_ID):
    for ID, layout in list_of_layouts_with_ID.items():
        if len(layout) > 0:
            df = combine_columns(layout.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
            df1 = combine_columns(final_df.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
            new_df = combine(final_df, layout, df1, df, 'uid', ID)
            new_df = merge_columns(new_df.copy())
            con_final_df = pd.concat([final_df, new_df], ignore_index=True)
            con_final_df['non_null_count'] = con_final_df.notnull().sum(axis=1)
            con_final_df = con_final_df.sort_values(by=['uid', 'non_null_count'], ascending=[True, False])
            con_final_df = con_final_df.drop_duplicates(subset='uid', keep='first')
            con_final_df = con_final_df.drop(columns='non_null_count')
            con_final_df = con_final_df.reset_index()
            new_final_df = generate_unique_id(con_final_df.copy())
            final_df = new_final_df
    return final_df
