import pandas as pd
import numpy as np

def clean_id_cols(df, id_cols):
    df_new = df.copy()
    for col in id_cols:
        if col in df.columns:
            df_new[col] = df_new[col].apply(lambda id: "{:.0f}".format(id) if isinstance(id, (float,  np.float64, np.float32)) and not pd.isnull(id) else id)
    return df_new

def cast_cols_to_numeric(df, cols_to_cast):
    df_new = df.copy()
    for col in  cols_to_cast:
        if col in df.columns:
            df_new[col] = pd.to_numeric(df_new[col], errors="coerce")
        
    return df_new

def drop_all_na_rows(df):
    return df.dropna(how='all')

def drop_duplicates(df):
    return df.drop_duplicates()

def fill_na_values(df, cols_to_fill, fill=np.nan):
    df_new = df.copy()
    for col in cols_to_fill:
        if col in df.columns:
            df_new[col] = df[col].fillna(fill)
        
    return df_new

def convert_bool_or_str_to_numeric(df, cols_to_convert):
    mapping = {
        True: 1,
        False: 0,
        'True': 1,
        'true': 1,
        'False': 0,
        'false': 0,
    }
    
    df_new = df.copy()
    
    for col in cols_to_convert:
        if col in df_new.columns:
            df_new[col] = df_new[col].replace(mapping).fillna(0)
            df_new[col] = df_new[col].astype("int32")
    
    return df_new

def transform_bool_or_str_to_text(df, cols_to_tranform, true_text, false_text, null=""):
    mapping = {
        True: true_text,
        False: false_text,
        'True': true_text,
        'true': true_text,
        'False': false_text,
        'false': false_text,
    }
    
    df_new = df.copy()
    
    for col in cols_to_tranform:
        if col in df_new.columns:
            df_new[col] = df_new[col].replace(mapping).fillna(null)
            df_new[col] = df_new[col].astype("object")
    
    return df_new