import pandas as pd
import numpy as np
import json
import ast

def add_working_hours_columns(df, json_column):
    def extract_working_hours(row):
        if not pd.isnull(row):
            working_hours = ast.literal_eval(row)
            # Extract hours for Saturday, Sunday, and Monday (used for weekdays)
            saturday_hours = working_hours.get('Saturday', 'Closed')
            sunday_hours = working_hours.get('Sunday', 'Closed')
            weekdays_hours = working_hours.get('Monday', 'Closed') 
            return pd.Series([saturday_hours, sunday_hours, weekdays_hours])
        return pd.Series([None, None, None])

    df[['saturday_working_hours', 'sunday_working_hours', 'weekdays_working_hours']] = df[json_column].apply(extract_working_hours)

    return df

def add_about_columns(df, json_column):
    """
    Expands a JSON string column into multiple DataFrame columns
    """
    categories_to_include = [
        "From the business",
        "Service options",
        "Accessibility",
        "Crowd",
        "Planning",
        "Amenities"
    ]

    def process_json(json_str):
        try:
            data = json.loads(json_str)
            return {k: v for k, v in data.items() if k in categories_to_include}
        except json.JSONDecodeError:
            return {}

    df['json_dict'] = df[json_column].apply(process_json)
    df_expanded = pd.json_normalize(df['json_dict'])
    df = df.drop(columns=['json_dict'])

    def rename_columns(col):
        for prefix in categories_to_include:
            prefix_with_sep = prefix + '.'
            if col.startswith(prefix_with_sep):
                col = col[len(prefix_with_sep):]
                break
        col = col.lower().replace(' ', '_').replace('+', 'plus').replace('-', '_').replace(',', '').replace('__', '_')
        return col

    df_expanded.columns = [rename_columns(c) for c in df_expanded.columns]

    all_columns = [
        "identifies_as_latino_owned",
        "identifies_as_lgbtq_plus_owned",
        "identifies_as_women_owned",
        "identifies_as_black_owned",
        "identifies_as_veteran_owned",
        "identifies_as_asian_owned",
        "online_appointments",
        "onsite_services",
        "language_assistance",
        "online_estimates",
        "wheelchair_accessible_entrance",
        "wheelchair_accessible_parking_lot",
        "wheelchair_accessible_restroom",
        "wheelchair_accessible_seating",
        "lgbtq_plus_friendly",
        "transgender_safespace",
        "appointment_required",
        "gender_neutral_restroom"
    ]

    for col in all_columns:
        if col not in df_expanded.columns:
            if col == "language_assistance":
                df_expanded[col] = None
            else:
                df_expanded[col] = 0

    df_expanded = df_expanded[all_columns]

    boolean_columns = [
        "identifies_as_latino_owned",
        "identifies_as_lgbtq_plus_owned",
        "identifies_as_women_owned",
        "identifies_as_black_owned",
        "identifies_as_veteran_owned",
        "identifies_as_asian_owned",
        "online_appointments",
        "onsite_services",
        "online_estimates",
        "wheelchair_accessible_entrance",
        "wheelchair_accessible_parking_lot",
        "wheelchair_accessible_restroom",
        "wheelchair_accessible_seating",
        "lgbtq_plus_friendly",
        "transgender_safespace",
        "appointment_required",
        "gender_neutral_restroom"
    ]

    for col in boolean_columns:
        df_expanded[col] = df_expanded[col].map({True: 1, False: 0, np.nan:None})

    df_final = pd.concat([df, df_expanded], axis=1)

    return df_final