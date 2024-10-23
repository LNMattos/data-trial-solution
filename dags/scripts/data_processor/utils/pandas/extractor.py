import pandas as pd
import ast

def add_working_hours_columns(df, col):
    def extract_working_hours(row):
        if not pd.isnull(row):
            working_hours = ast.literal_eval(row)
            # Extract hours for Saturday, Sunday, and Monday (used for weekdays)
            saturday_hours = working_hours.get('Saturday', 'Closed')
            sunday_hours = working_hours.get('Sunday', 'Closed')
            weekdays_hours = working_hours.get('Monday', 'Closed') 
            return pd.Series([saturday_hours, sunday_hours, weekdays_hours])
        return pd.Series([None, None, None])

    df[['saturday_working_hours', 'sunday_working_hours', 'weekdays_working_hours']] = df[col].apply(extract_working_hours)

    return df