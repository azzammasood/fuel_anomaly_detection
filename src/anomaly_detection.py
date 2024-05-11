import pandas as pd
from sklearn.ensemble import IsolationForest
import logging
from datetime import timedelta

# Configure logging
logging.basicConfig(level=logging.DEBUG)

def calculate_litre_changes(df, theft_threshold, refill_threshold):
    if 'cumulative_change' not in df.columns:
        logging.error("cumulative_change column is missing in calculate_litre_changes")
    else:
        logging.debug(f"Calculating litre changes with cumulative_change:\n{df[['cumulative_change']].head()}")
    df['theft_litre'] = df['cumulative_change'].apply(lambda x: abs(x) if x < 0 and abs(x) > theft_threshold else 0)
    df['refill_litre'] = df['cumulative_change'].apply(lambda x: x if x > 0 and x > refill_threshold else 0)
    return df

def classify_displaypoint(row, refill_threshold, theft_threshold):
    if row['sensor_failure']:
        return 'sensor_failure'
    elif row['cumulative_change'] > refill_threshold:
        return 'refill'
    elif row['cumulative_change'] < -theft_threshold:
        if row['powerstate'].lower() in ['DG', 'DG-batt', 'solar-DG', 'solar-dg-mains'] and abs(row['cumulative_change']) > theft_threshold:
            return 'pilferage'
        else:
            return 'normal'
    else:
        return 'normal'

def check_generator_activity(power_states):
    generator_states = ['DG', 'DG-batt', 'solar-DG', 'solar-dg-mains']
    return any(state.lower() in generator_states for state in power_states)

def adjust_timestamps(df, columns, hours):
    for column in columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = df[column] - timedelta(hours=hours)
        else:
            df[column] = pd.to_datetime(df[column]) - timedelta(hours=hours)
    return df

async def process_new_data(new_data, refill_threshold, theft_threshold):
    logging.info("Processing new data")
    
    fuel_data = new_data[['siteid', 'updatetime', 'gateway', 'hwcode', 'powerstate', 'fuellevel1', 'fuellevel2', 'fuellevel3']].copy()
    logging.debug(f"Filtered data: {fuel_data.head()}")

    fuel_data['updatetime'] = pd.to_datetime(fuel_data['updatetime'], unit='s')
    
    parameters_to_smooth = ['fuellevel1', 'fuellevel2', 'fuellevel3']
    
    window_size = 40  # Adjust this value as needed
    for param in parameters_to_smooth:
        # First apply rolling median
        fuel_data[f'smoothed_{param}'] = fuel_data.groupby(['siteid'])[param].transform(lambda x: x.rolling(window=window_size, center=True, min_periods=1).median())
        
        # Then apply interpolation
        fuel_data[f'smoothed_{param}'] = fuel_data.groupby(['siteid'])[f'smoothed_{param}'].transform(lambda x: x.interpolate(method='linear').fillna(method='ffill').fillna(method='bfill'))
    
        # Finally apply rolling median again
        fuel_data[f'smoothed_{param}'] = fuel_data.groupby(['siteid'])[f'smoothed_{param}'].transform(lambda x: x.rolling(window=window_size, center=True, min_periods=1).median())

        # Fill any remaining NaN values with the original data
        fuel_data[f'smoothed_{param}'].fillna(fuel_data[param], inplace=True)
        
    fuel_data['gentotalfuellevel'] = fuel_data[['smoothed_fuellevel1', 'smoothed_fuellevel2', 'smoothed_fuellevel3']].sum(axis=1)
    logging.debug(f"Total smoothed fuel levels calculated: {fuel_data['gentotalfuellevel'].head()}")

    # Calculate the difference in fuel level between consecutive timestamps
    fuel_data['fuel_diff'] = fuel_data.groupby('siteid')['gentotalfuellevel'].diff().fillna(0)
    
    # Calculate cumulative change using the current and previous 3 differences
    for i in range(1, 4):
        fuel_data[f'fuel_diff_lag_{i}'] = fuel_data.groupby('siteid')['fuel_diff'].shift(i).fillna(0)
    fuel_data['cumulative_change'] = fuel_data[['fuel_diff_lag_1', 'fuel_diff_lag_2', 'fuel_diff_lag_3']].sum(axis=1)
    logging.debug(f"Cumulative changes calculated: {fuel_data['cumulative_change'].head()}")

    # Anomaly detection using Isolation Forest
    X = fuel_data[['cumulative_change']]
    model = IsolationForest(n_estimators=100, contamination='auto', random_state=42)
    model.fit(X)
    fuel_data['anomaly'] = model.predict(X)
    fuel_data['anomaly'] = fuel_data['anomaly'] == -1
    logging.debug(f"Anomalies detected: {fuel_data['anomaly'].sum()}")

    fuel_data['sensor_failure'] = (fuel_data['gentotalfuellevel'] < 0) | (fuel_data['gentotalfuellevel'] > 3000)

    fuel_data['displaypoint'] = fuel_data.apply(classify_displaypoint, axis=1, args=(refill_threshold, theft_threshold))
    logging.debug(f"Fuel data with displaypoint classification: {fuel_data[['siteid', 'updatetime', 'fuel_diff', 'cumulative_change', 'displaypoint']].head()}")

    logging.info(f"Display points after classification: {fuel_data['displaypoint'].unique()}")

    fuel_data['severity'] = fuel_data['displaypoint'].apply(lambda x: 'major' if x in ['sensor_failure', 'refill', 'pilferage'] else 'normal')
    logging.debug(f"Severity assigned: {fuel_data['severity'].value_counts()}")

    # DataFrame 1: Basic information with smoothed fuel levels
    df1 = fuel_data[['siteid', 'updatetime', 'powerstate', 'gateway', 'hwcode',
                     'smoothed_fuellevel1', 'smoothed_fuellevel2', 'smoothed_fuellevel3',
                     'gentotalfuellevel']].copy()
    df1['time'] = pd.Timestamp.now().floor('s').timestamp()
    df1 = df1.drop_duplicates(subset=['siteid'], keep='last')
    logging.debug(f"DataFrame 1: {df1.head()}")

    # DataFrame 2: Display point and anomaly information (including sensor failures)
    displaypoint_data = fuel_data[fuel_data['displaypoint'].isin(['refill', 'pilferage', 'sensor_failure'])].copy()
    displaypoint_data['opentime'] = displaypoint_data['updatetime']
    displaypoint_data['closetime'] = None  # Initially no closetime
    displaypoint_data['start_fuellevel'] = displaypoint_data.groupby(['siteid', 'displaypoint'])['gentotalfuellevel'].transform('first')
    displaypoint_data['end_fuellevel'] = None  # Initially no end_fuellevel
    displaypoint_data['time'] = pd.Timestamp.now().floor('s').timestamp()

    displaypoint_data['type'] = 'alert'
    displaypoint_data['protocol'] = 'mqtt'

    df2 = displaypoint_data[['siteid', 'gateway', 'hwcode', 'displaypoint', 'opentime', 'closetime', 'start_fuellevel', 'end_fuellevel', 'severity', 'time', 'type', 'protocol']].copy()
    logging.debug(f"DataFrame 2: {df2.head()}")

    # DataFrame 3: Current display points
    current_displaypoints = fuel_data[fuel_data['displaypoint'].isin(['refill', 'pilferage', 'sensor_failure'])][['siteid', 'gateway', 'hwcode', 'displaypoint', 'updatetime', 'severity']].copy()
    current_displaypoints['time'] = pd.Timestamp.now().floor('s').timestamp()
    current_displaypoints['updatetime'] = current_displaypoints['updatetime'].astype('datetime64[ns]')

    current_displaypoints['type'] = 'alert'
    current_displaypoints['protocol'] = 'mqtt'

    df3 = current_displaypoints[['siteid', 'gateway', 'hwcode', 'displaypoint', 'updatetime', 'severity', 'time', 'type', 'protocol']].copy()
    logging.debug(f"DataFrame 3: {df3.head()}")

    # DataFrame 4: Daily fuel consumption statistics
    if 'cumulative_change' not in fuel_data.columns:
        logging.error("cumulative_change column is missing before daily aggregation")
    fuel_data['day'] = fuel_data['updatetime'].dt.floor('D')
    daily_data = fuel_data.groupby(['siteid', 'day']).agg({
        'gentotalfuellevel': ['first', 'last'],
        'fuel_diff': 'sum',
        'cumulative_change': 'sum',
        'powerstate': lambda x: list(x)
    }).reset_index()
    daily_data.columns = ['siteid', 'day', 'day_start_fuellevel', 'day_end_fuellevel', 'fuel_diff', 'cumulative_change', 'power_states']

    daily_data = calculate_litre_changes(daily_data, theft_threshold, refill_threshold)
    daily_data['generator_activity'] = daily_data['power_states'].apply(check_generator_activity)
    daily_data['consumption_litre'] = daily_data.apply(
        lambda row: (row['day_start_fuellevel'] + row['theft_litre'] - row['refill_litre']) - row['day_end_fuellevel']
        if row['generator_activity'] or row['theft_litre'] > 0 else 0, axis=1)
    daily_data['consumption_litre'] = daily_data.apply(
        lambda row: 0 if (row['day_start_fuellevel'] - row['day_end_fuellevel'] < 0 and row['refill_litre'] == 0) else row['consumption_litre'],
        axis=1)
    daily_data['updatetime'] = daily_data['day'] + timedelta(days=1)
    daily_data = adjust_timestamps(daily_data, ['updatetime', 'day'], 8)

    df4 = daily_data[['siteid', 'consumption_litre', 'refill_litre', 'theft_litre', 'day', 'day_start_fuellevel', 'day_end_fuellevel', 'updatetime']].copy()
    logging.debug(f"DataFrame 4: {df4.head()}")

    return df1, df2, df3, df4
