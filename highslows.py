# Initial imports
import pandas as pd
import dask.dataframe as dd

# Read all csv's and fix columns
dataset = dd.read_csv(r'\data\*.csv', include_path_column=True)
dataset.columns = ['Date', 'Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Pair']
dataset['Date'] = dd.to_datetime(dataset['Date'] + ' ' + dataset['Time'])
dataset = dataset.drop('Time', axis=1)

# Compute dask dataframes to pandas dataframes
dataset = dataset.compute()

# Clean pair names
dataset['Pair'] = dataset['Pair'].str.replace("c:/Users/eduardo/Documents/ForexEdge/data/","")
dataset['Pair'] = dataset['Pair'].str.replace('5.csv','')
dataset['Day'] = dataset['Date'].dt.date
dataset['Hour'] = dataset['Date'].dt.hour

# Get unique pair names
pairlist = dataset['Pair'].unique()

# Create empty dataframes to get results of break highs and lows
newdfhigh = pd.DataFrame()
newdflow = pd.DataFrame()

# Iterate over each pair
for pair in pairlist:
  dfprov = dataset[dataset['Pair'] == pair].copy()
  dfprov['PivotHigh'] = dfprov[(dfprov['High'] > dfprov['High'].shift(1)) & 
                        (dfprov['High'] > dfprov['High'].shift(2)) & 
                        (dfprov['High'] > dfprov['High'].shift(-1)) & 
                        (dfprov['High'] > dfprov['High'].shift(-2))]['High']
  dfprov['PivotLow'] = dfprov[(dfprov['Low'] < dfprov['Low'].shift(1)) & 
                        (dfprov['Low'] < dfprov['Low'].shift(2)) & 
                        (dfprov['Low'] < dfprov['Low'].shift(-1)) & 
                        (dfprov['Low'] < dfprov['Low'].shift(-2))]['Low']

  dfprov['AnyPivot'] = pd.notna(dfprov['PivotHigh']) | pd.notna(dfprov['PivotLow'])
  dfpivots = dfprov[dfprov['AnyPivot']]
  dfpivots = dfpivots.drop(['Open', 'High', 'Low', 'Close',	'Volume'], axis=1)

  days = dfpivots['Day'].unique()

  # Iterate over each day
  for day in days[1:-1]:

    dfdayhigh = dfpivots[(dfpivots['Day'] == day) & (dfpivots['PivotHigh'] > 0)].copy()
    dfdaylow = dfpivots[(dfpivots['Day'] == day) & (dfpivots['PivotLow'] > 0)].copy()

    dfdayhigh['CurrentHigh'] = dfdayhigh['PivotHigh'].cummax()
    dfdaylow['CurrentLow'] = dfdaylow['PivotLow'].cummin()

    dfdayhigh['PrevHigh'] = dfdayhigh['CurrentHigh'].shift(1)
    dfdaylow['PrevLow'] = dfdaylow['CurrentLow'].shift(1)

    dfdayhigh['BreakHigh'] = dfdayhigh.apply(lambda x : True if x['CurrentHigh'] > x['PrevHigh'] else False, axis=1)
    dfdaylow['BreakLow'] = dfdaylow.apply(lambda x : True if x['CurrentLow'] < x['PrevLow'] else False, axis=1)

    newdfhigh = newdfhigh.append(dfdayhigh)
    newdflow = newdflow.append(dfdaylow)

# Select criteria do be met and group by pair and day, while aggregating the sum of breaks per day
bhigh = newdfhigh[newdfhigh['Hour'] >= 15].groupby(['Pair', 'Day']).agg(breakshigh = ('BreakHigh', 'sum')).reset_index()
blow = newdflow[newdflow['Hour'] >= 15].groupby(['Pair', 'Day']).agg(breakslow = ('BreakLow', 'sum')).reset_index()

# Merge the results into one dataframe
merged = pd.merge(bhigh, blow, how='left', left_on=['Pair', 'Day'], right_on=['Pair', 'Day'])

# Import for SQL connection
import pyodbc
import sqlalchemy as sal
from sqlalchemy import create_engine

engine = sal.create_engine('mssql+pyodbc://localhost\SQLEXPRESS/data_counts?driver=SQL Server?Trusted_Connection=yes')

conn = engine.connect()

# Export dataframe to the SQL Server database
merged.to_sql('merged_data', con=engine, if_exists='append', index=False, chunksize=1000)