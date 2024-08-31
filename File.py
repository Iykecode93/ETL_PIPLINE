import pandas as pd
from sqlalchemy import create_engine

# Step 1: Extract Data  
df = pd.read_csv('./sale_data.csv')
print('')
print('******************************************')
print('**                ETL Sample            **')
print('******************************************')
print('')
print('Extracted Data:')
print(df)

# Step 2: Transform Data
df = df.dropna() # Clean Data
df['total_sales'] = df['quantity'] * df['price'] # Calculate total sales
print('')
print('Transformed Data')
print(df)

#Step 3: Load Data
engine = create_engine('sqlite:///sales.db')
df.to_sql('sales', con=engine, if_exists='replace', index=False)

# Verify data in the database
loaded_df = pd.read_sql('sales', con=engine)
print('')
print('Loaded Data')
print(loaded_df)
print('')