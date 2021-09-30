"""
Script to get the POS that are recurrently out of stock
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, when, lag, sum as ps_sum, unix_timestamp, countDistinct
from pyspark.sql.window import Window

# Global variables
STOCK_LEVEL_FILENAME = os.environ['STOCK_LEVEL_FILENAME']
TRANSACTIONS_FILENAME = os.environ['TRANSACTIONS_FILENAME']
OUT_OF_STOCK_FACTOR = float(os.environ['OUT_OF_STOCK_FACTOR'])
OUT_OF_STOCK_MIN_HOURS = float(os.environ['OUT_OF_STOCK_MIN_HOURS'])
RECURRENT_OUT_OF_STOCK_MIN_TIMES = float(os.environ['RECURRENT_OUT_OF_STOCK_MIN_TIMES'])

spark = SparkSession \
    .builder \
    .getOrCreate()

# Read & clean data
df_stock_level = spark.read.csv(f'data/{STOCK_LEVEL_FILENAME}', header=True, inferSchema=True)
df_stock_level = df_stock_level.drop('terminal_id')
df_stock_level = df_stock_level.withColumnRenamed('date', 'timestamp')

df_transactions = spark.read.csv(f'data/{TRANSACTIONS_FILENAME}', header=True, inferSchema=True)
df_transactions = df_transactions.drop('terminal_id')
df_transactions = df_transactions.withColumnRenamed('date', 'timestamp').withColumn('date', to_date('timestamp'))

# Calculate the out of stock value for each POS
df_pos_daily_avg = \
    df_transactions.groupBy('pos_id', 'date').sum('transaction_amount').groupBy('pos_id').avg('sum(transaction_amount)')
df_pos_out_of_stock_data = \
    df_pos_daily_avg.withColumn('out_of_stock_value', col('avg(sum(transaction_amount))') * OUT_OF_STOCK_FACTOR)
df_pos_out_of_stock_data = df_pos_out_of_stock_data.drop('avg(sum(transaction_amount))')

# Union both sources
df_transactions = df_transactions.drop('transaction_amount').drop('date')
df_union = df_stock_level.union(df_transactions)

# Bring the out_of_stock_value to every row
df_union = df_union.join(df_pos_out_of_stock_data, on='pos_id')

# Add the flag has_stock to each row
df_has_stock_flag = df_union.select('pos_id', 'timestamp', 'stock_balance', 'out_of_stock_value',
                                    when(col('stock_balance') >= col('out_of_stock_value'), True).
                                    otherwise(False).alias('has_stock'))

# Add the flag state_change, that identifies when there has been a change in the has_stock flag
window_def = Window.partitionBy('pos_id').orderBy('timestamp')
df_state_change = df_has_stock_flag.select('*',
                                           when((col('has_stock') != lag('has_stock').over(window_def)) |
                                                (lag('has_stock').over(window_def).isNull()), 1).
                                           otherwise(0).alias('state_change'))

# Add the counter state_sequence, to help identify the entries that belong to the same sequence of a state
df_state_sequence = df_state_change.select('pos_id', 'timestamp', 'has_stock',
                                           ps_sum('state_change').over(window_def).alias('state_sequence'))

# Add the difference of hours between entries
window_def_state_sequence = Window.partitionBy('pos_id', 'state_sequence').orderBy('timestamp')
df_hours_difference = \
    df_state_sequence.select('*', (unix_timestamp(col('timestamp')) -
                                   unix_timestamp(lag('timestamp').
                                                  over(window_def_state_sequence)) / 3600).alias('hours_difference'))

# Add the accumulation of hours in each sequence
df_accumulated_hours = df_hours_difference.select('pos_id', 'has_stock', 'state_sequence',
                                                  ps_sum('hours_difference').
                                                  over(window_def_state_sequence).alias('accumulated_hours'))

# Find out how many times each POS was out of stock
df_recurrent = df_accumulated_hours.filter((col('accumulated_hours') > OUT_OF_STOCK_MIN_HOURS) &
                                           ~(col('has_stock'))). \
    groupBy('pos_id').agg(countDistinct('state_sequence').alias('times_recurrent_oos'))

# Identify which POS are recurrently out of stock
df_recurrent_flag = df_union.select('pos_id').distinct().join(df_recurrent, 'pos_id', 'left'). \
    select('pos_id', when(col('times_recurrent_oos') >= RECURRENT_OUT_OF_STOCK_MIN_TIMES, True).otherwise(False).
           alias('is_recurrently_oos'))

df_recurrent_flag.show()
