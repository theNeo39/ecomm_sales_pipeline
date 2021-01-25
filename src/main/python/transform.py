from pyspark.sql.functions import from_json, dayofmonth, to_utc_timestamp, to_timestamp, regexp_replace
from pyspark.sql.functions import window
from util import read_schema


def transform(stream_df):
    schema = read_schema()
    stream_df = stream_df.selectExpr('cast(value as string)')
    stream_df = stream_df.select(from_json(stream_df['value'], schema).alias('values')).selectExpr('values.*')
    stream_df = stream_df.withColumn('price', stream_df['price'].cast('double'))
    stream_df=stream_df.withColumn('event_time',to_timestamp(stream_df['event_time']))
    stream_df=stream_df.filter('event_type="purchase"')
    query = stream_df.withWatermark('event_time','45 seconds')\
        .dropDuplicates(['event_time','user_id','product_id'])\
        .groupBy(window('event_time','45 seconds'),'product_id')\
        .sum('price')
    query=query.select('window','product_id',query['sum(price)'].cast('string').alias('total'))
    query.coalesce(1)
    return query
