from pyspark.sql.functions import to_json, struct, concat


def write_stream(data, topic_name, server_add, checkpoint, process_time):
    df = data.select(to_json(struct('product_id', 'window')).alias('key'),
                     to_json(struct('product_id', 'total', 'window.start', 'window.end')).alias('value'))
    df.selectExpr('cast(key as string)', 'cast(value as string)') \
        .writeStream \
        .format('kafka') \
        .outputMode('update') \
        .option('topic', topic_name) \
        .option('kafka.bootstrap.servers', server_add) \
        .option('checkpointLocation', checkpoint) \
        .trigger(processingTime=process_time) \
        .start() \
        .awaitTermination()
