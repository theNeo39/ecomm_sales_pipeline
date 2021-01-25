def read_stream(spark, conf_dict):
    stream_data = spark \
        .readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', conf_dict['brokerAddress'] + ':' + conf_dict['brokerPort']) \
        .option('subscribe', conf_dict['topicName']) \
        .option('startingOffsets', conf_dict['readingOffset']) \
        .load()
    return stream_data
