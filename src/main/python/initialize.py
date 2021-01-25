import sys
from read_streaming import read_stream
from util import create_session, parse_config
from transform import transform
from write_streaming import write_stream


def main():
    conf = parse_config()
    spark = create_session(conf['appName'], conf['innerPartitions'])
    stream_data = read_stream(spark, conf)
    transformed_stream = transform(stream_data)
    write_stream(transformed_stream,
                 conf['writeTopicName'],
                 conf['brokerAddress'] + ':' + conf['brokerPort'],
                 conf['checkpoint'],
                 conf['processingTime'])


if __name__ == '__main__':
    main()
