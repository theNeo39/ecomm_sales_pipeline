from pyspark.sql.types import StructField,StructType,StringType,TimestampType,DoubleType

JSON_SCHEMA= StructType([
        StructField('event_time',StringType()),
        StructField('event_type',StringType()),
        StructField('product_id',StringType()),
        StructField('category_id',StringType()),
        StructField('category_code',StringType()),
        StructField('brand',StringType()),
        StructField('price',StringType()),
        StructField('user_id',StringType()),
        StructField('user_session',StringType())
    ])
