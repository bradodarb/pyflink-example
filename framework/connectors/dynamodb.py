from pyflink.datastream.connectors import Sink
from pyflink.java_gateway import get_gateway


class DynamoDbSink(Sink):
    """
    A Dynamo DB (DDB) Sink that performs async requests against a destination table
    using the buffering protocol.
    """

    class Java:
        implements = ['java.io.Serializable']

    def __init__(self, **kwargs):
        java_src_class = get_gateway().jvm.com.example.pyflink.sink.ddb.DdbSink
        exe_props = java_src_class.DdbExecutionProperties.builder().build()
        Properties = get_gateway().jvm.java.util.Properties
        props = Properties()
        for k, v in kwargs.items():
            props.setProperty(k, str(v))
        java_src_obj = java_src_class.getJsonSink(props, exe_props)
        super(DynamoDbSink, self).__init__(java_src_obj)
