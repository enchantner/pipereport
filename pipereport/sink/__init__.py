from pipereport.sink.s3 import S3Sink
from pipereport.sink.local import LocalSink


def get_sink(sink_name: str):
    sinks = {"s3": S3Sink, "local": LocalSink}
    if sink_name not in sinks:
        raise Exception(f"No implementation found for sink '{sink_name}'!")
    return sinks[sink_name]
