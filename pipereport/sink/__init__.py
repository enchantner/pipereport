from pipereport.sink.s3 import S3Sink


def get_sink(sink_name: str):
    sinks = {
        "s3": S3Sink
    }
    if sink_name not in sinks:
        raise Exception(f"No implementation found for sink '{sink_name}'!")
    return sinks[sink_name]
