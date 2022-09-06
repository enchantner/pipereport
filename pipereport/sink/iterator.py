from pipereport.base.sink import BaseSink


class IteratorSink(BaseSink):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs) 

