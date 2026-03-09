from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.common import Types


class SensorRowAssembler(KeyedProcessFunction):

    REQUIRED_TYPES = {
        "pm10",
        "pm25",
        "temperature",
        "humidity",
        "pressure",
        "wind_speed",
        "no2",
        "co",
        "o3"
    }

    TIMEOUT_MS = 10000

    def open(self, runtime_context):

        print("SensorRowAssembler state initialized")

        rows_desc = MapStateDescriptor(
            "rows",
            Types.STRING(),             
            Types.PICKLED_BYTE_ARRAY()   
        )

        timers_desc = MapStateDescriptor(
            "timers",
            Types.LONG(),      
            Types.STRING()     
        )

        self.rows = runtime_context.get_map_state(rows_desc)
        self.timers = runtime_context.get_map_state(timers_desc)


    def process_element(self, value, ctx):

        sensor_id = str(value["sensorId"])
        timestamp = value["timestamp"]
        sensor_type = value["type"]
        sensor_value = float(value["value"])

        row = self.rows.get(timestamp)

        if row is None:

            row = {
                "sensorId": sensor_id,
                "city": value["city"],
                "timestamp": timestamp
            }

            timer = (
                ctx.timer_service().current_processing_time()
                + self.TIMEOUT_MS
            )

            ctx.timer_service().register_processing_time_timer(timer)

            self.timers.put(timer, timestamp)

        row[sensor_type] = sensor_value

        self.rows.put(timestamp, row)

        received = set(row.keys()) - {"sensorId", "timestamp", "city"}

        if self.REQUIRED_TYPES.issubset(received):

            yield self.build_row(row)

            for timer in self.timers.keys():

                if self.timers.get(timer) == timestamp:
                    self.cleanup(timer, timestamp, ctx)
                    break


    def on_timer(self, timer, ctx):

        timestamp = self.timers.get(timer)

        if timestamp:

            row = self.rows.get(timestamp)

            if row:
                yield self.build_row(row)

            self.cleanup(timer, timestamp, ctx)


    def build_row(self, row):

        result = dict(row)

        for sensor in self.REQUIRED_TYPES:
            if sensor not in result:
                result[sensor] = None

        return result


    def cleanup(self, timer, timestamp, ctx):

        ctx.timer_service().delete_processing_time_timer(timer)

        self.rows.remove(timestamp)
        self.timers.remove(timer)