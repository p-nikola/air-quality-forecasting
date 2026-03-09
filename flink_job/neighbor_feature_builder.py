from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.common import Types

import json
import os


class NeighborFeatureBuilder(KeyedProcessFunction):

    FEATURES = {"pm10", "pm25", "wind_speed"}

    def open(self, runtime_context):


        latest_desc = MapStateDescriptor(
            "latest_sensor_values",
            Types.STRING(),              
            Types.PICKLED_BYTE_ARRAY()   
        )

        self.latest_values = runtime_context.get_map_state(latest_desc)

        base_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(base_dir, "../data/neighbors_data/neighbors.json")

        with open(path) as f:
            raw = json.load(f)
            self.neighbors = {str(k): [str(n) for n in v] for k, v in raw.items()}

        print("Neighbor keys:", list(self.neighbors.keys())[:5])

        print("NeighborFeatureBuilder initialized")


    def process_element(self, row, ctx):

        sensor_id = str(row["sensorId"])

        neighbors = self.neighbors.get(sensor_id, [])

        for i, neighbor in enumerate(neighbors, start=1):

            neighbor_values = self.latest_values.get(neighbor)

            if neighbor_values is None:
                neighbor_values = {}

            row[f"neighbor_{i}_pm10"] = neighbor_values.get("pm10")
            row[f"neighbor_{i}_pm25"] = neighbor_values.get("pm25")
            row[f"neighbor_{i}_wind_speed"] = neighbor_values.get("wind_speed")

        yield row

        current = self.latest_values.get(sensor_id)

        if current is None:
            current = {}

        for f in self.FEATURES:
            if row.get(f) is not None:
                current[f] = row[f]

        self.latest_values.put(sensor_id, current)