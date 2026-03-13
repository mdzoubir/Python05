from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str):
        self.stream_id = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"stream_id": self.stream_id}


class SensorStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            temps = []
            for d in data_batch:
                k, v = d.split(":")
                if "temp" in k:
                    temps.append(v)
            temps = [float(v) for v in temps]
            avg = sum(temps) / len(temps) if temps else 0.0
            return (f"Sensor analysis: {len(data_batch)} readings processed, "
                    f"avg temp: {avg:.1F}°C")
        except Exception as e:
            return f"Error: {str(e)}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "priority":
            return [d for d in data_batch if "alert" in d.lower()]
        return data_batch


class TransactionStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            flow = 0.0
            for item in data_batch:
                action, val = item.split(":")
                val = float(val)
                if action == "buy":
                    flow -= val
                elif action == "sell":
                    flow += val
            pref = "+" if flow >= 0 else ""
            return (f"Transaction analysis: {len(data_batch)} operations, "
                    f"net flow: {pref}{int(flow)} units")
        except Exception as e:
            return f"Error: {str(e)}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "priority":
            def get_val(item: str) -> float:
                return float(item.split(":")[1])
            return [d for d in data_batch if get_val(d) > 100]
        return data_batch


class EventStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            errs = sum(1 for e in data_batch if "error" in e.lower())
            return (f"Event analysis: {len(data_batch)} events, "
                    f"{errs} error detected")
        except Exception as e:
            return f"Error: {str(e)}"


class StreamProcessor:
    def process(self, stream: DataStream, data_batch: List[Any]) -> str:
        return stream.process_batch(data_batch)


def main():
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

    print("Initializing Sensor Stream...")
    s_stream = SensorStream("SENSOR_001")
    s_batch = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Stream ID: {s_stream.stream_id}, Type: Environmental Data")
    print(f"Processing sensor batch: [{', '.join(s_batch)}]")
    print(s_stream.process_batch(s_batch))
    print()

    print("Initializing Transaction Stream...")
    t_stream = TransactionStream("TRANS_001")
    t_batch = ["buy:100", "sell:150", "buy:75"]
    print(f"Stream ID: {t_stream.stream_id}, Type: Financial Data")
    print(f"Processing transaction batch: [{', '.join(t_batch)}]")
    print(t_stream.process_batch(t_batch))
    print()

    print("Initializing Event Stream...")
    e_stream = EventStream("EVENT_001")
    e_batch = ["login", "error", "logout"]
    print(f"Stream ID: {e_stream.stream_id}, Type: System Events")
    print(f"Processing event batch: [{', '.join(e_batch)}]")
    print(e_stream.process_batch(e_batch))
    print()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print()

    processor = StreamProcessor()
    streams = [s_stream, t_stream, e_stream]
    batches = [
        ["temp:20.0", "temp:25.0"],
        ["buy:50", "sell:200", "buy:10", "sell:20"],
        ["login", "action", "logout"]
    ]

    print("Batch 1 Results:")
    types = ["Sensor data", "Transaction data", "Event data"]
    for i, (strm, batch) in enumerate(zip(streams, batches)):
        res = processor.process(strm, batch)
        count = len(batch)
        units = ("readings" if i == 0 else "operations" if i == 1
                 else "events")
        print(f"- {types[i]}: {count} {units} processed")

    print()
    print("Stream filtering active: High-priority data only")
    s_c = s_stream.filter_data(["temp:40.0:alert", "temp:22.0"], "priority")
    t_l = t_stream.filter_data(["buy:500", "buy:20"], "priority")
    print(f"Filtered results: {len(s_c)} critical sensor alerts, "
          f"{len(t_l)} large transaction")

    print()
    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nError : {e}")
