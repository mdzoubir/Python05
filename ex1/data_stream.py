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
            return (f"Sensor analysis: {len(data_batch)} "
                    f"{"readings" if len(data_batch) > 1 else "reading"}"
                    f" processed, avg temp: {avg:.1F}°C")
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
            flow = 0
            for item in data_batch:
                action, val = item.split(":")
                val = int(val)
                if action == "buy":
                    flow += val
                elif action == "sell":
                    flow -= val
            pref = "+" if flow >= 0 else ""
            return (f"Transaction analysis: {len(data_batch)} "
                    f"{"operations" if len(data_batch) > 1 else "operation"}"
                    f" processed, net flow: "
                    f"{pref}{flow} {"units" if flow != 1 else "unit"}")
        except Exception as e:
            return f"Error: {str(e)}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "priority":
            return [d for d in data_batch if float(d.split(":")[1]) > 100]
        return data_batch


class EventStream(DataStream):
    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            errs = sum(1 for e in data_batch if "error" in e.lower())
            return (f"Event analysis: {len(data_batch)} "
                    f"{"events" if len(data_batch) > 1 else "event"}"
                    f" processed, {errs} {"errors" if errs > 1 else "error"}"
                    f" detected")
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
    print(f"Processing sensor batch: [{', '.join(map(str, s_batch))}]")
    print(s_stream.process_batch(s_batch))
    print()

    print("Initializing Transaction Stream...")
    t_stream = TransactionStream("TRANS_001")
    t_batch = ["buy:100", "sell:150", "buy:75"]
    print(f"Stream ID: {t_stream.stream_id}, Type: Financial Data")
    print(f"Processing transaction batch: [{', '.join(map(str, t_batch))}]")
    print(t_stream.process_batch(t_batch))
    print()

    print("Initializing Event Stream...")
    e_stream = EventStream("EVENT_001")
    e_batch = ["login", "error", "logout"]
    print(f"Stream ID: {e_stream.stream_id}, Type: System Events")
    print(f"Processing event batch: [{', '.join(map(str, e_batch))}]")
    print(e_stream.process_batch(e_batch))
    print()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print()

    processor = StreamProcessor()
    stream_batches = {
        "sensor": {
            "stream": s_stream,
            "batch": ["temp:20.0", "temp:25.0"],
            "type": "Sensor data",
            "unit": "reading"
        },
        "transaction": {
            "stream": t_stream,
            "batch": ["buy:50", "sell:200", "buy:10", "sell:20"],
            "type": "Transaction data",
            "unit": "operation"
        },
        "event": {
            "stream": e_stream,
            "batch": ["login", "action", "logout"],
            "type": "Event data",
            "unit": "event"
        }
    }

    print("Batch 1 Results:")

    for key, data in stream_batches.items():
        stream = data["stream"]
        batch = data["batch"]

        processor.process(stream, batch)

        count = len(batch)

        unit = data["unit"]
        if count > 1:
            unit += "s"

        print(f"- {data['type']}: {count} {unit} processed")

    print()
    print("Stream filtering active: High-priority data only")

    s_c = s_stream.filter_data(
        ["temp:40.0:alert", "temp:42:alert", "temp:22.0"], "priority"
    )
    t_l = t_stream.filter_data(["buy:500", "buy:20"], "priority")

    print(f"Filtered results: {len(s_c)} critical sensor "
          f"{"alerts" if len(s_c) > 1 else "alert"}, "
          f"{len(t_l)} large transaction")

    print()
    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nError : {e}")
# need to use super()
