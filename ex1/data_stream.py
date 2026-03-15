from abc import ABC, abstractmethod
from typing import Any, List, Dict, Union, Optional


class DataStream(ABC):
    def __init__(self, stream_id: str) -> None:
        self.stream_id: str = stream_id

    @abstractmethod
    def process_batch(self, data_batch: List[Any]) -> str:
        pass

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {"stream_id": self.stream_id}


class SensorStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            if not isinstance(data_batch, list):
                raise TypeError("Expected list")
            temps = []
            for d in data_batch:
                k, v = d.split(":")
                if not k or not v:
                    raise ValueError("Invalid data format")
                if "temp" in k:
                    temps.append(float(v))
            avg = sum(temps) / len(temps) if temps else 0.0
            return (f"Sensor analysis: {len(data_batch)} "
                    f"{"readings" if len(data_batch) > 1 else "reading"}"
                    f" processed, avg temp: {avg:.1f}°C")
        except Exception as e:
            return f"Error: {str(e)}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "priority":
            return [d for d in data_batch if "alert" in d.lower()]
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "sensor",
        }


class TransactionStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            if not isinstance(data_batch, list):
                raise TypeError("Expected list")
            flow = 0
            for item in data_batch:
                action, val = item.split(":")
                if not action or not val:
                    raise ValueError("Invalid data format")
                val = int(val)
                if action == "buy":
                    flow += val
                elif action == "sell":
                    flow -= val
            pref = "+" if flow >= 0 else ""
            return (f"Transaction analysis: {len(data_batch)} "
                    f"{"operations" if len(data_batch) > 1 else "operation"}"
                    f" processed, net flow: "
                    f"{pref}{flow} {"units" if abs(flow) != 1 else "unit"}")
        except Exception as e:
            return f"Error: {str(e)}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "priority":
            return [d for d in data_batch if float(d.split(":")[1]) > 100]
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "transaction",
        }


class EventStream(DataStream):
    def __init__(self, stream_id: str) -> None:
        super().__init__(stream_id)

    def process_batch(self, data_batch: List[Any]) -> str:
        try:
            if not isinstance(data_batch, list):
                raise TypeError("Expected list")
            errs = sum(
                1 for e in data_batch if "error" in e.lower()
            )
            return (f"Event analysis: {len(data_batch)} "
                    f"{"events" if len(data_batch) > 1 else "event"}"
                    f" processed, {errs} {"errors" if errs > 1 else "error"}"
                    f" detected")
        except Exception as e:
            return f"Error: {str(e)}"

    def filter_data(self, data_batch: List[Any],
                    criteria: Optional[str] = None) -> List[Any]:
        if criteria == "priority":
            return [d for d in data_batch if "error" in d.lower()]
        return data_batch

    def get_stats(self) -> Dict[str, Union[str, int, float]]:
        return {
            "stream_id": self.stream_id,
            "type": "event",
        }


class StreamProcessor:
    def __init__(self) -> None:
        self.streams: List[DataStream] = []

    def add_stream(self, stream: DataStream) -> None:
        self.streams.append(stream)

    def process(self, stream: DataStream,
                data_batch: List[Any]) -> str:
        if not isinstance(stream, DataStream):
            return "Error: Invalid stream type"
        return stream.process_batch(data_batch)

    def process_all(self, batches: List[List[Any]]) -> List[str]:
        results: List[str] = []

        for stream, batch in zip(self.streams, batches):
            result = stream.process_batch(batch)
            results.append(result)

        return results


def main() -> None:
    print("=== CODE NEXUS - POLYMORPHIC STREAM SYSTEM ===")
    print()

    print("Initializing Sensor Stream...")
    s_stream: SensorStream = SensorStream("SENSOR_001")
    s_batch: List[str] = ["temp:22.5", "humidity:65", "pressure:1013"]
    print(f"Stream ID: {s_stream.stream_id}, Type: Environmental Data")
    print(f"Processing sensor batch: [{', '.join(map(str, s_batch))}]")
    print(s_stream.process_batch(s_batch))
    print()

    print("Initializing Transaction Stream...")
    t_stream: TransactionStream = TransactionStream("TRANS_001")
    t_batch: List[str] = ["buy:100", "sell:150", "buy:75"]
    print(f"Stream ID: {t_stream.stream_id}, Type: Financial Data")
    print(f"Processing transaction batch: [{', '.join(map(str, t_batch))}]")
    print(t_stream.process_batch(t_batch))
    print()

    print("Initializing Event Stream...")
    e_stream: EventStream = EventStream("EVENT_001")
    e_batch: List[str] = ["login", "error", "logout"]
    print(f"Stream ID: {e_stream.stream_id}, Type: System Events")
    print(f"Processing event batch: [{', '.join(map(str, e_batch))}]")
    print(e_stream.process_batch(e_batch))
    print()

    print("=== Polymorphic Stream Processing ===")
    print("Processing mixed stream types through unified interface...")
    print()

    processor: StreamProcessor = StreamProcessor()

    processor.add_stream(s_stream)
    processor.add_stream(t_stream)
    processor.add_stream(e_stream)

    batches = [
        ["temp:20.0", "temp:25.0"],
        ["buy:50", "sell:200", "buy:10", "sell:20"],
        ["login", "action", "logout"]
    ]

    results = processor.process_all(batches)

    print("Batch 1 Results:")

    stream_info = [
        ("Sensor data", "reading"),
        ("Transaction data", "operation"),
        ("Event data", "event")
    ]

    for (data_type, unit), batch in zip(stream_info, batches):
        count = len(batch)
        unit_plural = unit + "s" if count > 1 else unit
        print(f"- {data_type}: {count} {unit_plural} processed")

    print()
    print("Stream filtering active: High-priority data only")

    s_c = s_stream.filter_data(
        ["alert temp:40.0", "alert temp:42", "temp:22.0"], "priority"
    )
    t_l = t_stream.filter_data(["buy:500", "buy:20"], "priority")

    print(f"Filtered results: {len(s_c)} critical sensor "
          f"{"alerts" if len(s_c) > 1 else "alert"}, "
          f"{len(t_l)} large {"transactions" if len(t_l) > 1 else "transaction"}"
          )

    print()
    print("All streams processed successfully. Nexus throughput optimal.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nError : {e}")
