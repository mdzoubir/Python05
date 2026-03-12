from abc import ABC, abstractmethod
from typing import Any, List


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, list): #isinstance not allowed
            return False
        return all(isinstance(x, (int, float)) for x in data)  #isinstance not allowed

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Numeric data verification failed")
            count = len(data)
            total = sum(data)
            avg = total / count if count > 0 else 0.0
            return (f"Processed {count} numeric values, "
                    f"sum={total}, avg={avg:.1f}")
        except Exception as e:
            return f"Error: {str(e)}"

    def format_output(self, result: str) -> str:
        if result.startswith("Error"):
            return result
        return super().format_output(result)


class TextProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        return isinstance(data, str)

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Text data verification failed")
            chars = len(data)
            words = len(data.split())
            return f"Processed text: {chars} characters, {words} words"
        except Exception as e:
            return f"Error: {str(e)}"


class LogProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if not isinstance(data, str):
            return False
        return any(level in data for level in ["INFO", "ERROR", "ALERT"])

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Log entry verification failed")
            level = "UNKNOWN"
            for s in ["ALERT", "ERROR", "INFO"]:
                if s in data:
                    level = s
                    break
            msg = data.split(":", 1)[1].strip() if ":" in data else data
            return f"[{level}] {level} level detected: {msg}"
        except Exception as e:
            return f"Error: {str(e)}"


if __name__ == "__main__":
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    print("Initializing Numeric Processor...")
    num_proc = NumericProcessor()
    num_data = [1, 2, 3, 4, 5]
    print(f"Processing data: {num_data}")
    if num_proc.validate(num_data):
        print("Validation: Numeric data verified")
    print(num_proc.format_output(num_proc.process(num_data)))
    print()

    print("Initializing Text Processor...")
    txt_proc = TextProcessor()
    txt_data = "Hello Nexus World"
    print(f"Processing data: \"{txt_data}\"")
    if txt_proc.validate(txt_data):
        print("Validation: Text data verified")
    print(txt_proc.format_output(txt_proc.process(txt_data)))
    print()

    print("Initializing Log Processor...")
    log_proc = LogProcessor()
    log_data = "ERROR: Connection timeout"
    print(f"Processing data: \"{log_data}\"")
    if log_proc.validate(log_data):
        print("Validation: Log entry verified")
    print(log_proc.format_output(log_proc.process(log_data)))
    print()

    print("=== Polymorphic Processing Demo ===")
    print("Processing multiple data types through same interface...")

    processors: List[DataProcessor] = [
        NumericProcessor(),
        TextProcessor(),
        LogProcessor()
    ]

    test_data = [
        [1, 2, 3],
        "Hello Nexus",
        "INFO: System ready"
    ]

    for i, (proc, data) in enumerate(zip(processors, test_data), 1):
        print(f"Result {i}: {proc.process(data)}")

    print()
    print("Foundation systems online. Nexus ready for advanced streams.")
