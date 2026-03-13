from abc import ABC, abstractmethod
from typing import Any, List, Dict


class DataProcessor(ABC):
    @abstractmethod
    def process(self, data: Any) -> str:
        pass

    @abstractmethod
    def validate(self, data: Any) -> bool:
        pass

    def format_output(self, result: str) -> str:
        if result.startswith("Error"):
            return result
        return f"Output: {result}"


class NumericProcessor(DataProcessor):
    def validate(self, data: Any) -> bool:
        if isinstance(data, list):
            return all(isinstance(x, (int, float)) for x in data)
        return isinstance(data, (int, float))

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Numeric data verification failed")
            if isinstance(data, (int, float)):
                data = [data]
            count = len(data)
            total = sum(data)
            avg = total / count if count > 0 else 0.0
            return (f"Processed {count} numeric values, "
                    f"sum={total}, avg={avg:.1f}")
        except Exception as e:
            return f"Error: {str(e)}"


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
        parts = data.split(":", 1)
        if len(parts) != 2:
            return False
        return bool(parts[0].strip()) and bool(parts[1].strip())

    def process(self, data: Any) -> str:
        try:
            if not self.validate(data):
                raise ValueError("Log entry verification failed")
            header, msg = [part.strip() for part in data.split(":", 1)]
            level = "ALERT" if "error" in data.lower() else "INFO"
            return f"[{level}] {header} level detected: {msg}"
        except Exception as e:
            return f"Error: {str(e)}"


def main():
    print("=== CODE NEXUS - DATA PROCESSOR FOUNDATION ===")
    print()

    print("Initializing Numeric Processor...")
    num_proc = NumericProcessor()
    num_data: List[int] = [1, 2, 3, 4, 5]
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

    test_data: Dict[DataProcessor, Any] = {
        NumericProcessor(): [1, 2, 3],
        TextProcessor(): "Hello Nexus",
        LogProcessor(): "INFO: System ready"
    }

    for i, processor in enumerate(test_data, 1):
        data = test_data[processor]
        print(
            f"Result {i}: {processor.process(data)}"
        )

    print()
    print("Foundation systems online. Nexus ready for advanced streams.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nError : {e}")
