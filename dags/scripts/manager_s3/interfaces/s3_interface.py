from abc import ABC, abstractmethod
from typing import List

class S3Interface(ABC):
    @abstractmethod
    def list_files(self, directory: str) -> List[str]:
        pass

    @abstractmethod
    def move_file(self, source: str, destination: str) -> None:
        pass

    @abstractmethod
    def upload_file(self, file_path: str, bucket_path: str) -> None:
        pass

    @abstractmethod
    def file_exists(self, file_key: str) -> bool:
        pass
