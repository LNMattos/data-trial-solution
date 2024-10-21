from scripts.reader_writer_s3.interfaces.upserter import Upserter
from scripts.reader_writer_s3.backends.pandas import PandasUpserter
from scripts.reader_writer_s3.backends.pyspark import PySparkUpserter

class UpsertFactory:
    @staticmethod
    def get_upserter(backend: str, upsert_method: str, id_columns: list, modification_column: str = None) -> Upserter:
        backend = backend.lower()
        upsert_method = upsert_method.lower()
        
        if backend == 'pandas':
            return PandasUpserter(strategy=upsert_method, id_columns=id_columns, modification_column=modification_column)
        elif backend == 'pyspark':
            return PySparkUpserter(strategy=upsert_method, id_columns=id_columns, modification_column=modification_column)
        else:
            raise ValueError(f"Backend '{backend}' não suportado.")