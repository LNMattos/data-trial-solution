# Reader Writer S3 Framework

A package for reading, writing, and upserting data to Amazon S3-compatible storage using different backend engines. This framework provides a standardized interface for data operations, enabling seamless integration with popular data processing libraries like Pandas and PySpark.

## Table of Contents

- [Directory Structure](#directory-structure)
- [Core Components](#core-components)
  - [Interfaces](#interfaces)
  - [Factory](#factory)
  - [Backends](#backends)
  - [Client](#client)
  - [Core](#core)
  - [Utils](#utils)
- [Usage Overview](#usage-overview)
- [Extending the Framework](#extending-the-framework)
- [License](#license)

## Directory Structure

### Description

- **__pycache__/**: Contains Python bytecode files for optimized performance.
- **backends/**: Implements specific data processing backends (e.g., Pandas, PySpark).
  - **pandas.py**: Pandas-specific implementations for reading, writing, and upserting data.
  - **pyspark.py**: PySpark-specific implementations for reading, writing, and upserting data.
- **client/**: Manages interactions with external services.
  - **s3.py**: Handles S3 client configurations and operations.
- **core/**: Houses the high-level classes that provide the main functionalities.
  - **data_reader_writer.py**: Facilitates reading and writing data to S3.
  - **upsert.py**: Manages upsert operations on data stored in S3.
- **factory/**: Contains factory classes responsible for creating instances of interfaces and upserters.
  - **reader_writer_factory.py**: Factory for creating data reader and writer instances.
  - **upsert_factory.py**: Factory for creating upserter instances based on backend and method.
- **interfaces/**: Defines the abstract base classes that specify the required methods for data operations.
  - **data_reader.py**: Abstract class for data reading operations.
  - **data_writer.py**: Abstract class for data writing operations.
  - **upserter.py**: Abstract class for upsert operations.

## Core Components

### Interfaces

The `interfaces` module defines abstract base classes that outline the essential methods any data reader, writer, or upserter must implement. This ensures consistency and interoperability across different backend implementations.

**Key Interfaces:**

- **DataReader**: Defines the `read` method for reading data from a specified path.
- **DataWriter**: Defines the `write` method for writing data to a specified path.
- **Upserter**: Defines the `upsert` method for merging new data with existing data based on specified strategies.

### Factory

The `factory` module contains factory classes responsible for instantiating the appropriate backend implementations based on user input.

**Key Factories:**

- **DataReaderWriterFactory**: Creates instances of `DataReader` and `DataWriter` based on the chosen backend (e.g., Pandas, PySpark).
- **UpsertFactory**: Creates instances of `Upserter` based on the chosen backend and upsert method.

### Backends

The `backends` module includes concrete implementations of the interfaces for different data processing libraries. Each backend encapsulates the specifics of interacting with its respective library.

**Example Backends:**

- **Pandas Backend (`pandas.py`)**:
  - **PandasDataReader**: Implements data reading using Pandas.
  - **PandasDataWriter**: Implements data writing using Pandas.
  - **PandasUpserter**: Implements upsert operations using Pandas.
  
- **PySpark Backend (`pyspark.py`)**:
  - **PySparkDataReader**: Implements data reading using PySpark.
  - **PySparkDataWriter**: Implements data writing using PySpark.
  - **PySparkUpserter**: Implements upsert operations using PySpark.

### Client

The `client` module manages interactions with external services, such as Amazon S3.

**Key Component:**

- **S3Client**: Handles configuration and operations related to S3, including connecting to the S3 service, uploading, downloading, and managing data.

### Core

The `core` module contains high-level classes that provide the main functionalities of the framework, abstracting the complexities of backend interactions.

**Key Classes:**

- **DataReaderWriter**: Provides methods to read from and write to S3 using the specified backend.
  
  **Key Methods:**
  
  - `read(path: str, **kwargs)`: Reads data from the specified S3 path.
  - `write(data: Union[pd.DataFrame, SparkDataFrame], path: str, **kwargs)`: Writes data to the specified S3 path.

- **Upsert**: Manages upsert operations on data stored in S3.
  
  **Key Methods:**
  
  - `upsert(new_data: Union[pd.DataFrame, SparkDataFrame], target_path: str, upsert_method: str, id_columns: list, modification_column: str = None)`: Merges new data with existing data in S3 based on the specified upsert method and key columns.

### Utils

The `utils` module provides utility functions and helpers that support the core functionality of the framework. These may include data validation, logging, configuration management, and other auxiliary tasks.

## Usage Overview

To utilize the Reader Writer S3 Framework, follow these steps:

### 1. Initialize the Data Reader/Writer

```python
from core.data_reader_writer import DataReaderWriter

# Choose the desired backend ('pandas' or 'pyspark')
reader_writer = DataReaderWriter(
    backend='pandas',
    bucket_name='your-bucket-name',
    endpoint_url='https://s3.amazonaws.com',
    access_key='your-access-key',
    secret_key='your-secret-key'
)
