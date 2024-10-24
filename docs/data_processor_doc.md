# Data Processor Framework

A flexible and extensible data processing package designed to handle various data manipulation tasks using different backend engines. This framework provides a standardized interface for data operations, allowing seamless integration with popular data processing libraries like Pandas and PySpark.

## Table of Contents

- [Directory Structure](#directory-structure)
- [Core Components](#core-components)
  - [Interfaces](#interfaces)
  - [Factory](#factory)
  - [Backends](#backends)
  - [Utils](#utils)
- [Usage Overview](#usage-overview)
- [Extending the Framework](#extending-the-framework)
- [License](#license)

## Directory Structure
### Description
- **backends/**: Implements specific data processing backends (e.g., Pandas, PySpark).
- **core/**: Houses the high-level `DataProcessor` class that users interact with.
- **factory/**: Contains the `DataProcessorFactory` responsible for creating backend instances.
- **interfaces/**: Defines the abstract base classes that specify the required methods for data processors.
- **utils/**: Utility modules and helper functions.
- **__init__.py**: Makes the directory a Python package.

## Core Components

### Interfaces

The `interfaces` module defines abstract base classes that outline the essential methods any data processor must implement. This ensures consistency and interoperability across different backend implementations.

**Key Interface:**

- **DataProcessorInterface**: An abstract class that declares methods for data loading, column manipulation, row filtering, type conversion, and more.

### Factory

The `factory` module contains the `DataProcessorFactory`, a factory class responsible for instantiating the appropriate data processor backend based on user input.

**Key Component:**

- **DataProcessorFactory**: Provides a static method `create_processor` which takes a backend name (e.g., 'pandas', 'pyspark') and returns an instance of the corresponding data processor that implements the `DataProcessorInterface`.

### Backends

The `backends` module includes concrete implementations of the `DataProcessorInterface` for different data processing libraries. Each backend encapsulates the specifics of interacting with its respective library.

**Example Backends:**

- **PandasDataProcessor**: Implements data processing methods using the Pandas library.
- **SparkDataProcessor**: Implements data processing methods using PySpark.

### Core

The `core` module contains the `DataProcessor` class, a high-level interface that users interact with. It delegates data processing tasks to the appropriate backend processor created by the factory.

**Key Class:**

- **DataProcessor**: Initializes with a specified backend and provides methods to perform data operations such as loading data, removing columns, filtering rows, converting data types, creating new columns, applying functions, converting columns to datetime, dropping null columns, and retrieving the processed data.

### Utils

The `utils` module provides utility functions and helpers that support the core functionality of the framework. These may include data validation, logging, configuration management, and other auxiliary tasks.

## Usage Overview

To utilize the Data Processor Framework, follow these steps:

1. **Initialize the Data Processor:**

   ```python
   from core.data_processor import DataProcessor

   # Choose the desired backend ('pandas' or 'pyspark')
   processor = DataProcessor(backend='pandas')

