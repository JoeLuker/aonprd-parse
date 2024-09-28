# aonprd-parse

A comprehensive data processing pipeline for cleaning, parsing, deduplicating HTML data, and importing it into Memgraph.

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Logging](#logging)
- [Contributing](#contributing)
- [License](#license)

## Introduction

This project processes HTML data through various stages including manual cleaning, decomposing into graph structures, deduplication, and importing into Memgraph for advanced querying and analysis. It utilizes asynchronous programming for improved performance.

## Features

- **Manual Cleaning:** Applies specific string replacements to clean HTML files.
- **Decomposition:** Parses HTML into graph structures using BeautifulSoup and NetworkX.
- **Deduplication:** Identifies and removes duplicate HTML files based on hashing and similarity metrics.
- **CSV Preparation:** Converts graph data into CSV files suitable for Memgraph import.
- **Memgraph Integration:** Imports processed data into Memgraph with defined relationships.
- **Logging:** Comprehensive logging at each processing stage for easy debugging and monitoring.
- **Asynchronous Processing:** Utilizes asyncio for improved performance in I/O-bound operations.

## Project Structure

```text
aonprd-parse/
├── config/
│   ├── config.py
│   └── config.yaml
├── src/
│   ├── cleaning/
│   │   ├── __init__.py
│   │   ├── manual_cleaning.py
│   │   └── cleaner.py
│   ├── decomposing/
│   │   ├── __init__.py
│   │   ├── decomposer.py
│   │   └── condense_decomposition.py
│   ├── importing/
│   │   ├── __init__.py
│   │   ├── csv_prep.py
│   │   └── memgraph.py
│   ├── processing/
│   │   ├── __init__.py
│   │   └── unwrap.py
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── logging.py
│   │   ├── file_operations.py
│   │   └── data_handling.py
│   ├── __init__.py
│   └── process.py
├── tests/
│   ├── cleaning/
│   ├── decomposing/
│   ├── importing/
│   ├── processing/
│   └── utils/
├── data/
│   ├── raw_html_data/
│   ├── manual_cleaned_html_data/
│   ├── decomposed/
│   ├── condensed/
│   ├── processed/
│   └── import_files/
├── logs/
├── .gitignore
├── README.md
├── requirements.txt
└── pytest.ini
```

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/yourusername/aonprd-parse.git
   cd aonprd-parse
   ```

2. **Set Up Virtual Environment:**

   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the main processing script to execute the entire pipeline:

```bash
python src/process.py
```

**Note:** Ensure that all required directories and database files exist before running the scripts.

## Configuration

All configuration parameters are centralized in `config/config.py` and `config/config.yaml`. Adjust paths, logging configurations, and processing limits as needed.

## Logging

Logs are stored in the `logs/` directory. Each script generates its own log file for easy tracking. Log levels can be adjusted in the configuration files.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
