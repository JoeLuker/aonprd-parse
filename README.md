# aonprd-parse
Parsing aonprd 1e data into a memgraph, and perhaps more
=======
# Explore AONPRD

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

This project processes HTML data through various stages including manual cleaning, decomposing into graph structures, deduplication, and importing into Memgraph for advanced querying and analysis.

## Features

- **Manual Cleaning:** Applies specific string replacements to clean HTML files.
- **Decomposition:** Parses HTML into graph structures using BeautifulSoup and NetworkX.
- **Deduplication:** Identifies and removes duplicate HTML files based on hashing and similarity metrics.
- **CSV Preparation:** Converts graph data into CSV files suitable for Memgraph import.
- **Memgraph Integration:** Imports processed data into Memgraph with defined relationships.
- **Logging:** Comprehensive logging at each processing stage for easy debugging and monitoring.

## Project Structure

```
explore-aonprd/
├── config/
│   └── config.py
├── data/
│   ├── raw_html_data/
│   ├── consolidated/
│   │   └── import_files/
│   └── ...
├── scripts/
│   ├── csv_prep.py
│   ├── cleaner.py
│   ├── condense_decomposition.py
│   ├── decomposer.py
│   ├── memgraph.py
│   ├── manual_cleaning.py
│   ├── process.py
│   ├── unwrap.py
│   └── ...
├── utils/
│   └── utils.py
├── logs/
│   └── ... (Log files will be here)
├── tests/
│   └── ... (Unit and integration tests)
├── .gitignore
├── README.md
└── requirements.txt
```

## Installation

1. **Clone the Repository:**

   ```bash
   git clone https://github.com/yourusername/explore-aonprd.git
   cd explore-aonprd
   ```

2. **Set Up Virtual Environment:**

   It's recommended to use a virtual environment to manage dependencies.

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
python scripts/process.py
```

**Note:** Ensure that all required directories and database files exist before running the scripts.

## Configuration

All configuration parameters are centralized in `config/config.py`. Adjust paths, logging configurations, and processing limits as needed.

## Logging

Logs are stored in the `logs/` directory. Each script generates its own log file for easy tracking:

- `html_processor.log`
- `memgraph_importer.log`
- `unwrap_matching_nodes.log`
- `yaml_csv_prep.log`

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
