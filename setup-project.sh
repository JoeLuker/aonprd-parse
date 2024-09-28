#!/bin/bash

# setup_project.sh
# Bash script to set up the project structure, initialize Git and Git LFS,
# create essential files, and push to GitHub.

# Exit immediately if a command exits with a non-zero status.
set -e

# ------------------------------
# Function Definitions
# ------------------------------

# Function to check if a command exists
command_exists () {
    command -v "$1" >/dev/null 2>&1 ;
}

# Function to install Git LFS (for macOS using Homebrew)
install_git_lfs_mac () {
    echo "Installing Git LFS using Homebrew..."
    brew install git-lfs
}

# Function to install Git LFS (for Debian/Ubuntu)
install_git_lfs_linux () {
    echo "Installing Git LFS on Debian/Ubuntu..."
    curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | sudo bash
    sudo apt-get install git-lfs
}

# Function to install Git LFS (for Windows, prompt user)
install_git_lfs_windows () {
    echo "Please install Git LFS from https://git-lfs.github.com/ and rerun this script."
    exit 1
}

# Function to initialize Git LFS
initialize_git_lfs () {
    git lfs install
}

# Function to set up Git LFS tracking
setup_git_lfs_tracking () {
    git lfs track "data/raw_html_data/*.html"
    git lfs track "data/consolidated/crawler_state.db"
}

# Function to create directory structure
create_directories () {
    echo "Creating project directory structure..."
    mkdir -p config
    mkdir -p data/raw_html_data
    mkdir -p data/consolidated/import_files
    mkdir -p scripts
    mkdir -p utils
    mkdir -p logs
    mkdir -p tests
}

# Function to create .gitignore
create_gitignore () {
    echo "Creating .gitignore..."
    cat <<EOL > .gitignore
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# C extensions
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
*.egg-info/
.installed.cfg
*.egg

# Virtual environments
venv/
ENV/
env/
.venv/
.env/
env.bak/
venv.bak/

# Installer logs
pip-log.txt
pip-delete-this-directory.txt

# Unit test / coverage reports
htmlcov/
.tox/
.nox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
*.py,cover
.hypothesis/

# Translations
*.mo
*.pot

# Django stuff:
*.log
local_settings.py

# Flask stuff:
instance/
.webassets-cache

# Scrapy stuff:
.scrapy

# Sphinx documentation
docs/_build/

# PyBuilder
target/

# Jupyter Notebook
.ipynb_checkpoints

# IPython
profile_default/
ipython_config.py

# pyenv
.python-version

# celery beat schedule file
celerybeat-schedule

# dotenv
.env

# mypy
.mypy_cache/
.dmypy.json
dmypy.json

# Pyre type checker
.pyre/

# VSCode
.vscode/

# Logs
logs/
*.log

# Memgraph logs
memgraph_importer.log
unwrap_matching_nodes.log
yaml_csv_prep.log
html_processor.log

# Database files
*.sqlite3
*.db

# Git LFS tracked files
data/raw_html_data/*.html
data/consolidated/crawler_state.db
EOL
}

# Function to create README.md
create_readme () {
    echo "Creating README.md..."
    cat <<EOL > README.md
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

\`\`\`
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
\`\`\`

## Installation

1. **Clone the Repository:**

   \`\`\`bash
   git clone https://github.com/yourusername/explore-aonprd.git
   cd explore-aonprd
   \`\`\`

2. **Set Up Virtual Environment:**

   It's recommended to use a virtual environment to manage dependencies.

   \`\`\`bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\\Scripts\\activate
   \`\`\`

3. **Install Dependencies:**

   \`\`\`bash
   pip install -r requirements.txt
   \`\`\`

## Usage

Run the main processing script to execute the entire pipeline:

\`\`\`bash
python scripts/process.py
\`\`\`

**Note:** Ensure that all required directories and database files exist before running the scripts.

## Configuration

All configuration parameters are centralized in \`config/config.py\`. Adjust paths, logging configurations, and processing limits as needed.

## Logging

Logs are stored in the \`logs/\` directory. Each script generates its own log file for easy tracking:

- \`html_processor.log\`
- \`memgraph_importer.log\`
- \`unwrap_matching_nodes.log\`
- \`yaml_csv_prep.log\`

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE).
EOL
}

# Function to create requirements.txt
create_requirements () {
    echo "Creating requirements.txt..."
    cat <<EOL > requirements.txt
beautifulsoup4==4.11.2
networkx==3.1
tqdm==4.65.0
PyYAML==6.0
aiosqlite==0.17.0
aiofiles==23.1.0
uvloop==0.17.0
gqlalchemy==1.1.1
rapidfuzz==2.13.6
xxhash==3.3.4
memgraph==2.0.0
EOL
}

# Function to initialize Git repository
initialize_git () {
    echo "Initializing Git repository..."
    git init
}

# Function to add and commit files
initial_commit () {
    echo "Adding files to Git..."
    git add .
    echo "Making initial commit..."
    git commit -m "Initial commit: Set up project structure and configurations"
}

# Function to add remote repository and push
push_to_github () {
    read -p "Enter your GitHub repository URL (e.g., https://github.com/yourusername/explore-aonprd.git): " GITHUB_REPO

    if [[ ! "$GITHUB_REPO" =~ ^https?://github.com/.+/.+\.git$ ]]; then
        echo "Invalid GitHub repository URL. Please ensure it follows the format: https://github.com/username/repository.git"
        exit 1
    fi

    echo "Adding remote origin..."
    git remote add origin "$GITHUB_REPO"

    echo "Pushing to GitHub..."
    git branch -M main
    git push -u origin main
}

# ------------------------------
# Main Script Execution
# ------------------------------

echo "----------------------------------------"
echo "          Project Setup Script          "
echo "----------------------------------------"

# Check if Git is installed
if ! command_exists git ; then
    echo "Git is not installed. Please install Git and rerun this script."
    exit 1
fi

# Check if Git LFS is installed
if ! command_exists git-lfs ; then
    echo "Git LFS is not installed."
    read -p "Do you want to install Git LFS now? (y/n): " INSTALL_LFS
    if [[ "$INSTALL_LFS" == "y" || "$INSTALL_LFS" == "Y" ]]; then
        # Detect OS and install accordingly
        if [[ "$OSTYPE" == "darwin"* ]]; then
            if command_exists brew ; then
                install_git_lfs_mac
            else
                echo "Homebrew is not installed. Please install Homebrew or Git LFS manually."
                install_git_lfs_mac
            fi
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            install_git_lfs_linux
        elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
            install_git_lfs_windows
        else
            echo "Unsupported OS. Please install Git LFS manually."
            exit 1
        fi
    else
        echo "Git LFS is required for this project. Exiting."
        exit 1
    fi
fi

# Initialize Git LFS
initialize_git_lfs

# Create directory structure
create_directories

# Create essential files
create_gitignore
create_readme
create_requirements

# Initialize Git repository
initialize_git

# Set up Git LFS tracking
setup_git_lfs_tracking

# Add and commit files
initial_commit

# Push to GitHub
push_to_github

echo "----------------------------------------"
echo "          Setup Completed!              "
echo "----------------------------------------"
echo "Your project is now set up and pushed to GitHub."
