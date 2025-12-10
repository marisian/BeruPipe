# BeruPipe
An upstream data parsing pipeline designed to remove XML structure and boilerplate from raw occupational database 
extracts (Berufenet). 
Creates a simple data model that links cleaned description texts of multiple occupational dimensions to occupational and 
temporal metadata. Downstream, the data model can be used for text mining and feature extraction of occupation text data.

1. [About](#1-about)
2. [Requirements](#2-requirements)
3. [Installation](#3-installation)
4. [Settings](#4-settings)
5. [Usage](#5-usage)
6. [Data](#6-data)
7. [Testing](#7-testing)
8. [Contact Info](#8-contact)

## 1. About
This project implements a two-step pipeline for transforming XML files from the occupational databases
1. **XMLProcessor**: Finds raw XML files, parses content and creates a raw DataFrame with correct indexing
2. **TextTransformer**: Cleans and normalizes and specifically transforms occupation descriptions from occupation DataFrames.


## 2. Requirements
* Python 3.10 or higher
* Pip
* Access to raw occupation database data (Berufenet)

## 3. Installation

1.  **Clone repository**
    ```bash
    git clone https://github.com/marisian/BeruPipe
    cd BeruPipe
    ```

2.  **Create virtual environment and activate**
    ```bash
    python -m venv venv
    source venv/bin/activate  # Linux/macOS
    # or
    venv\Scripts\activate     # Windows
    ```

3.  **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

## 4. Settings

All settings, paths and required information fields are administered in the file `src/config.py`.

**Important parameters:**

* **`BASE_DIR`**: Base directory of the project
* **`Paths` Data Class**: Defines input-, intermediate and output directories
* **`Params.tag_map`**: Defines the mapping of XML-Tags (z.B. `b11-2`) to readable column names.

## 5. Usage
The main process is started by the script `main.py` (uses paths and parameters from `config.py`).

## 6. Data
### Input data
The modules of this program create cleaned and transformed data objects from raw input data. 
The following input data is required for the scripts to run:
* XML files with descriptive fields (b-fields) (1 file = 1 occupation-year)
* An XML file with occupation meta-attributes (e.g. berufe.xml)

### Output data
The main outputs of this program are data objects with transformed (cleaned, normalized) texts
of config-specified information fields. Additionally, a metadata file with codes and other attributes at constant 
occupation level is produced. The data can be linked across tables by the occupation ID ("dkz_id")

## 7. Testing

The project uses `pytest`-unit tests for testing the functionality of `XMLDataProcessor` and 
`TextTransformer`

**Run all tests:**

```bash
pytest
```

## 8. Contact info
[https://github.com/marisian](https://github.com/marisian)

