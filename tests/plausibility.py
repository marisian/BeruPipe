import pandas as pd
import re
from src.config import get_config
from src.utils import start_logger

logger = start_logger()
cfg = get_config()

def check_expected_columns(df, columns):
    expected_n_columns = len(columns)
    assert set(columns).issubset(set(df.columns)), \
        "Unexpected columns"
    assert df.shape[1] >= expected_n_columns, \
        (f"Expected {expected_n_columns}, got {df.shape[1]} columns\n"
         f"Expected columns: {columns}, got columns {df.columns}")
    
def check_main_dtypes(df):
    assert pd.api.types.is_integer_dtype(df["dkz_id"]), \
        f"'dkz_id' is not an integer dtype: {df.dkz_id.dtype}"
    assert pd.api.types.is_integer_dtype(df["year"]), \
        f"'year' is not an integer dtype: {df.year.dtype}"

def check_one_to_many(df, parent, child):
    mapping = df.dropna().groupby(child)[parent].nunique()
    assert (mapping == 1).all(), \
        f"Some {child}s are linked to multiple {parent}s"
        
def check_panel_stability(df, group, time, threshold=0.2):
    counts = df.dropna(subset=group).groupby(time)[group].count()
    divergence = counts.pct_change().abs()
    if not (divergence.fillna(0) <= threshold).all():
        logger.warning(f"Divergence in {group}s per {time}:\n{divergence}")
        
def check_year_balance_input_files(input_files, threshold=0.2):
    years = [
        int(re.search(r"_(\d{4})\.xml$", f).group(1)) for f in input_files
        ]
    counts = pd.Series(years).value_counts().sort_index()
    expected_years = [2012, 2015, 2019, 2024]
    missing = set(expected_years) - set(years)
    assert not missing, f"Missing years: {missing}"
    
    avg = counts.mean()
    deviation = (counts / avg - 1).abs()
    unbalanced = deviation[deviation > threshold]
    if not unbalanced.empty:
        logger.warning(f"Unbalanced years:\n{unbalanced}")
    return counts

def check_string_uniqueness(
        df: pd.DataFrame,
        id_col: str,
        str_col: str
        ) -> None:
    unique_per_id = df.groupby(id_col)[str_col].unique()
    all_strings = []
    for lst in unique_per_id:
        all_strings.extend(lst)
        
    duplicates = pd.Series(all_strings).duplicated(keep=False)
    dup_vals = pd.Series(all_strings)[duplicates].unique()
    assert len(dup_vals) == 0, f"Strings repeated across {id_col}: {dup_vals}"
    
def find_duplicates(df, subset):
    duplicates = df[df.duplicated(subset=subset, keep=False)]
    return duplicates

def plausibility_raw_data(
        df: pd.DataFrame, 
        input_files: list[str] = None
        ) -> None:
    """
    Run plausibility checks on the DataFrame that is generated from raw XML files.

    Parameters
    ----------
    df : DataFrame parsed from raw occupation data. This DataFrame is produced by the script parse_raw_data.py
    input_files : List of files that were used for parsing
    """
    # Input files are balanced by year
    if input_files:
        check_year_balance_input_files(input_files)
    
    # Has expected columns
    text_cols = [f"{col}_text" for col in cfg.params.tag_map_reduced.keys()]
    expected_columns = [
        "dkz_id", "year"
        ] + text_cols
    check_expected_columns(df, expected_columns)
    
    # No missings in critical fields
    assert not df["dkz_id"].isnull().any(), \
        "Missing values in dkz_id"
    assert not df["year"].isnull().any(), \
        "Missing values in year"
            
    # Correct dtypes
    check_main_dtypes(df)
        
    # Has text columns
    assert any(col.endswith("_text") for col in df.columns), \
        "No column with '_text' found"
    all_text_cols = [col for col in df.columns if col.endswith("_text")]
    for col in all_text_cols:
        assert pd.api.types.is_object_dtype(df[col]), \
            f"'{col}' is not an object dtype"
        if df[col].isnull().any():
            logger.warning(
                f"{df[col].isnull().sum()} missing values in '{col}'"
                )
    
    # Shape plausibility
    if input_files:
        assert df.shape[0] <= len(input_files), "More rows than input files"
        assert (df.shape[0] / len(input_files)) >= 0.85, \
            f"Too much divergence between row count and input files"
    
    # Value plausibility
    valid = df.dropna()
    assert valid["year"].isin([2012, 2015, 2019, 2024]).all(), \
        "Unexpected years"
    if "b11-0_text" in df.columns:
        valid_t = valid["b11-0_text"].astype(str)
        assert (valid_t.str.len() >= 100).all(), \
            f"Some of 'b11-0_text' are of length < 100"
    if "b11-2_text" in df.columns:
        assert valid["b11-2_text"].map(lambda x: isinstance(x, list)).all(), \
            f"Some of 'b11-2_text' contain non-list values"
        assert (valid["b11-2_text"].str.len() >= 3).all(), \
            f"Some of 'b11-2_text' have < 3 elements"
        assert (valid["b11-2_text"].str.len() <= 75).all(), \
            f"Some of 'b11-2_text' have > 75 elements"
    
    # Uniqueness
    if "dkz_id" in df.columns and "year" in df.columns:
        assert not df.duplicated(subset=["dkz_id", "year"]).any(), \
            "Non-unique dkz_id-year combinations"
            
    # Distribution and hierarchy
    #check_one_to_many(df, "fuenfsteller", "dkz_id") # re-check for merged
    check_panel_stability(df, "dkz_id", "year")

    return None

def plausibility_tasklevel_raw_data(df):
    # Has expected columns
    text_cols = [f"{col}_text" for col in cfg.params.tag_map_reduced.keys()]
    expected_columns = [
        "dkz_id", "year"
        ] + text_cols
    check_expected_columns(df, expected_columns)

    # No missings in critical fields
    assert not df["dkz_id"].isnull().any(), \
        "Missing values in dkz_id"
    assert not df["year"].isnull().any(), \
        "Missing values in year"
    for col in text_cols:
        if df[col].isnull().any():
            logger.warning(
                f"{df[col].isnull().sum()}" 
                "\nmissing values in '{col}'"
                )

    # Correct dtypes
    check_main_dtypes(df)

    # Shape plausibility
    assert df.shape[0] <= 200000, "Implausible number of rows: More than 200000 rows"

    # Value plausibility
    valid = df.dropna()
    assert valid["year"].isin([2012, 2015, 2019, 2024]).all(), \
        "Unexpected years"
    if "task" in df.columns:
        tasks = df["task"]
        contains_non_string = tasks[tasks.apply(type) != str].any()
        assert not contains_non_string, \
            f"Some of 'task' contain non-string values"
        assert (valid["task"].str.len() >= 3).all(), \
            f"Some of 'task' are of length < 3"
        assert (valid["task"].str.len() <= 300).all(), \
            f"Some of 'task' have > 300 elements"

    # Distribution and hierarchy
    check_panel_stability(df, "dkz_id", "year")

    return None

def plausibility_transformed_occ_data(df: pd.DataFrame, t_col: str) -> None:
    """
    Run plausibility checks on transformed DataFrame on occupation level
    (script: transform_data.py).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with transformed text and code columns.
        (script: transform_data.py)
    """
    # Has expected columns
    expected_columns = [
        "dkz_id", 
        "year",
        "fuenfsteller"
        ]
    check_expected_columns(df, expected_columns)

    # No missings / 0 in critical fields
    assert not df["dkz_id"].isnull().any(), \
        "Missing values in dkz_id"
    assert not df["year"].isnull().any(), \
        "Missing values in year"
    if df["fuenfsteller"].isnull().any():
        logger.warning(
            f"{df.fuenfsteller.isnull().sum()}"
            " missing values in 'fuenfsteller'"
            )
                
    # Uniqueness
    assert not df.duplicated(subset=["dkz_id", "year"]).any(), \
        "Non-unique dkz_id-year combinations"
    #check_string_uniqueness(df, "dkz_id", "b11-0_text")
                    
    # Check dtypes
    check_main_dtypes(df)
    
    # Text field check  
    assert any(col.endswith("_text") for col in df.columns), \
        "No column with '_text' found"
    all_text_cols = [col for col in df.columns if col.endswith("_text")]
    for col in all_text_cols:
        assert pd.api.types.is_object_dtype(df[col]), \
            f"'{col}' is not an object dtype"
        if df[col].isnull().any():
            logger.warning(
                f"{df[col].isnull().sum()} missing values in '{col}'"
                )
    all_norm_text_cols = [
        col for col in df.columns if col.endswith("_normalized")
        ]
    for col in all_norm_text_cols:
        assert pd.api.types.is_object_dtype(df[col]), \
            f"'{col}' is not an object dtype"
        if df[col].isnull().any():
            logger.warning(
                f"{df[col].isnull().sum()} missing values in '{col}'"
                )
            
    if "b11-2_text" in df.columns:
        valid = df["b11-2_text"].dropna()
        assert valid.map(type).eq(list).all(), \
            "Not all values in b11-2_text are lists"
        assert valid.map(len).gt(0).all(), \
            "Empty lists in b11-2_text"
        assert valid.map(len).ge(3).all(), \
            "Lists with less than 3 elements in 'b11-2_text'"
        assert valid.map(len).le(75).all(), \
            "Lists with less more than 75 elements in 'b11-2_text'"
    if "b11-0_text" in df.columns:
        valid = df["b11-0_text"].dropna()
        assert valid.map(type).eq(str).all(), \
            "Not all values in b11-0_text are strings"
        assert (valid.str.len() >= 100).all(), \
            f"Some of 'b11-0_text' are of length < 100"
            
    # Distribution and hierarchy
    check_one_to_many(df, "fuenfsteller", "dkz_id")
    check_panel_stability(df, "dkz_id", "year")
    
    if int((df.groupby("fuenfsteller")["dkz_id"].count() == 1).sum()) > 12:
        logger.warning(
            f"Found more than 12 'fuenfsteller' with exactly one dkz_id"
            )
    if int(df.groupby("fuenfsteller")["dkz_id"].count().max()) > 80:
        logger.warning(
            f"Found 'fuenfsteller' with more than 80 'dkz_id'"
            )
    if int(df.groupby("fuenfsteller")["dkz_id"].count().median()) != 8:
        logger.warning(
            f"Expected a median of 'dkz_id' per 'fuenfsteller' to be 8," \
            f"got {int(df.groupby("fuenfsteller")["dkz_id"].count().median())}"
            )
    return None


def plausibility_transformed_task_data(df: pd.DataFrame, t_col: str) -> None:
    """
    Run plausibility checks on transformed DataFrame on task level
    (script: transform_data.py).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame with transformed text and code columns.
        (script: transform_data.py)
    """
    logger.warning("No sanity checks implemented for task level")
    return None
