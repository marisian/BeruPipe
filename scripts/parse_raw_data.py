from src.config import get_config
from src.utils import start_logger
from src.raw_data_parsing import (
    get_file_paths,
    files_to_frame,
    explode_tasks
)
import pickle
from tests.plausibility import plausibility_raw_data, plausibility_tasklevel_raw_data

logger = start_logger()

def parse_raw_data_pipeline(config, save=True):
    logger.info("---Started raw data parsing pipeline---")

    tags = config.params.tag_map
    logger.info(f"The following tags will be extracted: {tags}")
    raw_data_directory = config.paths.raw_data_dir
    
    # Run core parsing
    input_files = get_file_paths(raw_data_directory)
    logger.info(f"Collected {len(input_files)} file paths")
    df = files_to_frame(input_files, tags, exclude_tags=None)
    logger.info(f"Created occupation DataFrame with {df.shape[0]} rows")

    index_cols = [config.params.core_input_columns["id"], config.params.core_input_columns["date"]]
    df = df.set_index(index_cols, verify_integrity=True)
    df.index.set_names(index_cols)
    logger.info(f"Set index columns: {df.index.names}")

    # Split full df by b-fields
    b_list = list(tags.keys())
    df_dict = {}
    for b_field in b_list:
        cols = df.filter(regex=f"^{b_field}_").columns.tolist()
        if cols:
            subdf = df[cols].copy()
        df_dict[b_field] = subdf
    print(df_dict)

    # Explode tasks from list-like description column
    logger.info(f"Exploding b11-2 to 1 row per task.")
    before = df_dict["b11-2"].shape[0]
    df_dict["b11-2"] = explode_tasks(df_dict["b11-2"], "b11-2_text")
    logger.info(
        f"Exploded task descriptions to 1 row per task. "
        f"Created {df_dict["b11-2"].shape[0]- before} rows"
        )

    # Sanity checks
    logger.info("Running sanity checks")
    #plausibility_raw_data(df, input_files)
    #plausibility_tasklevel_raw_data(df)
    logger.info("Sanity checks completed")
    
    # Save to file
    if save:
        with open(config.paths.intermediate_data_dir / "bfield_dict.pkl", "wb") as f:
            pickle.dump(df_dict, f, protocol=pickle.HIGHEST_PROTOCOL)

    else:
        logger.warning("Data has not been saved. Consider setting save=True")
    
    logger.info("---Completed raw data parsing pipeline---")
    return df_dict

if __name__ == "__main__":
    cfg = get_config()
    df_dict = parse_raw_data_pipeline(cfg, save=True)

    print(df_dict["b11-2"].info())
