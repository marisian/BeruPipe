from src.config import get_config
from src.utils import start_logger
from src.raw_data_parsing import (
    get_file_paths,
    parse_mapping_xml_to_frame,
)

logger = start_logger()

def parse_raw_data_pipeline(config, save=True):
    logger.info("---Started raw data parsing pipeline---")

    metadata_files = get_file_paths(config.paths.raw_data_dir, prefix="berufe")
    logger.info(f"Collected {len(metadata_files)} mapping file paths")

    df_meta = parse_mapping_xml_to_frame(metadata_files)
    df_meta = df_meta.set_index("dkz_id")
    logger.info(f"Created metadata DataFrame with {df_meta.shape[0]} rows")

    # Sanity checks
    logger.info("Running sanity checks")
    # plausibility_raw_meta(df_meta, input_files)
    logger.info("Sanity checks completed")

    # Save to file
    if save:
        df_meta.to_pickle(config.paths.processed_data_dir / "dkz_attributes.pkl")
        df_meta.to_csv(config.paths.processed_data_dir / "dkz_attributes.csv", index=True, na_rep="NA")
    else:
        logger.warning("Data has not been saved. Consider setting save=True")

    logger.info("---Completed raw metadata parsing pipeline---")
    return df_meta

if __name__ == "__main__":
    cfg = get_config()
    df_meta = parse_raw_data_pipeline(cfg, save=True)
