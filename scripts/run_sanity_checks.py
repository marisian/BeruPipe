from src.config import get_config
from temp.plausibility import (
    plausibility_transformed_occ_data
    )
import pandas as pd
import pickle

# load config
cfg = get_config()

# load intermediate
with open(cfg.paths.intermediate_data_dir / "bfield_dict.pkl", "rb") as f:
    parsed_dict = pickle.load(f)

# load attributes
df_1 = pd.read_pickle(cfg.paths.processed_data_dir / "dkz_attributes.pkl")

# load transformed b-fields
transformed_dict = {}

df_2 = pd.read_pickle(cfg.paths.intermediate_data)


# run sanity checks on data
#plausibility_transformed_occ_data(df_2)


