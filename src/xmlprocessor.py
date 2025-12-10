import os
import re
import logging
import pickle
from typing import List, Dict
import pandas as pd
import xml.etree.ElementTree as ET
from src.config import Config

class XMLProcessor:
    def __init__(
            self,
            config: Config,
            exclude_tags: List[str] = None,
    ):
        # logging
        self.logger = logging.getLogger(self.__class__.__name__)

        # config
        self._config = config
        self._paths = self._config.paths
        self._params = self._config.params

        self.raw_dir = self._paths.raw_data_dir
        self.tag_dict = self._params.tag_map
        self.core_cols = self._params.core_input_columns
        self.exclude_tags = exclude_tags

        # raw inputs
        self.occ_input_files: List[str] = []
        self.meta_input_files: List[str] = []

        # outputs
        self.bfield_dict: Dict[str, pd.DataFrame] = {}
        self.full_occ_df: pd.DataFrame = None
        self.meta_df: pd.DataFrame = None

    def run_occparsing_pipeline(self, save=True):
        self.logger.info("--- Started raw occupation data parsing pipeline ---")
        self.logger.info(f"The following tags will be extracted: {list(self.tag_dict)}")

        # base df
        self._process_occdata_to_dataframe()
        if self.full_occ_df.empty:
            self.logger.warning("No data to process found. Stopped pipeline.")
            return {}

        # set and clean index
        self._set_and_clean_index()

        # split df by b-field
        self._split_by_bfield()

        # explode task field
        self._transform_explode_tasks()

        # save
        if save:
            self._save_bfield_dict()

        self.logger.info("---Completed raw occupation data parsing pipeline---")
        return self.bfield_dict

    def run_metaparsing_pipeline(self, save=True):
        self.logger.info("--- Started raw metadata parsing pipeline ---")

        # base df
        self._parse_meta_xml_to_data_frame()
        if self.meta_df.empty:
            self.logger.warning("No data to process found. Stopped metadata parsing pipeline.")
            return {}

        self.logger.info(f"Created metadata DataFrame with {self.meta_df.shape[0]} rows")

        # save
        if save:
            self._save_metadata()
        else:
            self.logger.warning("Data has not been saved. Consider setting save=True")

        self.logger.info("---Completed raw metadata parsing pipeline---")
        return self.meta_df

    def _get_input_files(
            self,
            prefix: str
    ) -> List[str]:
        files = []
        try:
            if not os.path.isdir(self.raw_dir):
                return []
            for filename in os.listdir(self.raw_dir):
                f = os.path.join(self.raw_dir, filename)
                if os.path.isfile(f) and filename.startswith(prefix):
                    files.append(f)
            return files
        except (FileNotFoundError, NotADirectoryError, PermissionError) as e:
            self.logger.error(f"Warning: could not read '{self.raw_dir}'. Error: {e}")
            return []

    def _parse_meta_xml_to_data_frame(
            self,
            prefix: str = "berufe"
    ) -> pd.DataFrame:
        appended_data = []
        self.meta_input_files = self._get_input_files(prefix)
        if not self.meta_input_files:
            self.meta_df = pd.DataFrame()
            return self.meta_df
        # Loop through each raw data file
        for file in self.meta_input_files:
            tree = ET.parse(file)
            root = tree.getroot()
            berufe_list = []
            id_col = self.core_cols["id"]
            for beruf in root.findall("beruf"):
                berufe_data = {
                    id_col: int(beruf.get("id")),
                    "codenr": beruf.get("codenr"),
                    "fuenfsteller": int(beruf.get("codenr")[2:7]),
                    "kurzbezeichnung": beruf.get("kurzbezeichnung"),
                    "qualistufe": beruf.get("qualistufe"),
                    "reglementiert": beruf.get("reglementiert"),
                    "bkgr": beruf.get("bkgr")
                }
                nachf = beruf.find("nachfolger")
                if nachf is not None:
                    berufe_data["nf_dkz_id"] = int(nachf.get("id"))
                    berufe_data["nf_codenr"] = nachf.get("codenr")
                    berufe_data["nf_fuenfsteller"] = int(nachf.get("codenr")[2:7])
                    berufe_data["nf_kurzbezeichnung"] = nachf.get("kurzbezeichnung")
                vorg = beruf.find("vorgaenger")
                if vorg is not None:
                    berufe_data["vg_dkz_id"] = int(vorg.get("id"))
                    berufe_data["vg_codenr"] = vorg.get("codenr")
                    berufe_data["vg_fuenfsteller"] = int(vorg.get("codenr")[2:7])
                    berufe_data["vg_kurzbezeichnung"] = vorg.get("kurzbezeichnung")
                berufe_list.append(berufe_data)

            data = pd.DataFrame(berufe_list)
            data[[id_col, "fuenfsteller", "nf_dkz_id", "vg_dkz_id"]] = data[[
                id_col, "fuenfsteller", "nf_dkz_id", "vg_dkz_id"
            ]].astype("Int64")
            appended_data.append(data)
        if len(appended_data) > 0:
            self.meta_df = pd.concat(appended_data)
            self.meta_df = self.meta_df.set_index(id_col)
        else:
            self.meta_df = None
        return self.meta_df

    def _parse_occ_xml_to_dict(
            self,
            input_file: str | os.PathLike
    ) -> Dict:
        # Get elements from filename
        head, tail = os.path.split(input_file)
        integers = re.findall(r"[0-9]+", tail)

        data = {
            "dkz_id": int(integers[0]),
            "year": int(integers[1])
        }

        # Get elements from XML
        tree = ET.parse(input_file)
        root = tree.getroot()

        for key in self.tag_dict.keys():
            if key == "b20-32":
                for b in root.findall(key):
                    data[key + "_revd"] = b.get("rev")
                    data[key + "_text"] = XMLProcessor._get_comp_ids(b)
            elif key == "b11-2":
                for b in root.findall(key):
                    data[key + "_revd"] = b.get("rev")
                    text = XMLProcessor._extract_text_b112(b, self.exclude_tags)
                    data[key + "_text"] = text
            elif key == "b11-0":
                for b in root.findall(key):
                    data[key + "_revd"] = b.get("rev")
                    text = XMLProcessor._extract_text_b110(b, self.exclude_tags)
                    data[key + "_text"] = text
            else:
                for b in root.findall(key):
                    data[key + "_revd"] = b.get("rev")
                    text = XMLProcessor._extract_text(b, self.exclude_tags)
                    data[key + "_text"] = text

        return data

    def _process_occdata_to_dataframe(
            self,
            prefix: str = "beschreibung_beruf_"
    ) -> pd.DataFrame:
        # Find files
        self.occ_input_files = self._get_input_files(prefix)
        self.logger.info(f"Collected {len(self.occ_input_files)} XML file paths")
        if not self.occ_input_files:
            self.full_occ_df = pd.DataFrame()
            return

        # Parse
        rows = [self._parse_occ_xml_to_dict(file) for file in self.occ_input_files]

        # Create df
        self.full_occ_df = pd.DataFrame.from_dict([r for r in rows if r is not None])
        self.logger.info(f"Created occupation DataFrame with {self.full_occ_df.shape[0]} rows")
        return self.full_occ_df

    def _set_and_clean_index(self):
        if self.full_occ_df is None or self.full_occ_df.empty:
            return

        id_col = self.core_cols["id"]
        date_col = self.core_cols["date"]
        index_cols = [id_col, date_col]

        try:
            self.full_occ_df = self.full_occ_df.set_index(index_cols, verify_integrity=True)
            self.full_occ_df.index.set_names(index_cols, inplace=True)
            self.logger.info(f"Set index columns: {self.full_occ_df.index.names}")
        except ValueError as e:
            self.logger.error(f"Error setting index: {e}")

    def _split_by_bfield(self):
        if self.full_occ_df is None or self.full_occ_df.empty:
            return

        blist = list(self.tag_dict.keys())
        self.bfield_dict = {}

        for bfield in blist:
            cols = self.full_occ_df.filter(regex=f"^{bfield}_").columns.tolist()

            if cols:
                subdf = self.full_occ_df[cols].copy()
                self.bfield_dict[bfield] = subdf
        self.logger.info(f"Split DataFrame into {len(self.bfield_dict)} DataFrames by b-field")

    def _save_bfield_dict(self):
        try:
            output_path = self._paths.intermediate_data_dir / "bfield_dict.pkl"

            with open(output_path, "wb") as f:
                pickle.dump(self.bfield_dict, f, protocol=pickle.HIGHEST_PROTOCOL)
            self.logger.info(f"Saved b-field dictionary to: {output_path}")

        except Exception as e:
            self.logger.error(f"Error saving b-field dict: {e}")

    def _save_metadata(self):
        try:
            output_path = self._paths.processed_data_dir
            self.meta_df.to_pickle(output_path / "dkz_attributes.pkl")
            self.meta_df.to_csv(output_path / "dkz_attributes.csv", index=True, na_rep="NA")
            self.logger.info("Metadata saved successfully")
        except Exception as e:
            self.logger.error(f"Error while saving metadata: {e}")

    @staticmethod
    def explode_tasks(
            df: pd.DataFrame,
            task_col: str
    ) -> pd.DataFrame:
        df_explode = df.copy().explode(task_col).reset_index(drop=True)
        return df_explode

    def _transform_explode_tasks(self):
        b11_2_key = "b11-2"
        task_col_name = "b11-2_text"

        if b11_2_key in self.bfield_dict:
            self.logger.info(f"Found {b11_2_key} in dictionary")
            current_df = self.bfield_dict[b11_2_key]
            before = current_df.shape[0]

            exploded_df = XMLProcessor.explode_tasks(current_df, task_col_name)

            self.bfield_dict[b11_2_key] = exploded_df

            self.logger.info(
                f"Exploded task descriptions to 1 row per task. "
                f"Created {exploded_df.shape[0] - before} rows. New shape: {exploded_df.shape}"
            )
        else:
            self.logger.warning("b11-2 not found in dictionary. Skipped task explosion")

    @staticmethod
    def _extract_text(
            element,
            exclude_tags: set[str] | None = None
    ) -> List[str]:
        if exclude_tags is None:
            exclude_tags = set()
        else:
            exclude_tags = set(exclude_tags)

        texts = []

        def recurse(elem):
            if elem.tag in exclude_tags:
                return ""
            if elem.text and elem.text.strip():
                texts.append(elem.text.strip())
            for child in elem:
                recurse(child)
                if child.tail and child.tail.strip():
                    texts.append(child.tail.strip())

        recurse(element)
        return texts

    @staticmethod
    def _extract_text_b110(
            element,
            exclude_tags: set | None = None
    ) -> str:
        if exclude_tags is None:
            exclude_tags = set()
        else:
            exclude_tags = set(exclude_tags)

        if element is None or element.tag in exclude_tags:
            return None
        text = [
            text.strip() for text in element.itertext()
            if text.strip() and text.strip() != ""
        ]
        i_text = " ".join(text)
        return i_text

    @staticmethod
    def _extract_text_b112(
            element,
            exclude_tags: set | None = None,
            listitem_tag: str = "listitem"
    ) -> List[str]:
        def recurse(subelement):
            text = subelement.text or ""
            for child in subelement:
                if child.tag == listitem_tag:
                    text += child.tail or ""
                elif exclude_tags and child.tag in exclude_tags:
                    pass
                else:
                    text += recurse(child)
                    text += child.tail or ""
            return text.strip()

        listitems = element.findall(f".//{listitem_tag}")
        result_list = []
        for item in listitems:
            list_item_text = recurse(item)
            if list_item_text:
                result_list.append(list_item_text)
        return result_list

    @staticmethod
    def _get_comp_ids(element) -> list[str]:
        ids = []
        for extsysref in element.findall(".//extsysref"):
            if extsysref.attrib.get("matrix") == "true":
                ids.append(extsysref.attrib.get("idref"))
        return ids

