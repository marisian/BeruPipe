from typing import List, Dict
import re
import os
import pandas as pd
import xml.etree.ElementTree as ET

def get_file_paths(
    raw_file_directory: os.PathLike,
    prefix: str = "beschreibung_beruf_"
    ) -> List[str]:
    """
    Collects the path strings of all files in the raw_file_directory. Returns all files that have the given prefix in
    their file names in a list.
    Parameters
    ----------
    raw_file_directory: Path object containing the single directory in which all raw data files are located.
    prefix: File name prefix used for relevant file identification.

    Returns
    -------
    List of file paths
    """
    files = []
    try:
        if not os.path.isdir(raw_file_directory):
            return []
        for filename in os.listdir(raw_file_directory):
            f = os.path.join(raw_file_directory, filename)
            if os.path.isfile(f):
                if filename.startswith(prefix):
                    files.append(f)
        return files
    except (FileNotFoundError, NotADirectoryError, PermissionError) as e:
        print(f"Warning: could not read '{raw_file_directory}'. Error: {e}")

def files_to_frame(
        input_files: List[str],
        tag_dict: Dict[str, str],
        exclude_tags: List[str] = None
) -> pd.DataFrame:
    """
    Creates a list of dicts containing the features collected in each raw data file. Loops through all raw data files
    and applies parse_xml to each file, given a dict of relevant features to extract. Optionally passes exclude_tags
    set of tags that should be ignored.

    Parameters
    ----------
    input_files: List of input file paths
    tag_dict: Dict of xml tags that enclose the text data to be extracted
    exclude_tags: List of tags to ignore that will be passed to parse_xml

    Returns
    -------
    DataFrame with columns containing the extracted features. 1 parsed XML file = 1 row of data.
    """
    rows = []
    for file in input_files:
        # Create a dict for each raw XML file
        row = parse_xml(file, tag_dict, exclude_tags=exclude_tags)
        rows.append(row)
    df = pd.DataFrame.from_dict(rows)
    return df

def parse_mapping_xml_to_frame(
        input_file_paths: List[str]
) -> pd.DataFrame:
    """
    Parses meta attribute XML file to a DataFrame.

    Parameters
    ----------
    input_file_paths: List of raw meta attribute XML files.

    Returns
    -------
    DataFrame containing 1 row per dkz_id and all relevant features extracted from the meta attribute files.
    """
    appended_data =[]
    # Loop through each raw data file
    for file in input_file_paths:
        tree = ET.parse(file)
        root = tree.getroot()
        berufe_list = []
        for beruf in root.findall("beruf"):
            berufe_data = {
                "dkz_id": int(beruf.get("id")),
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
        data[["dkz_id", "fuenfsteller", "nf_dkz_id", "vg_dkz_id"]] = data[[
            "dkz_id", "fuenfsteller", "nf_dkz_id", "vg_dkz_id"
        ]].astype("Int64")
        appended_data.append(data)
    if len(appended_data) > 0:
        df = pd.concat(appended_data)
    else:
        return pd.DataFrame(None)
    return df

def explode_tasks(
        df: pd.DataFrame,
        task_col: str
        ) -> pd.DataFrame:
    """
    Explodes DataFrame column with list of tasks to 1 task per row.
    Parameters
    ----------
    df: DataFrame containing a column named task_col, which contains list objects
    task_col: Name of column to be exploded

    Returns
    -------
    DataFrame with 1 row per task
    """
    df_explode = df.copy()
    df_explode["task"] = df_explode[task_col]
    df_explode = df_explode.explode("task").reset_index(drop=True)
    return df_explode

def parse_xml(
        input_file: str | os.PathLike,
        tag_dict: dict,
        exclude_tags=None
) -> dict:
    """
    Takes XML file and extracts the text of relevant elements from file name and from body. Puts extracted elements
    into a dict. Information fields to be extracted need to be passed through parameter 'tag_dict'. Text in excluded
    tags will be ignored.
    Parameters
    ----------
    input_file: Raw XML file to be parsed
    tag_dict: Dict of XML tags that mark the information fields in the raw data that will be extracted
    exclude_tags: Enclosed text in this tag will be ignored

    Returns
    -------
    Dict of features
    """
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

    for key in tag_dict.keys():
        if key == "b20-32":
            for b in root.findall(key):
                data[key+"_revd"] = b.get("rev")
                data[key+"_text"] = get_comp_ids(b)
        elif key == "b11-2":
            for b in root.findall(key):
                data[key+"_revd"] = b.get("rev")
                text = extract_text_b112(b, exclude_tags)
                data[key+"_text"] = text
        elif key == "b11-0":
            for b in root.findall(key):
                data[key+"_revd"] = b.get("rev")
                text = extract_text_b110(b, exclude_tags)
                data[key+"_text"] = text
        else:
            for b in root.findall(key):
                data[key+"_revd"] = b.get("rev")
                text = extract_text(b, exclude_tags)
                data[key+"_text"] = text
                
    return data

def extract_text(
        element,
        exclude_tags: set[str] | None = None
) -> List[str]:
    """
    Recursively extracts text from XML element and all of its child elements. Ignores text found in exclude_tags.
    Parameters
    ----------
    element: ET element
    exclude_tags: Set of tag names to be excluded

    Returns
    -------
    List of strings containing text found in the element.
    """
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

def extract_text_b110(
        element,
        exclude_tags: set | None = None
) -> str:
    """
    Extracts text from the specific XML element <b_11-0> and all of its child elements. Joins extracted text to a single
    string. Ignores text found in exclude_tags.
    Parameters
    ----------
    element: ET element
    exclude_tags: Set of tag names to be excluded

    Returns
    -------
    String containing all pieces of text found in the element.
    """
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

def extract_text_b112(
        element,
        exclude_tags: set | None = None,
        listitem_tag: str = "listitem"
) -> List[str]:
    """
    Extracts text features from a list-structured XML element. Each element with the tag listitem_tag will be considered
    as a single task belonging to the parent occupation. Tasks are returned in the form of a string list.
    Parameters
    ----------
    element: ET element containing a nested list structure
    exclude_tags: Tags whose text will be ignored
    listitem_tag: Tag name of the XML tag that encloses a single list item

    Returns
    -------
    List of single task strings.
    """
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

def get_comp_ids(element) -> list[str]:
    """
    Extracts a list of competence ids from an ET element (XML).
    Parameters
    ----------
    element: ET element that will be searched for the extsystef-tag containing reference keys of the competence matrix

    Returns
    -------
    List of competence ids
    """
    ids = []
    for extsysref in element.findall(".//extsysref"):
        if extsysref.attrib.get("matrix") == "true":
            ids.append(extsysref.attrib.get("idref"))
    return ids
