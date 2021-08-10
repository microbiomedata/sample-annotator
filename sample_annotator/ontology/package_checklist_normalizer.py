# PyCharm complaining that argparse==1.4.0 is not satisfied
# and that yaml is not listed in the project requirements
# unsuccessful previous attempt to install nim plugin for code-formatting .cfg files
# https://plugins.jetbrains.com/plugin/15128-nim
import yaml
import requests
import pandas as pd
from xml.etree import ElementTree
import re
from string import punctuation

MULTI_WHITESPACE = re.compile(r"\s+")

# put some caching mechanism in place

# # get/show the most common env package values in harmonized_table.db ?
# # created by https://github.com/INCATools/biosample-analysis
# # data/env_package_counts_202108100944.tsv
# select
# 	env_package,
# 	count(1)
# from
# 	biosample b
# group by
# 	env_package
# order by
# 	count(1) desc;

# what to do with NULLs and empty strings?
# prefix "MIGS/MIMS/MIMARKS."

# manual overrides for common strings in NCBI but not mixs
# put this in a config file?
# or express these with patterns in executable code?
env_package_overrides = {
    "default": "no environmental package",
    # ?
    "microbial": "microbial mat/biofilm",
    "miscellaneous natural or artificial environment": "misc environment",
    "missing": "no environmental package",
    "not applicable": "no environmental package",
    "not available": "no environmental package",
    "not collected": "no environmental package",
    "soil associated": "soil",
    "soil-associated": "soil",
    "unknown": "no environmental package",
    "unspecified": "no environmental package",
}


# examine https://github.com/cmungall/mixs-source with linkml methods to see what packages are allowed?
#   or maybe just pyyaml if no fancy methods are required
# env_package_enum is defined in in terms.yaml
# and corresponds to EnvPackageDisplay in https://www.ncbi.nlm.nih.gov/biosample/docs/packages/?format=xml
# ['air', 'built environment', 'host-associated', 'human-associated', 'human-skin', 'human-oral', 'human-gut',
# 'human-vaginal', 'hydrocarbon resources-cores', 'hydrocarbon resources-fluids/swabs', 'microbial mat/biofilm',
# 'misc environment', 'plant-associated', 'sediment', 'soil', 'wastewater/sludge', 'water']

def get_envpacks_from_mixssource(mixssource_terms_url="https://raw.githubusercontent.com/cmungall/mixs-source/main"
                                                      "/model/schema/terms.yaml") -> [str]:
    r = requests.get(mixssource_terms_url)
    t = r.text
    model = yaml.safe_load(t)
    enums = list(model['enums']['env_package_enum']['permissible_values'].keys())
    return enums


# # also handy to look at https://www.ncbi.nlm.nih.gov/biosample/docs/packages/?format=xml
# # because that has slightly EnvPackage and EnvPackageDisplay fields that can be matched against input
# envpack_frame.columns
# Index(['Name', 'DisplayName', 'ShortName', 'EnvPackage', 'EnvPackageDisplay',
#        'NotAppropriateFor', 'Description', 'Example'],
#       dtype='object')
def get_envpack_frame_from_ncbi(ncbi_packages_url="https://www.ncbi.nlm.nih.gov/biosample/docs/packages/"
                                                  "?format=xml") -> pd.DataFrame:
    bio_s_columns = ['Name', 'DisplayName', 'ShortName', 'EnvPackage', 'EnvPackageDisplay', 'NotAppropriateFor',
                     'Description', 'Example']
    bio_s_df = pd.DataFrame(columns=bio_s_columns)
    bio_s_xml = requests.get(ncbi_packages_url, allow_redirects=True)
    bio_s_root = ElementTree.fromstring(bio_s_xml.content)
    for node in bio_s_root:
        rowdict = {}
        for framecol in bio_s_columns:
            temp = node.find(framecol).text
            temptext = ''
            if temp is not None:
                temptext = temp
            rowdict[framecol] = temptext
        bio_s_df = bio_s_df.append(rowdict, ignore_index=True)
    return bio_s_df


def standard_tidy(raw_string: str) -> str:
    placeholder = raw_string.lower()
    # slow? but explicit and thorough
    for char in punctuation:
        placeholder = placeholder.replace(char, ' ')
    placeholder = MULTI_WHITESPACE.sub(" ", placeholder).strip()
    return placeholder


# SettingWithCopyWarning:
# A value is trying to be set on a copy of a slice from a DataFrame
def emphasize_envpack(entire_envpack_frame):
    envpack_packcols = entire_envpack_frame[['EnvPackage', 'EnvPackageDisplay']]
    envpack_packcols.drop_duplicates(inplace=True)
    envpack_packcols['EnvPackage_tidy'] = envpack_packcols['EnvPackage'].apply(standard_tidy)
    envpack_packcols['EnvPackageDisplay_tidy'] = envpack_packcols['EnvPackageDisplay'].apply(standard_tidy)
    envpack_packcols = envpack_packcols[envpack_packcols['EnvPackage'] != '']
    return envpack_packcols


# could programmatically reconcile these:
# model['enums']['env_package_enum']['permissible_values']
#   from https://raw.githubusercontent.com/cmungall/mixs-source/main/model/schema/terms.yaml
# https://www.ncbi.nlm.nih.gov/biosample/docs/packages/?format=xml
# common env_package values in https://www.ncbi.nlm.nih.gov/biosample/docs/packages/?format=xml
#   OR
# just pre-compose a curated mapping to
#   https://raw.githubusercontent.com/cmungall/mixs-source/main/model/schema/terms.yaml
def get_curated_env_package_mappings(filepath="../data/curated_env_packages.txt") -> pd.DataFrame:
    curated_env_package = pd.read_csv(filepath, sep='\t')
    return curated_env_package


def normalize_package(raw_package: str) -> str:
    # bootstrapping off of env_package_nomralizastion in https://github.com/turbomam/scoped-mapping
    # decompose_series from scoped mapping tries to detect and isolate CURIE term IDs
    #   that requires knowing what patterns to look for
    #   I had been determining patterns by importing rexpy from tdda and running that over entire ontology files
    #   Can that be run over multiple patterns at a time? Had just been doing ENVO
    #   This may not be worth it in terms of the number of unique env_package values containing an EnvO term
    #
    # # return an indicator of whether the normalized env_package is known to mixs?
    # enums = get_envpacks_from_mixssource()
    # tidy_enums = [standard_tidy(item) for item in enums]
    # # print(tidy_enums)
    # envpack_frame = get_envpack_frame_from_ncbi()
    # envpack_emphasized = emphasize_envpack(envpack_frame)
    # envpack_emphasized.to_clipboard()
    # print(envpack_emphasized)
    # # apply standard_tidy to INPUT AND MIXS SOURCE VALUES!
    curated_env_package_mappings = get_curated_env_package_mappings()
    placeholder = standard_tidy(raw_package)
    placeholder = re.sub("^migs mims mimarks ", "", placeholder)
    match = curated_env_package_mappings.loc[curated_env_package_mappings['tidied_input'] == placeholder, "target"]
    if len(match) == 1:
        return match.squeeze()
    # signature says return string
    # option for returning None?
    else:
        return ""
