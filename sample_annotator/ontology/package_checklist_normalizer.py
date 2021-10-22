# PyCharm complaining that argparse==1.4.0 is not satisfied
# and that yaml is not listed in the project requirements
# unsuccessful previous attempt to install nim plugin for code-formatting .cfg files
# https://plugins.jetbrains.com/plugin/15128-nim
from typing import Optional

import pandas as pd
from linkml_runtime.utils.schemaview import SchemaView


# MULTI_WHITESPACE = re.compile(r"\s+")
#
# default_mixssource_terms_url = "https://raw.githubusercontent.com/cmungall/mixs-source/main/model/schema/terms.yaml"
# default_ncbi_packages_url = "https://www.ncbi.nlm.nih.gov/biosample/docs/packages/?format=xml"
# curated_env_packages_path = "../../data/curated_env_packages.txt"
#
#
# # put some caching mechanism in place
#
# # # get/show the most common env package values in harmonized_table.db ?
# # # created by https://github.com/INCATools/biosample-analysis
# # # data/env_package_counts_202108100944.tsv
# # select
# # 	env_package,
# # 	count(1)
# # from
# # 	biosample b
# # group by
# # 	env_package
# # order by
# # 	count(1) desc;
#
# # what to do with NULLs and empty strings?
#
# def get_envpacks_from_mixssource(mixssource_terms_url=default_mixssource_terms_url) -> [str]:
#     """
#     examine https://github.com/cmungall/mixs-source with yaml methods to see what packages are allowed
#
#     Simple, so not using limkml methods
#     env_package_enum is defined in in terms.yaml and corresponds to EnvPackageDisplay in
#     https://www.ncbi.nlm.nih.gov/biosample/docs/packages/?format=xml
#     values:
#     ['air', 'built environment', 'host-associated', 'human-associated', 'human-skin', 'human-oral', 'human-gut',
#     'human-vaginal', 'hydrocarbon resources-cores', 'hydrocarbon resources-fluids/swabs', 'microbial mat/biofilm',
#     'misc environment', 'plant-associated', 'sediment', 'soil', 'wastewater/sludge', 'water']
#     """
#     r = requests.get(mixssource_terms_url)
#     t = r.text
#     model = yaml.safe_load(t)
#     enums = list(model['enums']['env_package_enum']['permissible_values'].keys())
#     return enums
#
#
# def get_envpack_frame_from_ncbi(ncbi_packages_url=default_ncbi_packages_url) -> pd.DataFrame:
#     """
#     handy to check https://www.ncbi.nlm.nih.gov/biosample/docs/packages/?format=xml for reasonable env_package vals
#
#     It has slightly different EnvPackage and EnvPackageDisplay fields that can be matched against input
#     envpack_frame.columns:
#     Index(['Name', 'DisplayName', 'ShortName', 'EnvPackage', 'EnvPackageDisplay',
#            'NotAppropriateFor', 'Description', 'Example'],
#           dtype='object')
#     """
#     bio_s_columns = ['Name', 'DisplayName', 'ShortName', 'EnvPackage', 'EnvPackageDisplay', 'NotAppropriateFor',
#                      'Description', 'Example']
#     bio_s_df = pd.DataFrame(columns=bio_s_columns)
#     bio_s_xml = requests.get(ncbi_packages_url, allow_redirects=True)
#     bio_s_root = ElementTree.fromstring(bio_s_xml.content)
#     for node in bio_s_root:
#         rowdict = {}
#         for framecol in bio_s_columns:
#             temp = node.find(framecol).text
#             temptext = ''
#             if temp is not None:
#                 temptext = temp
#             rowdict[framecol] = temptext
#         bio_s_df = bio_s_df.append(rowdict, ignore_index=True)
#     return bio_s_df
#
#
# def standard_tidy(raw_string: str) -> str:
#     """
#     Lowercase; replace all punctuation with whitespace and strip leading, trailing or multi whitespace
#
#     punctuation -> whitespace: thorough and easy to read. possible slow?
#     """
#     placeholder = raw_string.lower()
#     for char in punctuation:
#         placeholder = placeholder.replace(char, ' ')
#     placeholder = MULTI_WHITESPACE.sub(" ", placeholder).strip()
#     return placeholder
#
#
# # SettingWithCopyWarning:
# # A value is trying to be set on a copy of a slice from a DataFrame
# def emphasize_envpack(entire_envpack_frame):
#     envpack_packcols = entire_envpack_frame[['EnvPackage', 'EnvPackageDisplay']]
#     envpack_packcols.drop_duplicates(inplace=True)
#     envpack_packcols['EnvPackage_tidy'] = envpack_packcols['EnvPackage'].apply(standard_tidy)
#     envpack_packcols['EnvPackageDisplay_tidy'] = envpack_packcols['EnvPackageDisplay'].apply(standard_tidy)
#     envpack_packcols = envpack_packcols[envpack_packcols['EnvPackage'] != '']
#     return envpack_packcols
#
#
# # could programmatically reconcile these:
# # model['enums']['env_package_enum']['permissible_values']
# #   from https://raw.githubusercontent.com/cmungall/mixs-source/main/model/schema/terms.yaml
# # https://www.ncbi.nlm.nih.gov/biosample/docs/packages/?format=xml
# # common env_package values in https://www.ncbi.nlm.nih.gov/biosample/docs/packages/?format=xml
# #   OR
# # just pre-compose a curated mapping to
# #   https://raw.githubusercontent.com/cmungall/mixs-source/main/model/schema/terms.yaml
#
# # enums = get_envpacks_from_mixssource()
# # tidy_enums = [standard_tidy(item) for item in enums]
# # # print(tidy_enums)
# # envpack_frame = get_envpack_frame_from_ncbi()
# # envpack_emphasized = emphasize_envpack(envpack_frame)
# # envpack_emphasized.to_clipboard()
# # print(envpack_emphasized)
# # # apply standard_tidy to both the INPUT AND MIXS SOURCE VALUES,
# # # so that they will both have punctuation replaced with whitespace!
# def get_curated_env_package_mappings(filepath=curated_env_packages_path) -> pd.DataFrame:
#     curated_env_package = pd.read_csv(filepath, sep='\t')
#     return curated_env_package
#
#
# # is there a better place to do this globally?
# curated_env_package_mappings = get_curated_env_package_mappings()
#
#
def normalize_package(raw_package: str) -> Optional[str]:
    """
    Inspect raw_package and return a matching MIxS environmental package term if possible

    This could be based on the (misspelled) method
    env_package_nomralizastion in https://github.com/turbomam/scoped-mapping
    We could take the general approach of trying to parse out OBO foundry term IDs from label-like strings
    decompose_series from scoped mapping an do the term ID isolation,
    but it requires a specification of term ID patterns to look for
    Those patterns can be deduced from a "large enough" set of valid IDs
    by using rexpy from tdda
    Could the term isolation be run over multiple patterns at a time? We have been just running it over ENVO
    This is probably of more use in normalizing triad strings.
    EnvO IDs don't appear very often in the env_package strings
    TODO: return an indicator of whether the normalized env_package is known to mixs? Or None on failure?
    """
    # placeholder = standard_tidy(raw_package)
    # placeholder = re.sub("^migs mims mimarks ", "", placeholder)
    # match = curated_env_package_mappings.loc[curated_env_package_mappings['tidied_input'] == placeholder, "target"]
    # if len(match) == 1:
    #     return match.squeeze()
    # else:
    #     return None

    MIXS_LINKML_ROOT = '../mixs-source/model/schema/mixs.yaml'
    mixs_view = SchemaView(MIXS_LINKML_ROOT)

    # infer packages as the parents of classes that have mixins?
    # and the mixins would be the checklists?
    class_keys = list(mixs_view.all_classes().keys())
    class_keys.sort()
    class_structures = []
    for current_key in class_keys:
        current_structure = {"class": current_key}
        current_class = mixs_view.get_class(current_key)
        current_parent = current_class.is_a
        if current_parent is not None:
            current_structure["parent"] = str(current_parent)
        current_mixers = current_class.mixins
        if len(current_mixers) == 1:
            current_structure["mixin"] = current_mixers[0]
        class_structures.append(current_structure)
    structure_frame = pd.DataFrame(class_structures)
    has_mixin = structure_frame.loc[~ structure_frame["mixin"].isnull()]
    inferred_packages = has_mixin['parent'].unique()
    inferred_packages = list(inferred_packages)
    inferred_packages = [x for x in inferred_packages if pd.isnull(x) == False]
    inferred_packages.sort()
    print(inferred_packages)

# sources of truth about package and checklist names:
# NCBI XML download at
# MIxS LinkML YAML at
# default_ncbi_packages_url = "https://www.ncbi.nlm.nih.gov/biosample/docs/packages/?format=xml"
# AND how to do this all once, not each time the function is called?
