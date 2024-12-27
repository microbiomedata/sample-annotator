import json
from typing import Any, Dict, List, Union
import click


def remove_null_keys(obj: Union[Dict, List]) -> Union[Dict, List]:
    """
    Recursively remove keys with null (None) values or empty lists from a JSON-like object.

    Args:
        obj (Union[Dict, List]): The JSON-like object (dictionary or list) to process.

    Returns:
        Union[Dict, List]: The cleaned JSON-like object with null keys and empty lists removed.
    """
    if isinstance(obj, dict):
        return {k: remove_null_keys(v) for k, v in obj.items() if v is not None and v != []}
    elif isinstance(obj, list):
        return [remove_null_keys(v) for v in obj]
    return obj


def split_study_biosample_project(
        input_file: str,
        study_output_file: str,
        biosample_output_file: str,
        project_output_file: str,
        remove_contacts: bool,
        remove_nulls: bool
) -> None:
    """
    Splits a JSON file containing study metadata into three JSON files:
    one for studies, one for biosamples, and one for projects.

    Args:
        input_file (str): Path to the input JSON file containing study metadata.
        study_output_file (str): Path to the output JSON file for studies.
        biosample_output_file (str): Path to the output JSON file for biosamples.
        project_output_file (str): Path to the output JSON file for projects.
        remove_contacts (bool): Whether to remove the 'contacts' key from all objects.
        remove_nulls (bool): Whether to remove keys with null values and empty lists from all objects.
    """

    def remove_key(obj: Dict, key: str) -> None:
        """Remove a key from a dictionary if it exists."""
        if key in obj:
            del obj[key]

    with open(input_file, 'r') as infile:
        study_data: List[Dict] = json.load(infile)

    studies: List[Dict] = []
    biosamples: List[Dict] = []
    projects: List[Dict] = []

    for study in study_data:
        if remove_contacts:
            remove_key(study, 'contacts')
        if remove_nulls:
            study = remove_null_keys(study)
        if 'biosamples' in study:
            biosample_ids = []
            for biosample in study['biosamples']:
                if remove_contacts:
                    remove_key(biosample, 'contacts')
                if remove_nulls:
                    biosample = remove_null_keys(biosample)
                if 'projects' in biosample:
                    project_ids = [project['projectGoldId'] for project in biosample['projects']]
                    for project in biosample['projects']:
                        if remove_contacts:
                            remove_key(project, 'contacts')
                        if remove_nulls:
                            project = remove_null_keys(project)
                        projects.append(project)
                    biosample['projects'] = project_ids  # Replace with only IDs
                biosamples.append(biosample)
                biosample_ids.append(biosample['biosampleGoldId'])
            study['biosamples'] = biosample_ids  # Replace with only IDs
        studies.append(study)

    # Write studies to output file
    with open(study_output_file, 'w') as study_outfile:
        json.dump(studies, study_outfile, indent=4)

    # Write biosamples to output file
    with open(biosample_output_file, 'w') as biosample_outfile:
        json.dump(biosamples, biosample_outfile, indent=4)

    # Write projects to output file
    with open(project_output_file, 'w') as project_outfile:
        json.dump(projects, project_outfile, indent=4)


@click.command()
@click.option('--input-file', '-i', type=click.Path(exists=True, dir_okay=False, readable=True), required=True,
              help='Path to the input JSON file containing study metadata.')
@click.option('--study-output-file', '-s', type=click.Path(writable=True), required=True,
              help='Path to the output JSON file for studies.')
@click.option('--biosample-output-file', '-b', type=click.Path(writable=True), required=True,
              help='Path to the output JSON file for biosamples.')
@click.option('--project-output-file', '-p', type=click.Path(writable=True), required=True,
              help='Path to the output JSON file for projects.')
@click.option('--remove-contacts', '-r', is_flag=True, default=False,
              help="If set, removes the 'contacts' key from all objects.")
@click.option('--remove-nulls', '-n', is_flag=True, default=False,
              help="If set, removes keys with null values or empty lists from all objects.")
def main(input_file: str, study_output_file: str, biosample_output_file: str, project_output_file: str,
         remove_contacts: bool, remove_nulls: bool) -> None:
    """
    CLI tool to split a JSON file of study metadata into three JSON files:
    one for studies, one for biosamples, and one for projects.

    Optionally removes the 'contacts' key and/or keys with null values and empty lists from all objects.
    """
    split_study_biosample_project(input_file, study_output_file, biosample_output_file, project_output_file,
                                  remove_contacts, remove_nulls)


if __name__ == "__main__":
    main()
