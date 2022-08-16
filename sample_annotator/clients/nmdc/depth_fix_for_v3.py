import json

import click


@click.command()
@click.option('--input_json_file', type=click.Path(exists=True), required=True)
@click.option('--output_json_file', type=click.Path(), required=True)
def add_depth_2(input_json_file, output_json_file):
    """Simple program that greets NAME for a total of COUNT times."""
    with open(input_json_file) as json_file:
        input_json = json.load(json_file)

    bss = input_json['biosample_set']

    if "depth" in bss:
        if bss["depth"]:
            for i in bss:
                new_depth = i['depth'].copy()
                if 'has_numeric_value' not in i:
                    new_depth['has_numeric_value'] = new_depth['has_minimum_numeric_value']
                else:
                    print(f"{new_depth} already had has numeric value")
                depth2 = i['depth'].copy()
                depth2['has_numeric_value'] = depth2['has_maximum_numeric_value']
                del depth2['has_minimum_numeric_value']
                del depth2['has_maximum_numeric_value']
                i['depth'] = new_depth
                i['depth2'] = depth2

    with open(output_json_file, "w") as outfile:
        json.dump(input_json, outfile, indent=2, sort_keys=True)


if __name__ == '__main__':
    add_depth_2()
