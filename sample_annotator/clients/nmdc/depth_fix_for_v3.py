import json
import pprint

import click


@click.command()
@click.option('--input_json_file', type=click.Path(exists=True), required=True)
@click.option('--output_json_file', type=click.Path(), required=True)
def add_depth_2(input_json_file, output_json_file):
    """Simple program that greets NAME for a total of COUNT times."""

    print(f"Loading input json file: {input_json_file}")
    with open(input_json_file) as json_file:
        input_json = json.load(json_file)

    bss = input_json['biosample_set']

    for i in bss:
        if "depth" in i:
            # pprint.pprint(i['depth'])
            pass
            if "has_numeric_value" in i['depth']:
                # pprint.pprint(i['depth']['has_numeric_value'])
                pass
            else:
                # print("NO DEPTH has_numeric_value")
                if "has_minimum_numeric_value" in i['depth']:
                    # pprint.pprint(i['depth']['has_minimum_numeric_value'])
                    print("updating depth.has_numeric_value with depth.has_minimum_numeric_value")
                    i['depth']['has_numeric_value'] = i['depth']['has_minimum_numeric_value']
                else:
                    print("NO depth.has_minimum_numeric_value for replacing depth.has_numeric_value")
        else:
            print("NO DEPTH")
        if "depth2" in i:
            pprint.pprint(i['depth2'])
        else:
            print("NO DEPTH2")
        # pprint.pprint(i)
        print("\n")

    # pprint.pprint(input_json)

    # if "depth" in bss:
    #     if bss["depth"]:
    #         for i in bss:
    #             new_depth = i['depth'].copy()
    #             if 'has_numeric_value' not in i:
    #                 new_depth['has_numeric_value'] = new_depth['has_minimum_numeric_value']
    #             else:
    #                 print(f"{new_depth} already had has numeric value")
    #             depth2 = i['depth'].copy()
    #             depth2['has_numeric_value'] = depth2['has_maximum_numeric_value']
    #             del depth2['has_minimum_numeric_value']
    #             del depth2['has_maximum_numeric_value']
    #             i['depth'] = new_depth
    #             i['depth2'] = depth2

    with open(output_json_file, "w") as outfile:
        json.dump(input_json, outfile, indent=2, sort_keys=True)


if __name__ == '__main__':
    add_depth_2()
