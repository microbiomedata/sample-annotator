import json
import pprint

import click
import logging
import click_log

logger = logging.getLogger(__name__)
click_log.basic_config(logger)


@click.command()
@click_log.simple_verbosity_option(logger)
@click.option('--input_json_file', type=click.Path(exists=True), required=True)
@click.option('--output_json_file', type=click.Path(), required=True)
def cli(input_json_file, output_json_file):
    """Simple program that greets NAME for a total of COUNT times."""

    logger.info(f"Loading input json file: {input_json_file}")
    with open(input_json_file) as json_file:
        input_json = json.load(json_file)

    bss = input_json['biosample_set']

    for i in bss:
        logger.info(f"Attempting to update depth and depth2 for biosample: {i['id']}")
        if "depth" in i:
            logger.debug(i['depth'])
            pass
            if "has_numeric_value" in i['depth']:
                logger.debug(i['depth']['has_numeric_value'])
                pass
            else:
                logger.debug("NO DEPTH has_numeric_value")
                if "has_minimum_numeric_value" in i['depth']:
                    logger.debug(i['depth']['has_minimum_numeric_value'])
                    logger.debug("updating depth.has_numeric_value with depth.has_minimum_numeric_value")
                    i['depth']['has_numeric_value'] = i['depth']['has_minimum_numeric_value']
                else:
                    logger.warning(
                        f"no depth.has_minimum_numeric_value for replacing depth.has_numeric_value in {i['id']}")
        else:
            logger.warning(f"no depth in {i['id']}")
        if "depth2" in i:
            logger.info(f"depth2 {i['depth2']} already present")
        else:
            logger.debug(f"no depth2 in {i['id']}")
            if "depth" in i and "has_maximum_numeric_value" in i['depth']:
                current_depth2 = {}
                if "has_raw_value" in i['depth']:
                    current_depth2['has_raw_value'] = i['depth']['has_raw_value']
                if "has_unit" in i['depth']:
                    current_depth2['has_unit'] = i['depth']['has_unit']
                if "has_numeric_value" in i['depth']:
                    current_depth2['has_numeric_value'] = i['depth']['has_numeric_value']
                i['depth2'] = current_depth2
                logger.debug(f"adding depth2 {i['depth2']} for {i['id']}")
            else:
                logger.warning(f"can't create a depth2 from {i['id']}")

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
    cli()
