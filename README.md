# NMDC Sample Annotator API

## Installing

This requires python 3.7.x or later (as default python).

If you have pipenv installed:

```bash
git clone ...
cd sample-annotator
make test
```

For those using venv, you'll need something like:

```bash
git clone ...
cd sample-annotator
python3.7 -m venv env
source ./env/bin/activate
pip install pipenv
PIPENV_IGNORE_VIRTUALENVS=1 make test
```

## What is it?

This is a python and flask API for performing annotation of samples from semi-structured or untidy data

The API takes as input a JSON object or dictionary representing a simple sample, where each key is a metadata field

It will attempt to tidy and infer missing data according to a specified schema (currently MIxS)

## Command Line

```bash
pipenv run annotate-sample --help

Usage: annotate-samples [OPTIONS] SAMPLEFILE

  Annotate a file of samples, producing a "repaired"/enhanced sample file as
  output, together with a report

  The input file must be a JSON fine containing an array of dicts

Options:
  -v, --validateonly / -g, --generate
                                  Just validate / generate output (default:
                                  generate)

  -s, --output TEXT               JSON for tidied samples
  -R, --report-file TEXT          report file
  -G, --googlemaps-api-key-path TEXT
                                  path to file containing google maps API KEY
  -B, --bioportal-api-key-path TEXT
                                  path to file containing bioportal API KEY
  --help                          Show this message and exit.
```

E.g.

```bash
pipenv run annotate-sample -G config/googlemaps-api-key.txt -R examples/report.tsv examples/gold.json
```

This will transform input such as:

```json
[
    {
        "id": "gold:Gb0108335",
        "community": "microbial communities",
        "depth": "0.0 m",
        "ecosystem": "Environmental",
        "ecosystem_category": "Terrestrial",
        "ecosystem_subtype": "Wetlands",
        "ecosystem_type": "Soil",
        "env_broad_scale": "ENVO:00000446",
        "env_local_scale": "ENVO:00000489",
        "env_medium": "ENVO:00000134",
        "geo_loc_name": "Sweden: Kiruna",
        "habitat": "Thawing permafrost",
        "identifier": "studying carbon transformations",
        "lat_lon": "68.3534 19.0472",
        "location": "from the Arctic",
        "mod_date": "15-MAY-20 10.04.19.473000000 AM",
        "name": "Thawing permafrost microbial communities from the Arctic, studying carbon transformations - Permafrost 712P3D",
        "ncbi_taxonomy_name": "permafrost metagenome",
        "sample_collection_site": "Palsa",
        "specific_ecosystem": "Permafrost",
        "study_description": "A fundamental challenge of microbial environmental science is to understand how earth systems will respond to climate change. A parallel challenge in biology is to unverstand how information encoded in organismal genes manifests as biogeochemical processes at ecosystem-to-global scales. These grand challenges intersect in the need to understand the glocal carbon (C) cycle, which is both mediated by biological processes and a key driver of climate through the greenhouse gases carbon dioxide (CO2) and methane (CH4). A key aspect of these challenges is the C cycle implications of the predicted dramatic shrinkage in northern permafrost in the coming century.",
        "type": "nmdc:Biosample"
    },
```

into:

```json

[
    {
        "id": "gold:Gb0108335",
        "community": "microbial communities",
        "depth": {
            "has_numeric_value": 0.0,
            "has_raw_value": "0.0 m",
            "has_unit": "metre"
        },
        "ecosystem": "Environmental",
        "ecosystem_category": "Terrestrial",
        "ecosystem_subtype": "Wetlands",
        "ecosystem_type": "Soil",
        "elev": {
            "has_numeric_value": 359,
            "has_unit": "meter"
        },
        "env_broad_scale": "ENVO:00000446",
        "env_local_scale": "ENVO:00000489",
        "env_medium": "ENVO:00000134",
        "geo_loc_name": "Sweden: Kiruna",
        "habitat": "Thawing permafrost",
        "identifier": "studying carbon transformations",
        "lat_lon": {
            "latitude": 68.3534,
            "longitude": 19.0472
        },
        "location": "from the Arctic",
        "mod_date": "15-MAY-20 10.04.19.473000000 AM",
        "name": "Thawing permafrost microbial communities from the Arctic, studying carbon transformations - Permafrost 712P3D",
        "ncbi_taxonomy_name": "permafrost metagenome",
        "sample_collection_site": "Palsa",
        "specific_ecosystem": "Permafrost",
        "study_description": "A fundamental challenge of microbial environmental science is to understand how earth systems will respond to climate change. A parallel challenge in biology is to unverstand how information encoded in organismal genes manifests as biogeochemical processes at ecosystem-to-global scales. These grand challenges intersect in the need to understand the glocal carbon (C) cycle, which is both mediated by biological processes and a key driver of climate through the greenhouse gases carbon dioxide (CO2) and methane (CH4). A key aspect of these challenges is the C cycle implications of the predicted dramatic shrinkage in northern permafrost in the coming century.",

```

Differences between input and output:

 * measurement fields are normalized
 * information inferred from lat_lon (currently only `elev`)
 * TODO: ENVO from text mining
 * TODO: annotation sufficiency score
 * TODO: more...

### Validation reports

These are created as report objects, and exported to pandas dataframes for basic statistical aggregation.
See tests for details

Example report:

|description|severity|field|was_repaired|category|
|---|---|---|---|---|
|No package specified|1|||Category.MissingCore|
|No checklist specified|1|||Category.Unclassified|
|Key not underscored: total particulate carbon|1||True|Category.Unclassified|
|Invalid field: id|1|||Category.UnknownField|
|Alias used: total_particulate_carbon => tot_part_carb|1||True|Category.Unclassified|
|Parsed unit-value: 2.0 metre|1|||Category.Unclassified|
|Missing unit 5|1|||Category.Unclassified|
|Skipping geo-checks|0|||Category.Unclassified|


## API Docs

TODO: readthedocs

## Testing

Currently the best way to understand this code is to understand the tests

 * [tests](tests)
     * [inputs](tests/inputs)
         * [test_sample_info.yaml](tests/inputs/test_sample_info.yaml)
   
This contains 'fake' samples that are intended to test validation and repair

## Schema Validation

See the [schema](sample_annotator/model/schema/) folder --
this contains a copy of the LinkML rendering of the MIxS schema
from [mixs-source](https://github.com/cmungall/mixs-source) which will later be integrated by GSC

## Modules

 * [geo](sample_annotator/geolocation)
     - currently this requires a googlemaps API key
     - TODO: rewrite to use ORNL Identify
 * [measurements](sample_annotator/measurements)
     - uses quantulum
     - TODO: use http://units.ontodev.com/
 * [text mining](sample_annotator/text_mining)
     - basic repair
     - NER using fields such as study fields
 * [ontology](sample_annotator/ontology)
     - LinkML enumerations to ontologies


Each module will take care of different aspects

For example, the measurement module will normalized all fields in the schema with range QuantityValue

E.g. Input:

```yaml
sample:
  id: TEST:1
  alt: 2m
  ...
```

Repair Output:

```yaml
sample:
  id: TEST:1
  alt:
    has_numeric_value: 2.0
    has_raw_value: 2m
    has_unit: metre
    ...
```


## Starting the web API

 - TODO: write flask code
