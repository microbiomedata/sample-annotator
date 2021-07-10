# NMDC Sample Annotator API

## Installing

```bash
git clone ...
cd sample-annotator
make test
```

## What is it?

This is a python and flask API for performing annotation of samples from semi-structured or untidy data

The API takes as input a JSON object or dictionary representing a simple sample, where each key is a metadata field

It will attempt to tidy and infer missing data according to a specified schema (currently MIxS)

## Examples

### Measurements normalization

Input:

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

## Command Line

 - TODO: write the command line interface, follow clig.dev

## Starting the web API

 - TODO: write flask code
