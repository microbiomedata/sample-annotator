# NMDC Sample Annotator API

The API is a September deliverable for NMDC. It is intended as something that can be generally applicable across multiple projects.

[https://github.com/microbiomedata/nmdc-metadata/issues/330](https://github.com/microbiomedata/nmdc-metadata/issues/330)

It takes as input one or more samples (possibly augmented by studies). Each sample is represented as a simple key-value list of attributes, ideally conforming to MIxS (id, lat-long, depth, env_biome, soil_type, etc). Note this makes it easy to take simple TSVs where rows are samples and columns are attributes.

It will perform a variety of processing on the inputs. The framework is extensible, so we can add more functionality over time. The processing includes:



* Checking to make sure columns are in MIxS, and suggesting column renames to make MIxS compliant
* Checking to make sure column values conform to the MIxS LinkML representation (e.g. measurement datamodel, regexes, enums)
* Normalize package/checklist
* Ensure required fields for a package and checklist are present
* Ensure that fields restricted to a package or checklist are not used inappropriately
* Checking to make sure any ontology terms used are still valid and not obsolete
* Suggesting repairs of column values, including
    * Normalization of ontology columns such as ENVO triad
    * Using replaced_by metadata for obsolete classes
    * Mapping to missing value vocabulary
    * Generic normalization of all measurement fields (e.g. s/(\d+)(UNIT)/$1 $2/)
    * Specific normalization where we have curated patterns, e.g. salinity
* Detecting potential duplicates
* Perform NER on narrative fields (sample description and study description) and other fields to suggest
    * Refinements or improvements on ENVO triad
    * Suggestions for specific fields, e.g fao_soil_class
* Use basic statistical rules mined from INSDC samples to spot outliers
    * E.g. high salinity on a freshwater lake samples
* Normalization geolocation field
* Perform lookup on geolocation databases (Wikidata, ORNL Identify) to suggest values for specific MIxS fields
    * e.g. elevation is easy and in many databases
    * fao_soil_class/ENVO from lookup in soil database
    * [NOTE: we can use Stan’s code as a basis here]

We could choose to represent the above using an explicit system like GORULES

The return will be a JSON object that includes both reporting info (think OBO Dashboard/ GO Rules) and repair suggestions, and a normalized version of the object that conforms to the NMDC/MIxS schema


## Implementation:

The API will be a thin layer of flask/fastapi on top of a Python library. The Python lib will be released separately on PyPI and should be usable e.g. to quickly iterate through all rows in a biosample sqlite database and suggesting repairs, producing aggregated reports.

The input data model will deliberately be laxer than the NMDC schema; part of the point here is to take non-conformant data, report on nonconformances, and suggest repairs. Minimally the input will be a list of samples ([sample_set](https://microbiomedata.github.io/nmdc-schema/biosample_set/)), where each sample is a flat dict. The output will be richer and more conformant. Some dict values may be expanded to objects (e.g. a string “2 cm” into a [QuantityValue](https://microbiomedata.github.io/nmdc-schema/QuantityValue) object). We will also use PROV as the simple data model for provenance. E.g. we will model operations such as performing checks on a sample as a prov activity, and have different “agents” such as ontology ID checker. We can provide start/end etc. Each inferred value will link back to a prov activity instance.

We will iterate on this. The very first version will have limited processing but will demonstrate end to end processing. The idea is that anyone can contribute a processing component, they would just write their python in such a way that it takes a standard input, e.g the sample tag/value dict, and produces a standard output. Multiple processors can be chained like a production line. Each should be reasonably fast and we don’t need to worry about fancy infrastructure here



* Core python lib: Bill, Chris
* Flask/fastapi layer: Bill
* Quantitative value normalization components: Marcin
* NER components: Mark and Harshad
* Geolocation components: TBD, we may be able to get ORNL but we should get framework first, Mark can help
* NMDC Requirements and priority: Mark
* KBase requirements: Marcin
* Extend nmdc schema to include sample enhancement workflow: Chris, Bill


### Examples

Input:

Sample_set:

{ id: GbNNNN,

lat: 12,

long: 23,

description: soil sample from volcano,

package: …,

checklist: …,

depth: “1cm” }

⇒

Activity_set: { ## conforms to [https://microbiomedata.github.io/nmdc-schema/Activity/](https://microbiomedata.github.io/nmdc-schema/Activity/)

a1234: {

    Start_at: 2021-06-01,

    Name: “Mark and Harshad’s NER”,

    was_generated_by : ...

    description: “NER using ENVO v2021-01-01””

}

[sample_set](https://microbiomedata.github.io/nmdc-schema/biosample_set/):

{id: gold:GbNNN,

Lat_long: “12’ 23’”

fao_soil_class: “volcanosoil”

Env_material:

    {term_name: “soil”

     Term_id: ENVO:nnnn

      Was_generated_by: a1234

     }

Env_biome: “”

Summary:

{

Annotation_completion_score: 0.4

}

Issues:[

Id: “was unprefixed”

Lat_long: “was repaired from two separate lat and long fields”

Env_biome_missing: ….

Depth: “did not match mixs regex”



]

Start with depth OR temperature, common across number of packages

Chris suggestion: check ALL slots that are QuantityValue, check against regex /\d+[\.\d+] \s+/

DO NOT try and further parse YET

Input:

Sample_set:

{ id: igsn:12345, ## pretend this a typo

lat: 12,

long: 23,

description: soil sample from volcano,

Depth: “1cm” }

Issues:

[bad_id: “no such ID igsn:12345”}


### Running en-masse over a database

It should be possible to run the annotator over a database one row at a time, but this may be inefficient

When validating or enhancing a database, it is more efficient to use SQL. E.g. for checking enums, a SQL query can be constructed that finds all string values not in the enum



## Starting the web API

- TODO: write flask code


## 2021-07-07

Name API package

Create new repo and move in relevant code from biosample_analysis and other

Chris suggestion:



* Repo name: sample-annotator
* Org: INCATools (for now)
