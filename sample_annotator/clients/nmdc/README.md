### Introduction

The files in this `nmdc` folder essentially serve as a wrapper around the GOLD API client, [gold_client.py](../gold_client.py) for transforming data fetched from the GOLD Database into NMDC compliant JSON.

### File Structure

```
├── gold_client_wrapper.py  # wrapper responsible for transformation
│
├── gold_paired_end.py  # post processing on paired end data from read QC analysis data
│
├── input
│   ├── EMP_soil_readQC.json    # file to compute value for has_input property
│   ├── EMP500_paired_end_summary_sheet1.tsv    # paired end summary sheet1
│   ├── EMP500_paired_end_summary_sheet2.tsv    # paired end summary sheet2
│   └── soil_ids.txt    # file with GOLD IDs of only soil samples
│
└── output
    ├── EMP500_paired_end_merged_sheet1.csv # result of running gold_paired_end.py on EMP_paired_end_summary_sheet1
    │
    ├── EMP500_paired_end_merged_sheet2.csv # result of running gold_paired_end.py on EMP_paired_end_summary_sheet2
    │
    └── gold_nmdc.json  # result of running gold_client_wrapper.py on data in GOLD
```
