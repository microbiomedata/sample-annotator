#!/usr/bin/env python3

import yaml
import json
import sys

with open(sys.argv[1]) as s:
    obj = yaml.safe_load(s)
print(json.dumps(obj, indent=4, sort_keys=True))
