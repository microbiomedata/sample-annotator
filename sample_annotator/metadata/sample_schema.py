from linkml.generators.yamlgen import load_raw_schema, YAMLGenerator
from sample_annotator import MIXS_SCHEMA
import yaml
import json

from datetime import datetime
from typing import Optional, List, Set, Any, Dict
from dataclasses import dataclass
import logging

def underscore(t: str) -> str:
    return t.replace(' ', '_')

@dataclass
class SampleSchema:
    """
    This provides a wrapper on top of a LinkML schema instance, where the contents are a MIxS schema or similar
    """

    object: Dict = None
    slot_dict_by_alias = None

    def load(self, force=False) -> Dict:
        """
        Load the schema from config folded
        """
        if self.object and not force:
            return self.object
        with open(MIXS_SCHEMA) as stream:
            self.object = json.load(stream)
            return self.object

    def slotdict(self) -> Dict:
        return self.load()['slots']

    def enumdict(self) -> Dict:
        return self.load()['enums']

    def get_slot(self, name: str, class_name: str = None, use_aliases=False) -> Dict:
        """
        Return a slot object by name

        If use_aliases is True, then also look at aliases
        """
        name = underscore(name)
        sd = self.slotdict()
        if name in sd:
            return sd[name]
        if use_aliases:
            for s in sd.values():
                if name in [underscore(a) for a in s.get('aliases', [])]:
                    return s
        return None

    def is_measurement_field(self, k: str) -> bool:
        if k == 'depth':
            # this is a bug in the mixs spec - depth SHOULD be specified as QC
            return True
        range = self.get_range(k)
        if range is not None and range == 'quantity value':
            return True
        else:
            return False

    def get_enumerations(self, k: str) -> Optional[Dict]:
        range = self.get_range(k)
        return self.enumdict().get(range, None)


    def get_range(self, k) -> str:
        slot = self.get_slot(k)
        if slot is not None:
            return slot.get('range', None)



