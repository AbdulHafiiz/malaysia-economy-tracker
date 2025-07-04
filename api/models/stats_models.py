import json
from pydantic import BaseModel, Field
from typing import Literal, Optional

with open ('literal_lists.json', 'r') as f:
    config_json = json.loads(f.read())

PREMISE_TYPE_LIST = config_json['PREMISE_TYPE_LIST']
STATE_LIST = config_json['STATE_LIST']
ITEM_GROUP = ['ITEM_GROUP']
ITEM_CATEGORY = ['ITEM_CATEGORY']

class PremiseSearchOptions(BaseModel):
    premise_type: Optional[list[Literal[*PREMISE_TYPE_LIST]]]
    state: Optional[list[Literal[*STATE_LIST]]]
    district: Optional[list[str]] = None
    premise: Optional[list[str]] = None
    address: Optional[list[str]] = None
    limit: Optional[int] = Field(10, ge=1)

class ItemSearchOptions(BaseModel):
    item_code: Optional[list[int]] = None
    item: Optional[list[str]] = None
    item_group: Optional[list[Literal[*ITEM_GROUP]]] = None
    item_category: Optional[list[Literal[*ITEM_CATEGORY]]] = None
    limit: Optional[int] = Field(10, ge=1)