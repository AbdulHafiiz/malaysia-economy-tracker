import json
from pathlib import Path
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Literal, Optional, Union

ROOT_DIR = Path(__file__).parents[2]
with open (ROOT_DIR / 'api/models/literal_lists.json', 'r') as f:
    config_json = json.loads(f.read())

PREMISE_TYPE_LIST = config_json['PREMISE_TYPE_LIST']
STATE_LIST = config_json['STATE_LIST']
ITEM_GROUP = config_json['ITEM_GROUP']
ITEM_CATEGORY = config_json['ITEM_CATEGORY']

class PremiseSearchOptions(BaseModel):
    premise_code: Optional[list[int]] = None
    premise_type: Optional[list[Literal[*PREMISE_TYPE_LIST]]] = None
    state: Optional[list[Literal[*STATE_LIST]]] = None
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

class PricecatcherStatsSearch(BaseModel):
    month_start: Optional[list[datetime]] = [datetime.now().date().strftime('%Y-%m-01')]
    premise_type: Optional[list[Literal[*PREMISE_TYPE_LIST]]] = None
    state: Optional[list[Literal[*STATE_LIST]]] = None
    district: Optional[list[str]] = None
    item_code: Optional[list[int]] = None
    item_name: Optional[list[str]] = None
    min_price: tuple[Optional[int], Optional[int]] = None
    max_price: tuple[Optional[int], Optional[int]] = None
    mean_price: tuple[Optional[int], Optional[int]] = None
    limit: Optional[int] = Field(10, ge=1)