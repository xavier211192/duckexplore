from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class TableProperties:
    name: str
    z_order: Optional[List[str]]=field(default_factory=list)
    vacuum:bool = True
    optimize:bool = True
    vacuum_retention_hour:int = 168
    optim_filter:Optional[str]=""

dataset = TableProperties(name="ds1",vacuum=True,optimize=True,z_order:['a','b'])

from delta.tables import *
deltaTable = DeltaTable.forName(spark,dataset.name)
if dataset.z_order:
  deltaTable.optimize().executeZOrderBy(dataset.z_order)
elif dataset.optim_filter:
  deltaTable.optimize().where(dataset.optim_filter).executeCompaction()
