import re
from ast import literal_eval
from pydantic import BaseModel
from collections import defaultdict

DATATYPE_MATCHING = {
    'int': 'INTEGER',
    'str': 'STRING',
    'datetime.date': 'TIMESTAMP',
}

def infer_model_datatypes(model: BaseModel):
    datatype_dict = defaultdict(list)
    for name, datatype in model.model_fields.items():
        field_annotation = str(datatype.annotation)
        print(name, field_annotation, sep=': ')
        if arr_type := re.search(r'(list|tuple)(?=\[.*?\])', field_annotation):
            datatype_dict[name].append(arr_type.group(0))

        if data_type := re.search(r'(?<=\[)[\w\.]*?(?=\])', field_annotation):
            datatype_dict[name].append(DATATYPE_MATCHING.get(data_type.group(0), 'NULL'))

        if literal_type := re.search(r'(?<=typing\.Literal)\[.*?\]', field_annotation):
            datatype_dict[name].extend([*{
                DATATYPE_MATCHING.get(type(obj).__name__, 'NULL')
                for obj in literal_eval(literal_type.group(0))
            }])
    return datatype_dict


if __name__ == '__main__':
    pass