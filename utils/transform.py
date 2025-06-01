import re

def snake_case(column_name:str) -> str:
    name = re.sub(r"[’'\"()]", "", column_name)
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_").lower()
    return name