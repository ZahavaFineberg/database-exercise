import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Type

#from dataclasses_json import dataclass_json

DB_ROOT = Path('db_files')


#@dataclass_json
@dataclass
class DBField:
    name: str  # "id"
    type: Type  # int

    def __init__(self, name, type):
        self.name = name
        self.type = type


#@dataclass_json
@dataclass
class SelectionCriteria:
    field_name: str
    operator: str
    value: Any


#@dataclass_json
@dataclass
class DBTable:  # sick
    name: str  # sick
    fields: List[DBField]  # [id, name, is_sympomatic...]
    key_field_name: str  # "id"

    def __init__(self, name, fields, key_field_name):
        self.name = name
        self.fields = [x for x in fields if x.name != key_field_name]
        with open(f"{self.name}.json", "w") as the_file:
            json.dump({}, the_file)
        for field in fields:
            if key_field_name == field.name:
                self.key_field_name = key_field_name
        else:
            pass#raise NotImplementedError  # Primary key Error...

    # dict_id = {
    #     12: ["Zahava", False],
    #     97: ["Shlomo", False]
    # }  JSON

    def count(self) -> int:
        with open(f"{self.name}.json", 'r') as json_file:
            table = json.load(json_file)
        return len(DB_tables)

    def insert_record(self, values: Dict[str, Any]) -> None:
        with open(f"{self.name}.json", "r") as json_file:
            records = json.load(json_file)
            if str(values[self.key_field_name]) not in records:
                records[values[self.key_field_name]] = [values.get(key.name) for key in self.fields]
                write(records, f"{self.name}.json")
            else:
                raise NotImplementedError

    def delete_record(self, key: Any) -> None:
        with open(f"{self.name}.json", 'r') as json_file:
            table = json.load(json_file)
            del table[key]
            write(table, f"{self.name}.json")
    #
    # def delete_records(self, criteria: List[SelectionCriteria]) -> None:
    #     raise NotImplementedError
    #
    def get_record(self, key: Any) -> Dict[str, Any]:
        with open(f"{self.name}.json", 'r') as json_file:
            table = json.load(json_file)
            record = {}
            record[self.key_field_name] = key
            for y, field in enumerate(self.fields):
                record[field.name] = table[key][y]
            return record

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        with open(f"{self.name}.json", "r") as json_file:
            records = json.load(json_file)
            if key in records:
                records[key] = [values.get(key) for key in self.fields]
                write(records, f"{self.name}.json")
            else:
                raise NotImplementedError
#
    # def query_table(self, criteria: List[SelectionCriteria]) \
    #         -> List[Dict[str, Any]]:
    #     raise NotImplementedError
    #
    # def create_index(self, field_to_index: str) -> None:
    #     raise NotImplementedError / TODO


#@dataclass_json
@dataclass
class DataBase:
    # Put here any instance information needed to support the API
    # [DBTable1, DBTable2, DBTable3...]  CSV
    def __init__(self, DB_name):
        self.name = DB_name
        with open(f"{self.name}.json", "w") as the_file:
            json.dump({}, the_file)
#[[DBTable, json],[DBTable, json], ....]
    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str):
        new_table = DBTable(table_name, fields, key_field_name)
        with open(f"{self.name}.json", 'r') as json_file:
            DB_tables = json.load(json_file)
            DB_tables[table_name] = f"{table_name}.json"
            write(DB_tables, f"{self.name}.json")
        return new_table

    def num_tables(self) -> int:
        with open(f"{self.name}.json", 'r') as json_file:
            DB_tables = json.load(json_file)
        return len(DB_tables)

    def get_table(self, table_name: str) -> DBTable:
        pass

    def delete_table(self, table_name: str) -> None:
        with open(f"{self.name}.json", 'r') as json_file:
            DB_tables = json.load(json_file)
            del DB_tables[table_name]
            write(DB_tables, f"{self.name}.json")

    def get_tables_names(self) -> List[Any]:
        with open(f"{self.name}.json", 'r') as json_file:
            DB_tables = json.load(json_file)
            table_names = [x for x in DB_tables.keys()]
        return table_names

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError  #TODO

def write(data, file_name):
    with open(file_name, 'w') as json_file:
        json.dump(data, json_file)


my_db = DataBase("Oppen")
Person = my_db.create_table("Person", [DBField("id", int), DBField("name", str), DBField("address", str)], "id")
Doctor = my_db.create_table("Doctor", [DBField("id", int), DBField("name", str), DBField("clinic", str)], "id")
# my_db.delete_table("Person")
# my_db.create_table("Person", [DBField("id", int), DBField("name", str), DBField("address", str)], "id")
print(my_db.get_tables_names())
print(my_db.num_tables())
Person.insert_record({"name": "Zahava", "id": 1, "address": "Jerusalem"})
Person.insert_record({"name": "Shlomo", "id": 2, "address": "Jerusalem"})
print(Person.get_record('2'))