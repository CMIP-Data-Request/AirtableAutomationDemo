
from pathlib import Path
from typing import Dict, Any, List, Tuple
import pandas as pd
import pyairtable
from pyairtable import Api
from tqdm import tqdm
import pickle,shutil

FROMBASE = False

def nstr(value: Any) -> str:
    """
    Convert the given value to a string.
    If the value is None, return an empty string.

    Args:
        value (Any): The value to be converted to a string.

    Returns:
        str: The string representation of the input value.
    """
    return str(value) if value is not None else ''
  
class LoadBase:
    """
    Loads Airtable bases into a dictionary object.
    """

    def __init__(self, apikey: pyairtable.api.base.Base) -> None:
        """
        Initialize the LoadBase object.

        Args:
            api (pyairtable.Api): The Airtable API object.
        """
        self.tables: Dict[str, Dict[str, Any]] = {}
        self.table_objs: Dict[str, pyairtable.models.Table] = {}
        self.api = Api(apikey)
        self.shadow_table_names = ['tblQcdKgPGU0jFq1b', 'tbl7L210y9LFpFI7b']
      
    @property
    def bases(self) -> List[pyairtable.api.base.Base]:
        """
        Get the Airtable bases.

        Returns:
            List[pyairtable.models.Base]: The Airtable bases.
        """
        print(self.api)
        return self.api.bases()  
    
    def load(self, base) -> None:
        """
        Load the records from all tables in the given Airtable base.

        Args:
            base (pyairtable.models.Base): The Airtable base to load.
        """
        for table in base.tables():
            print(f'\tReading {base.name} {table.name}')
            records = {record['id']: record['fields'] for record in table.all()}
            table_name = f'__{table.name}__' if table.id in self.shadow_table_names else table.name
            self.tables[table_name] = {
                'id': table.id,
                'name': table.name,
                'description': nstr(table.schema().description),
                'base_id': base.id,
                'base_name': base.name,
                'records': records
            }
            self.table_objs[table_name] = table

    def dump(self, temp_dir = './info_dump/') -> None:
        """
        Dump the loaded Airtable data to CSV, JSON, and pickle files.

        Args:
            temp_dir (str, optional): The directory to save the data files. Defaults to './info_dump/'.
        """

        # Path to the temporary directory
        temp_dir = Path(temp_dir)
        self.temp_dir = temp_dir

        # If the directory exists, delete all its contents
        if temp_dir.exists() and temp_dir.is_dir():
            shutil.rmtree(temp_dir)

        # Recreate the directory
        temp_dir.mkdir(parents=True, exist_ok=True)

        panel: Dict[str, pd.DataFrame] = {}
        substitutions: Dict[str, Dict[str, Any]] = {}
        skip: Dict[str, List[str]] = {}

        for table in tqdm(self.tables.values()):
            name = table['name']
            
            try:
              skip[name] = [field.name for field in self.table_objs[name]._schema.fields
                  if isinstance(field, pyairtable.models.schema.MultipleRecordLinksFieldSchema)]
            except Exception as e:
              print(f"Error Skipping: {e}, this is likely defined with underscores around it. ")
              skip[name] = []

            # Convert records to DataFrame
            df = pd.DataFrame.from_dict(table['records'], orient='index').fillna('')
            
            if name != 'Variable' or name != 'Variables':
                df.drop(skip[name], axis=1, inplace=True, errors='ignore')

            # Save to CSV and JSON
            (temp_dir / f"{name}.csv").write_text(df.to_csv(index=True))
            (temp_dir / f"{name}.json").write_text(df.to_json(orient='records', indent=4))

            panel[name] = df
            substitutions.update(df.T.to_dict())

        # Save substitutions to pickle
        with open(temp_dir / 'substitutions.pkl', 'wb') as f:
            pickle.dump(dict(panel=panel, substitutions=substitutions), f)

        print(f'\nData dumped to ./{temp_dir}/\n')
        print('-' * 8, 'cyclical definitions', '-' * 8)
        for table_name, skipped_fields in skip.items():
            print(table_name, skipped_fields)
            
    @property
    def pkldump(self) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Dict[str, Any]]]:
        """
        Read the saved pickle file.

        Returns:
            Tuple[Dict[str, pd.DataFrame], Dict[str, Dict[str, Any]]]: The panel and substitutions.
        """
        with open(self.temp_dir / 'substitutions.pkl', 'rb') as f:
            data = pickle.load(f)
        return data
