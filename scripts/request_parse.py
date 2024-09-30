import json
import pyjq
from collections import OrderedDict, Counter
from pathlib import Path
import shutil
import pickle
from p_tqdm import p_map
import cmipld
from cmipld.utils import DotAccessibleDict

"""
This script processes and standardizes climate model variable data.
It ensures that CF (Climate and Forecast) standard names match those used by CF convention.
The script loads data, processes it, and saves it in a structured format.
"""

async def get_cf_standard_names():
    """
    Load CF standard names from a JSON-LD file hosted on GitHub.
    
    Returns:
        dict: Processed CF standard names data.
    """
    print('Loading CF standard names...')
    cfdata = await cmipld.file_io.gh_read_file('WCRP-CMIP', 'CF', 'standard-name/graph.jsonld')
    frame = {
        "@type": 'cf:standard-name',
        "standard-name:name": "",
        "@explicit": True,
    }
    return cmipld.Frame(cfdata, frame).clean().json

# Load CF standard names
cfnames = cmipld.sync(get_cf_standard_names())
print('\n')

class SubRequest(DotAccessibleDict):
    """A class to represent a sub-request with an ID property."""
    @property
    def id(self):
        return self['@id']

def parse_entry(data, idkey, variable_dir='./variables/', entry='Variable', ldtype='variable'):
    """
    Parse and process variable entries from the input data.
    
    Args:
        data (dict): Input data containing variables and substitutions.
        idkey (str): Key to use as the identifier for each variable.
        variable_dir (str): Directory to save processed variables.
        entry (str): Key in the data dictionary for variables.
        ldtype (str): Type of the linked data.
    
    Returns:
        list: List of processed SubRequest objects.
    """
    # Substitute in linked references
    variables = substitute_json(data, entry)
    
    # Prepare output directory
    variable_dir = Path(variable_dir)
    if variable_dir.exists() and variable_dir.is_dir():
        shutil.rmtree(variable_dir)
    variable_dir.mkdir(parents=True, exist_ok=True)

    def write_variable(args):
        """
        Process and write a single variable to a JSON file.
        
        Args:
            args (tuple): Tuple containing index and variable data.
        
        Returns:
            SubRequest: Processed variable as a SubRequest object.
        """
        i, v = args
        variable = OrderedDict()
        review = OrderedDict()
        priority = OrderedDict()
        rank = OrderedDict()
        
        id = v[idkey].replace(' ', '-').lower()
        
        variable['@id'] = f'dr:{str(variable_dir)}/{id}'
        variable['@type'] = f'dr:{ldtype}'
        
        for key in sorted(v):
            okey = '' + key
            key = key.lower().replace(' ', '-')
            
            if 'review' in key:
                review[key] = v[okey]
            elif 'priority' in key:
                priority[key] = v[okey]
            elif 'rank' in key:
                rank[key] = None if v[okey] == '' else int(v[okey])
            elif 'CF Standard Name' in okey:
                stdnm = v[okey]
                exists = stdnm in cfnames
                variable['cf-standard-name'] = {"@id": f'cf:standard-name/{v[okey]}'}
                variable['cf-standard-name-exists'] = exists
            elif key == 'modeling-realm':
                variable[key] = [{"@id": f'mip-cmor-tables:auxillary/realm/{realm.lower()}'} for realm in v[okey]]
            elif key == 'frequency':
                variable[key] = [{"@id": f'mip-cmor-tables:auxillary/frequency/{freq.lower()}'} for freq in v[okey]]
            else:
                variable[key.replace(' ', "").lower()] = v[okey]
        
        if review:
            variable['review'] = review
        if priority:
            variable['priority'] = priority
        if rank:
            variable['rank'] = rank
        
        with open(variable_dir / f'{id}.json', 'w') as f:
            json.dump(variable, f, indent=4)
        
        return SubRequest(variable)
    
    variable_obj = p_map(write_variable, variables.items())
    
    # with open(variable_dir / 'objects.pkl', 'wb') as f:
    #     pickle.dump(dict(zip([i.entries[idkey.replace(' ', '-').lower()] for i in variable_obj], variable_obj)), f)
    
    return variable_obj

def substitute_json(data, key, othersub='test'):
    """
    Substitute values in the given JSON-like dictionary based on 'rec' fields.

    Args:
        data (dict): The input dictionary containing the data.
        key (str): The key to access the panel data.
        othersub (str, optional): A placeholder for other substitutions. Defaults to 'test'.

    Returns:
        dict: The modified data with substitutions applied.
    """
    substitutions = data['substitutions']
    data = data['panel'][key].T.to_dict()

    assert isinstance(data, dict), 'data must be a dictionary'
    
    # Get references to all the rec fields
    results = pyjq.all(
        'paths(scalars) as $p | select(getpath($p) | type == "string" and startswith("rec")) | {path: $p, value: getpath($p)}',
        data
    )
    
    for result in results:
        # Ensure the substitution exists to avoid KeyError
        subin = substitutions.get(result['value'], othersub)
        
        # Add corrections here
        for idstr in ["Compound Name"]:
            if idstr in subin:
                subin = {"@id": f'dr:variables/{subin[idstr].replace(" ", "-").lower()}'}

        # Navigate to the target location in the data
        dummy = data
        for i in result['path'][:-1]:
            dummy = dummy[i]
        
        # Apply the substitution
        dummy[result['path'][-1]] = subin

    return data
