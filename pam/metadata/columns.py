# Data Columns metadata
# Retrieve the list of data columns available as input options in analyses requests.

from refinitiv.data.delivery import endpoint_request
import pandas as pd

# static endpoint
ENDPOINT = 'https://api.refinitiv.com/user-data/portfolio-management/v1/metadata/data-columns'

def get_data_columns() -> pd.DataFrame:
    """Retrieve the list of data columns available as input options in analyses requests.
    
    Returns:
        pd.DataFrame    
    """    
    definition = endpoint_request.Definition(ENDPOINT)

    # Submit request
    try:
        response = definition.get_data()
        if response.is_success:
            return pd.DataFrame.from_records(response.data.raw['dataColumns'])
        
        # Throw an exception
        raise Exception(f"HTTP Error. Code: {response.raw.status_code}. Reason: {response.raw.reason_phrase}\n[{response.raw.text}")
        
    except Exception as e:
        raise RuntimeError(f"An error occurred: {str(e)}") from None