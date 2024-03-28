# Identifiers metadata
# Retrieve a list of available identifiers for tickers.

from refinitiv.data.delivery import endpoint_request
import pandas as pd

# static endpoint
ENDPOINT = 'https://api.refinitiv.com/user-data/portfolio-management/v1/metadata/identifiers'

def get_identifiers() -> pd.DataFrame:
    """Retrieve a list of available identifiers for tickers
    
    Returns:
        pd.DataFrame    
    """    
    definition = endpoint_request.Definition(ENDPOINT)

    # Submit request
    try:
        response = definition.get_data()
        if response.is_success:
            return pd.DataFrame.from_records(response.data.raw['identifiers'])
        
        # Throw an exception
        raise Exception(f"HTTP Error. Code: {response.raw.status_code}. Reason: {response.raw.reason_phrase}\n[{response.raw.text}")     
        
    except Exception as e:
        raise RuntimeError(f"An error occurred: {str(e)}") from None