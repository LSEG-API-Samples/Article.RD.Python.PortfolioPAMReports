# Attributes metadata
# Retrieve a list of attributes based on specified criteria.

from refinitiv.data.delivery import endpoint_request
import pandas as pd

# static endpoint
ENDPOINT = 'https://api.refinitiv.com/user-data/portfolio-management/v1/metadata/attributes'

def get_attributes(include_portfolio_attributes=None, data_owner_type=None, 
                   attribute_types=None, attribute_data_types=None) -> pd.DataFrame:
    """
    Request for a list of attributes based on the optional criteria:

    Args:
        include_portfolio_attributes (bool, optional): Include portfolio-level attributes. Defaults to None.
        data_owner_type (str, optional): Type of owner (e.g., "All"). Defaults to None.
        attribute_types (str or list of str, optional): List of attribute types (e.g., ["Price", "CashRate", "Classification"]). Defaults to None.
        attribute_data_types (str or list of str, optional): List of attribute data types (e.g., ["Sector", "Identifier", "Money", "Number", "Date"]). Defaults to None.

    Returns:
        pd.DataFrame
    """  
    
    params = {}
    
    # Include optional parameters
    if include_portfolio_attributes is not None:
        params["portfolioAttributes"] = include_portfolio_attributes
    if data_owner_type:
        params["dataOwnerType"] = data_owner_type
    if attribute_types:
        if isinstance(attribute_types, str):
            attribute_types = [attribute_types]       
        params["attributeTypes"] = ",".join(attribute_types)
        
    if attribute_data_types:
        if isinstance(attribute_data_types, str):
            attribute_data_types = [attribute_data_types]   
        params["attributeDataTypes"] = ",".join(attribute_data_types)
    
    # Prepare endpoint definition...
    definition = endpoint_request.Definition(
        url = ENDPOINT,
        query_parameters = params
    )
    
    # Submit request
    try:
        response = definition.get_data()
        if response.is_success:
            return pd.DataFrame.from_records(response.data.raw['attributes'])
        
        # Throw an exception
        raise Exception(f"HTTP Error. Code: {response.raw.status_code}. Reason: {response.raw.reason_phrase}\n[{response.raw.text}")
        
    except Exception as e:
        raise RuntimeError(f"An error occurred: {str(e)}") from None
