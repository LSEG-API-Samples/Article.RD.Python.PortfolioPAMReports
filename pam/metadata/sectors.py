# Classification Sectors metadata
# Retrieve the list of sectors by classification codes.

from refinitiv.data.delivery import endpoint_request
import pandas as pd

# static endpoint
ENDPOINT = 'https://api.refinitiv.com/user-data/portfolio-management/v1/metadata/classification-sectors'

def get_classification_sectors(classification_codes="MAJOR_ASSET_CLASS") -> pd.DataFrame:
    """
    Retrieve the list of sectors by classification code:

    Args:
        classification_codes (str, list of str): Classification codes to retrieve sectors for. Defaults to 'MAJOR_ASSET_CLASS'.

    Returns:
        pd.DataFrame
    """  
    
    params = {}
    
    # Include optional parameters       
    if classification_codes:
        if isinstance(classification_codes, str):
            classification_codes = [classification_codes]   
        params["classificationCodes"] = ",".join(classification_codes)
    
    # Prepare endpoint definition...
    definition = endpoint_request.Definition(
        url = ENDPOINT,
        query_parameters = params
    )

    # Submit request
    try:
        response = definition.get_data()
        if response.is_success:
            # Initialize an empty DataFrame
            df = pd.DataFrame()
            
            # Loop through each dictionary in the list
            for d in response.data.raw['classificationSectors']:
                # Flatten the 'classificationData' field and add 'classificationCode' as a column
                temp_df = pd.json_normalize(d['sectors'])
                temp_df['classificationCode'] = d['classificationCode']
                
                # Append the temporary DataFrame to the main DataFrame
                df = df.append(temp_df, ignore_index=True)
            
            # Set 'classificationCode' as the index
            df.set_index('classificationCode', inplace=True)
            return df
        
        # Throw an exception
        raise Exception(f"HTTP Error. Code: {response.raw.status_code}. Reason: {response.raw.reason_phrase}\n[{response.raw.text}")
        
    except Exception as e:
        raise RuntimeError(f"An error occurred: {str(e)}") from None
