# Holdings statements - Analytics
# API operation for getting holdings statements by date for one portfolio ID.

from refinitiv.data.delivery import endpoint_request
import pandas as pd

# static endpoint
ENDPOINT = 'https://api.refinitiv.com/user-data/portfolio-management/v1/portfolio-analytics/{portfolioId}/holdings-statements'

class Holdings:
    def __init__(self, data):
        self.__data = data
        self.__dataframes = {}

        # Dispatch table mapping keys to processing functions
        self.__dispatch_table = {
            'holdingsSummaries': pd.DataFrame.from_records,
            'holdingsDetails': pd.DataFrame.from_records,           
            'bulkStatuses': pd.DataFrame.from_records,
            'auditSecurityDetails': pd.DataFrame.from_records,
            'auditSummaries': pd.DataFrame.from_records,            
            'auditContributorRICDetails': pd.DataFrame.from_records
        }

    @property
    def data(self):
        return self.__data
        
    @property
    def holdingsSummaries(self):
        return self.__get('holdingsSummaries')

    @property
    def holdingsDetails(self):
        return self.__get('holdingsDetails')

    @property
    def bulkStatuses(self):
        return self.__get('bulkStatuses')

    @property
    def auditSecurityDetails(self):
        return self.__get('auditSecurityDetails')

    @property
    def auditSummaries(self):
        return self.__get('auditSummaries')    

    @property
    def auditContributorRICDetails(self):
        return self.__get('auditContributorRICDetails')     

    def __get(self, key):
        # Perform lazy-instantiation
        if key not in self.__dataframes:
            # Dispatch to the correct processing method
            processing_function = self.__dispatch_table.get(key, self.__default_processing)
            self.__dataframes[key] = processing_function(self.__data[key])
            
        return self.__dataframes[key]

    # Default processing function
    def __default_processing(self, data):
        return pd.DataFrame()

def get_holdings_statements(id, request) -> Holdings:
    """API operation for getting holdings statements by date for one portfolio ID.
    
    Args:
        Request details required to calculate holdings for one portfolio ID.  Refer to the API documentation for more details.
    
    Returns:
        pd.DataFrame    
    """    
    
    definition = endpoint_request.Definition(
        method = endpoint_request.RequestMethod.POST,
        url = ENDPOINT,
        path_parameters = {"portfolioId": id},
        body_parameters = request
    )

    # Submit request
    try:
        response = definition.get_data()
        if response.is_success:
            return Holdings(response.data.raw)
        
        # Throw an exception
        raise Exception(f"HTTP Error. Code: {response.raw.status_code}. Reason: {response.raw.reason_phrase}\n[{response.raw.text}")
        
    except Exception as e:
        raise RuntimeError(f"An error occurred: {str(e)}") from None
