# Return Statistics - Analytics
# API operation for calculating MPT (Modern Portfolio Theory) statistics for one or multiple portfolios.

from refinitiv.data.delivery import endpoint_request
import pandas as pd

# static endpoint
ENDPOINT = 'https://api.refinitiv.com/user-data/portfolio-management/v1/portfolio-analytics/return-statistics'

class Returns:
    def __init__(self, data):
        self.__data = data
        self.__dataframes = {}

        # Dispatch table mapping keys to processing functions
        self.__dispatch_table = {
            'portfolios': pd.DataFrame.from_records,
            'mptStatisticsData': pd.DataFrame.from_records,           
            'auditSummaries': pd.DataFrame.from_records,
            'auditHoldingsDetails': pd.DataFrame.from_records,
            'auditSecurityDetails': pd.DataFrame.from_records,            
            'auditContributorRICDetails': pd.DataFrame.from_records
        }

    @property
    def data(self):
        return self.__data
        
    @property
    def portfolios(self):
        return self.__get('portfolios')

    @property
    def mptStatisticsData(self):
        return self.__get('mptStatisticsData')   
        
    @property
    def auditSummaries(self):
        return self.__get('auditSummaries')

    @property
    def auditHoldingsDetails(self):
        return self.__get('auditHoldingsDetails')      

    @property
    def auditSecurityDetails(self):
        return self.__get('auditSecurityDetails')

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

def get_return_statistics(request) -> Returns:
    """API operation for calculating MPT (Modern Portfolio Theory) statistics for one or multiple portfolios.
    
    Args:
        Request details required to calculate statistics for one or multiple portfolios.  Refer to the API documentation for more details.
    
    Returns:
        pd.DataFrame    
    """    
    
    definition = endpoint_request.Definition(
        method = endpoint_request.RequestMethod.POST,
        url = ENDPOINT,
        body_parameters = request
    )

    # Submit request
    try:
        response = definition.get_data()
        if response.is_success:
            return Returns(response.data.raw)
        
        # Throw an exception
        raise Exception(f"HTTP Error. Code: {response.raw.status_code}. Reason: {response.raw.reason_phrase}\n[{response.raw.text}")
        
    except Exception as e:
        raise RuntimeError(f"An error occurred: {str(e)}") from None
