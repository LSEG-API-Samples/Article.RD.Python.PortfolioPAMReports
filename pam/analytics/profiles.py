# Profiles - Analytics
# API operation for running profile analysis for one or multiple portfolios. This API operation does not modify any portfolio data.

from refinitiv.data.delivery import endpoint_request
import pandas as pd

# static endpoint
ENDPOINT = 'https://api.refinitiv.com/user-data/portfolio-management/v1/portfolio-analytics/profiles'

class Profiles:
    def __init__(self, data):
        self.__data = data
        self.__dataframes = {}

        # Dispatch table mapping keys to processing functions
        self.__dispatch_table = {
            'portfolios': pd.DataFrame.from_records,
            'profileAttributes': pd.DataFrame.from_records,
            'longShortBreakDown': pd.DataFrame.from_records,            
            'classifications': self.__process_classifications,
            'securities': pd.DataFrame.from_records,
            'portfolioCentricCompositionSummaries': pd.DataFrame.from_records,
            'portfolioRelativeCompositionSummaries': pd.DataFrame.from_records,
            'breakpoints': pd.DataFrame.from_records,            
            'auditSummaries': pd.DataFrame.from_records,
            'auditSecurityDetails': pd.DataFrame.from_records,
            'auditHoldingsDetails': pd.DataFrame.from_records,
            'auditContributorRICDetails': pd.DataFrame.from_records
        }

    @property
    def data(self):
        return self.__data
        
    @property
    def portfolios(self):
        return self.__get('portfolios')

    @property
    def profileAttributes(self):
        return self.__get('profileAttributes')

    @property
    def longShortBreakDown(self):
        return self.__get('longShortBreakDown')    

    @property
    def classifications(self):
        return self.__get('classifications')

    @property
    def securities(self):
        return self.__get('securities')

    @property
    def portfolioCentricCompositionSummaries(self):
        return self.__get('portfolioCentricCompositionSummaries')

    @property
    def portfolioRelativeCompositionSummaries(self):
        return self.__get('portfolioRelativeCompositionSummaries')

    @property
    def breakpoints(self):
        return self.__get('breakpoints')    
        
    @property
    def auditSummaries(self):
        return self.__get('auditSummaries')

    @property
    def auditSecurityDetails(self):
        return self.__get('auditSecurityDetails')

    @property
    def auditHoldingsDetails(self):
        return self.__get('auditHoldingsDetails')  

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

    def __process_classifications(self, data):
        # Initialize an empty DataFrame
        df = pd.DataFrame()
            
        # Loop through each dictionary in the list
        for d in data:
            # Flatten the 'classificationData' field and add 'classificationCode' as a column
            temp_df = pd.json_normalize(d['classificationData'])
            temp_df['classificationCode'] = d['classificationCode']
                
            # Append the temporary DataFrame to the main DataFrame
            df = df.append(temp_df, ignore_index=True)
            
        # Set 'classificationCode' as the index
        df.set_index('classificationCode', inplace=True)
        return df

def get_profiles(request) -> Profiles:
    """API operation for running profile analysis for one or multiple portfolios
    
    Args:
        Request parameters required for running profile analysis for one or multiple portfolios.  Refer to the API documentation for more details.
    
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
            return Profiles(response.data.raw)
        
        # Throw an exception
        raise Exception(f"HTTP Error. Code: {response.raw.status_code}. Reason: {response.raw.reason_phrase}\n[{response.raw.text}")
        
    except Exception as e:
        raise RuntimeError(f"An error occurred: {str(e)}") from None
