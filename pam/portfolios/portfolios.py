# Portfolios
# API operation for getting a list of portfolios based on portfolio IDs.

from refinitiv.data.delivery import endpoint_request
import pandas as pd

# static endpoint
ENDPOINT = 'https://api.refinitiv.com/user-data/portfolio-management/v1/portfolios'

class Portfolios:
    def __init__(self, data):
        self.__data = data
        self.__dataframes = {}

        # Dispatch table mapping keys to processing functions
        self.__dispatch_table = {
            #'portfolios': self.__process_portfolios,
            'headers': self.__process_headers,
            'statements': self.__process_statements,
            'bulkStatuses': self.__process_bulk_statuses
        }

    @property
    def data(self):
        return self.__data

    @property
    def headers(self):
        return self.__get('headers')
        
    @property
    def statements(self):
        return self.__get('statements')

    @property
    def bulkStatuses(self):
        return self.__get('bulkStatuses')     

    def __get(self, key):
        # Perform lazy-instantiation
        if key not in self.__dataframes:
            # Dispatch to the correct processing method
            processing_function = self.__dispatch_table.get(key, self.__default_processing)
            self.__dataframes[key] = processing_function(key)
            
        return self.__dataframes[key]

    # Default processing function
    def __default_processing(self, key):
        return pd.DataFrame()

    def __process_headers(self, key):
        data = self.__data['portfolios']
        
        # Extract 'portfolioHeader' details
        portfolio_headers = [d['portfolioHeader'] for d in data if 'portfolioHeader' in d]
        
        # Create DataFrame
        df = pd.DataFrame(portfolio_headers)

        # Set 'portfolioId' as the index
        df.set_index('portfolioId', inplace=True)    

        return df

    def __process_statements(self, key):
        data = self.__data['portfolios']

        # normalize the 'holdingsStatementHeaders'
        df_statements = pd.json_normalize(data, record_path=['holdingsStatementHeaders'], meta=[['portfolioHeader', 'portfolioId']])

        return df_statements

    def __process_portfolios(self, data):
        # normalize the 'portfolioHeader'
        df_portfolio = pd.json_normalize([item['portfolioHeader'] for item in data])
        
        # normalize the 'holdingsStatementHeaders'
        df_holdings = pd.json_normalize(data, record_path=['holdingsStatementHeaders'], meta=[['portfolioHeader', 'portfolioId']])
        
        # set 'portfolioId' as the index for easy access
        df_portfolio.set_index('portfolioId', inplace=True)
        df_holdings.set_index('portfolioHeader.portfolioId', inplace=True)
        
        return df_holdings

    def __process_bulk_statuses(self, key):
        data = self.__data['bulkStatuses']

        return pd.DataFrame.from_records(data)

def get_portfolios(ids, startDate=None, endDate=None, includePortfolioLevelAttributes=True,
                   includeDefaultBenchmarkHeader=True, includeCarveOutBasePortfolioHeader=True,
                   traverseCompositePositions=True) -> Portfolios:
    """
    Request for a list of portfolios based on portfolio ID(s) and date range.

    Args:
        ids — The list of portfolio IDs of interest.
        startDate — Start date of the portfolio data retrieval request.
        endDate — End date of the portfolio data retrieval request.        
        includePortfolioLevelAttributes — Indicates whether to include portfolio level attributes.
        includeDefaultBenchmarkHeader — Indicates whether to include a default benchmark header.
        includeCarveOutBasePortfolioHeader — Indicates whether to include carve-out base portfolio header.
        traverseCompositePositions — Indicates whether to traverse composite positions.

    Returns:
        pd.DataFrame
    """  
    
    params = {}
    
    # Include optional parameters
    if isinstance(ids, str):
        ids = [ids]
    params["ids"] = ",".join(ids)
    if startDate is not None:
        params["startDate"] = startDate
    if endDate is not None:
        params["endDate"] = endDate
    if includePortfolioLevelAttributes is not None:
        params["includePortfolioLevelAttributes"] = includePortfolioLevelAttributes
    if includeDefaultBenchmarkHeader is not None:      
        params["includeDefaultBenchmarkHeader"] = includeDefaultBenchmarkHeader       
    if includeCarveOutBasePortfolioHeader is not None:  
        params["includeCarveOutBasePortfolioHeader"] = includeCarveOutBasePortfolioHeader
    if traverseCompositePositions is not None:  
        params["traverseCompositePositions"] = traverseCompositePositions        
    
    # Prepare endpoint definition...
    definition = endpoint_request.Definition(
        url = ENDPOINT,
        query_parameters = params
    )
    
    # Submit request
    try:
        response = definition.get_data()
        if response.is_success:
            return Portfolios(response.data.raw)
        
        # Throw an exception
        raise Exception(f"HTTP Error. Code: {response.raw.status_code}. Reason: {response.raw.reason_phrase}\n[{response.raw.text}")
        
    except Exception as e:
        raise RuntimeError(f"An error occurred: {str(e)}") from None



