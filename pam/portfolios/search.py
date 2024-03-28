# Portfolios - Search
# API operation for getting a list of portfolio headers based on request query options.

from refinitiv.data.delivery import endpoint_request
import pandas as pd

# static endpoint
ENDPOINT = 'https://api.refinitiv.com/user-data/portfolio-management/v1/portfolios/search'

def search(portfolioTypes=None, query=None, queryField=None, queryCondition=None, userSources=None, sort=None, 
           maxCount=None, includeDefaultBenchmarkHeader=True) -> pd.DataFrame:
    """
    Request for a list of portfolio headers based on request query options.

    Args:
        portfolioTypes — List of portfolio types: 'FundedPortfolio', 'WatchList', etc.  See docs for list.
        query — The search query string.        
        queryField — The query field for filtering portfolios.
        queryCondition — The query condition for filtering portfolios.
        userSources — List of user sources for the portfolio search query. Available user sources: CurrentUser, OtherUsers.        
        sort — The sort field for sorting the search results.
        maxCount — The maximum number of portfolios to be included in the search results.
        includeDefaultBenchmarkHeader - Indicates whether to include the default benchmark header in the search results.

    Returns:
        pd.DataFrame
    """  

    params = {} 

    # Include optional parameters
    if portfolioTypes is not None:
        if isinstance(portfolioTypes, str):
            portfolioTypes = [portfolioTypes]
        params["portfolioTypes"] = ",".join(portfolioTypes)
    if query is not None:
        params["query"] = query
    if queryField is not None:
        params["queryField"] = queryField
    if queryCondition is not None:
        params["queryCondition"] = queryCondition
    if userSources is not None:
        if isinstance(userSources, str):
            userSources = [userSources]
        params["userSources"] = ",".join(userSources)
    if sort is not None:
        params["sort"] = sort
    if maxCount is not None:
        params["maximumCount"] = maxCount
    if includeDefaultBenchmarkHeader is not None:
        params["includeDefaultBenchmarkHeader"] = includeDefaultBenchmarkHeader        
    
    # Prepare endpoint definition...
    definition = endpoint_request.Definition(
        url = ENDPOINT,
        query_parameters = params
    )
    
    # Submit request
    try:
        response = definition.get_data()
        if response.is_success:
            return pd.DataFrame.from_records(response.data.raw['portfolioHeaders'])
        
        # Throw an exception
        raise Exception(f"HTTP Error. Code: {response.raw.status_code}. Reason: {response.raw.reason_phrase}\n[{response.raw.text}")
        
    except Exception as e:
        raise RuntimeError(f"An error occurred: {str(e)}") from None



