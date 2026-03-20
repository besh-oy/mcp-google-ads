from typing import Any, Dict, List, Optional, Union
from pydantic import Field
import os
import json
import base64
import requests
from datetime import datetime, timedelta
from pathlib import Path

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google.auth.exceptions import RefreshError
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
import logging

# MCP
from mcp.server.fastmcp import FastMCP

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('google_ads_server')

mcp = FastMCP(
    "google-ads-server",
    dependencies=[
        "google-auth-oauthlib",
        "google-auth",
        "requests",
        "python-dotenv"
    ]
)

# Constants and configuration
SCOPES = ['https://www.googleapis.com/auth/adwords']
API_VERSION = "v23"  # Google Ads API version

# Load environment variables
try:
    from dotenv import load_dotenv
    # Load from .env file if it exists
    load_dotenv()
    logger.info("Environment variables loaded from .env file")
except ImportError:
    logger.warning("python-dotenv not installed, skipping .env file loading")

# Get credentials from environment variables
GOOGLE_ADS_DEVELOPER_TOKEN = os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")

def format_customer_id(customer_id: str) -> str:
    """Format customer ID to ensure it's 10 digits without dashes."""
    # Convert to string if passed as integer or another type
    customer_id = str(customer_id)
    
    # Remove any quotes surrounding the customer_id (both escaped and unescaped)
    customer_id = customer_id.replace('\"', '').replace('"', '')
    
    # Remove any non-digit characters (including dashes, braces, etc.)
    customer_id = ''.join(char for char in customer_id if char.isdigit())
    
    # Ensure it's 10 digits with leading zeros if needed
    return customer_id.zfill(10)

def get_credentials():
    """Build OAuth credentials directly from environment variables and refresh the access token."""
    client_id = os.environ.get("GOOGLE_ADS_CLIENT_ID")
    client_secret = os.environ.get("GOOGLE_ADS_CLIENT_SECRET")
    refresh_token = os.environ.get("GOOGLE_ADS_REFRESH_TOKEN")

    missing = [name for name, val in [
        ("GOOGLE_ADS_CLIENT_ID", client_id),
        ("GOOGLE_ADS_CLIENT_SECRET", client_secret),
        ("GOOGLE_ADS_REFRESH_TOKEN", refresh_token),
    ] if not val]

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
    )
    try:
        creds.refresh(Request())
    except RefreshError as e:
        raise ValueError(
            "OAuth refresh token has expired or been revoked. "
            "Re-run get_refresh_token.py to generate a new one and update GOOGLE_ADS_REFRESH_TOKEN."
        ) from e
    logger.info("OAuth credentials loaded and refreshed from environment variables")
    return creds

def get_headers(creds):
    """Get headers for Google Ads API requests."""
    if not GOOGLE_ADS_DEVELOPER_TOKEN:
        raise ValueError("GOOGLE_ADS_DEVELOPER_TOKEN environment variable not set")

    if not creds.valid:
        if creds.refresh_token:
            try:
                logger.info("Refreshing expired OAuth token in get_headers")
                creds.refresh(Request())
                logger.info("Token successfully refreshed in get_headers")
            except RefreshError as e:
                logger.error(f"Error refreshing token in get_headers: {str(e)}")
                raise ValueError(f"Failed to refresh OAuth token: {str(e)}")
        else:
            raise ValueError("OAuth credentials are invalid and cannot be refreshed")

    token = creds.token
        
    headers = {
        'Authorization': f'Bearer {token}',
        'developer-token': GOOGLE_ADS_DEVELOPER_TOKEN,
        'content-type': 'application/json'
    }
    
    if GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        headers['login-customer-id'] = format_customer_id(GOOGLE_ADS_LOGIN_CUSTOMER_ID)

    return headers

def get_google_ads_client() -> GoogleAdsClient:
    """Build a GoogleAdsClient from the same env vars used by the REST tools."""
    config = {
        "developer_token": os.environ.get("GOOGLE_ADS_DEVELOPER_TOKEN"),
        "client_id": os.environ.get("GOOGLE_ADS_CLIENT_ID"),
        "client_secret": os.environ.get("GOOGLE_ADS_CLIENT_SECRET"),
        "refresh_token": os.environ.get("GOOGLE_ADS_REFRESH_TOKEN"),
        "use_proto_plus": True,
    }
    login_customer_id = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID")
    if login_customer_id:
        config["login_customer_id"] = format_customer_id(login_customer_id)

    missing = [k for k, v in config.items() if v is None and k != "login_customer_id"]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return GoogleAdsClient.load_from_dict(config)

@mcp.tool()
async def list_accounts() -> str:
    """
    Lists all accessible Google Ads accounts.
    
    This is typically the first command you should run to identify which accounts 
    you have access to. The returned account IDs can be used in subsequent commands.
    
    Returns:
        A formatted list of all Google Ads accounts accessible with your credentials
    """
    try:
        creds = get_credentials()
        headers = get_headers(creds)
        
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers:listAccessibleCustomers"
        response = requests.get(url, headers=headers)
        
        if response.status_code != 200:
            return f"Error accessing accounts: {response.text}"
        
        customers = response.json()
        if not customers.get('resourceNames'):
            return "No accessible accounts found."
        
        # Format the results
        result_lines = ["Accessible Google Ads Accounts:"]
        result_lines.append("-" * 50)
        
        for resource_name in customers['resourceNames']:
            customer_id = resource_name.split('/')[-1]
            formatted_id = format_customer_id(customer_id)
            result_lines.append(f"Account ID: {formatted_id}")
        
        return "\n".join(result_lines)
    
    except Exception as e:
        return f"Error listing accounts: {str(e)}"

@mcp.tool()
async def execute_gaql_query(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes). Example: '9873186703'"),
    query: str = Field(description="Valid GAQL query string following Google Ads Query Language syntax")
) -> str:
    """
    Execute a custom GAQL (Google Ads Query Language) query.
    
    This tool allows you to run any valid GAQL query against the Google Ads API.
    
    Args:
        customer_id: The Google Ads customer ID as a string (10 digits, no dashes)
        query: The GAQL query to execute (must follow GAQL syntax)
        
    Returns:
        Formatted query results or error message
        
    Example:
        customer_id: "1234567890"
        query: "SELECT campaign.id, campaign.name FROM campaign LIMIT 10"
    """
    try:
        creds = get_credentials()
        headers = get_headers(creds)
        
        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"
        
        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            return f"Error executing query: {response.text}"
        
        results = response.json()
        if not results.get('results'):
            return "No results found for the query."
        
        # Format the results as a table
        result_lines = [f"Query Results for Account {formatted_customer_id}:"]
        result_lines.append("-" * 80)
        
        # Get field names from the first result
        fields = []
        first_result = results['results'][0]
        for key in first_result:
            if isinstance(first_result[key], dict):
                for subkey in first_result[key]:
                    fields.append(f"{key}.{subkey}")
            else:
                fields.append(key)
        
        # Add header
        result_lines.append(" | ".join(fields))
        result_lines.append("-" * 80)
        
        # Add data rows
        for result in results['results']:
            row_data = []
            for field in fields:
                if "." in field:
                    parent, child = field.split(".")
                    value = str(result.get(parent, {}).get(child, ""))
                else:
                    value = str(result.get(field, ""))
                row_data.append(value)
            result_lines.append(" | ".join(row_data))
        
        return "\n".join(result_lines)
    
    except Exception as e:
        return f"Error executing GAQL query: {str(e)}"

@mcp.tool()
async def get_campaign_performance(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes). Example: '9873186703'"),
    days: int = Field(default=30, description="Number of days to look back (7, 30, 90, etc.)")
) -> str:
    """
    Get campaign performance metrics for the specified time period.
    
    RECOMMENDED WORKFLOW:
    1. First run list_accounts() to get available account IDs
    2. Then run get_account_currency() to see what currency the account uses
    3. Finally run this command to get campaign performance
    
    Args:
        customer_id: The Google Ads customer ID as a string (10 digits, no dashes)
        days: Number of days to look back (default: 30)
        
    Returns:
        Formatted table of campaign performance data
        
    Note:
        Cost values are in micros (millionths) of the account currency
        (e.g., 1000000 = 1 USD in a USD account)
        
    Example:
        customer_id: "1234567890"
        days: 14
    """
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.average_cpc
        FROM campaign
        WHERE segments.date DURING LAST_{days}_DAYS
        ORDER BY metrics.cost_micros DESC
        LIMIT 50
    """
    
    return await execute_gaql_query(customer_id, query)

@mcp.tool()
async def get_ad_performance(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes). Example: '9873186703'"),
    days: int = Field(default=30, description="Number of days to look back (7, 30, 90, etc.)")
) -> str:
    """
    Get ad performance metrics for the specified time period.
    
    RECOMMENDED WORKFLOW:
    1. First run list_accounts() to get available account IDs
    2. Then run get_account_currency() to see what currency the account uses
    3. Finally run this command to get ad performance
    
    Args:
        customer_id: The Google Ads customer ID as a string (10 digits, no dashes)
        days: Number of days to look back (default: 30)
        
    Returns:
        Formatted table of ad performance data
        
    Note:
        Cost values are in micros (millionths) of the account currency
        (e.g., 1000000 = 1 USD in a USD account)
        
    Example:
        customer_id: "1234567890"
        days: 14
    """
    query = f"""
        SELECT
            ad_group_ad.ad.id,
            ad_group_ad.ad.name,
            ad_group_ad.status,
            campaign.name,
            ad_group.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions
        FROM ad_group_ad
        WHERE segments.date DURING LAST_{days}_DAYS
        ORDER BY metrics.impressions DESC
        LIMIT 50
    """
    
    return await execute_gaql_query(customer_id, query)

@mcp.tool()
async def run_gaql(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes). Example: '9873186703'"),
    query: str = Field(description="Valid GAQL query string following Google Ads Query Language syntax"),
    format: str = Field(default="table", description="Output format: 'table', 'json', or 'csv'")
) -> str:
    """
    Execute any arbitrary GAQL (Google Ads Query Language) query with custom formatting options.
    
    This is the most powerful tool for custom Google Ads data queries.
    
    Args:
        customer_id: The Google Ads customer ID as a string (10 digits, no dashes)
        query: The GAQL query to execute (any valid GAQL query)
        format: Output format ("table", "json", or "csv")
    
    Returns:
        Query results in the requested format
    
    EXAMPLE QUERIES:
    
    1. Basic campaign metrics:
        SELECT 
          campaign.name, 
          metrics.clicks, 
          metrics.impressions,
          metrics.cost_micros
        FROM campaign 
        WHERE segments.date DURING LAST_7_DAYS
    
    2. Ad group performance:
        SELECT 
          ad_group.name, 
          metrics.conversions, 
          metrics.cost_micros,
          campaign.name
        FROM ad_group 
        WHERE metrics.clicks > 100
    
    3. Keyword analysis:
        SELECT 
          keyword.text, 
          metrics.average_position, 
          metrics.ctr
        FROM keyword_view 
        ORDER BY metrics.impressions DESC
        
    4. Get conversion data:
        SELECT
          campaign.name,
          metrics.conversions,
          metrics.conversions_value,
          metrics.cost_micros
        FROM campaign
        WHERE segments.date DURING LAST_30_DAYS
        
            Note:
        Cost values are in micros (millionths) of the account currency
        (e.g., 1000000 = 1 USD in a USD account)
    """
    try:
        creds = get_credentials()
        headers = get_headers(creds)
        
        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"
        
        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            return f"Error executing query: {response.text}"
        
        results = response.json()
        if not results.get('results'):
            return "No results found for the query."
        
        if format.lower() == "json":
            return json.dumps(results, indent=2)
        
        elif format.lower() == "csv":
            # Get field names from the first result
            fields = []
            first_result = results['results'][0]
            for key, value in first_result.items():
                if isinstance(value, dict):
                    for subkey in value:
                        fields.append(f"{key}.{subkey}")
                else:
                    fields.append(key)
            
            # Create CSV string
            csv_lines = [",".join(fields)]
            for result in results['results']:
                row_data = []
                for field in fields:
                    if "." in field:
                        parent, child = field.split(".")
                        value = str(result.get(parent, {}).get(child, "")).replace(",", ";")
                    else:
                        value = str(result.get(field, "")).replace(",", ";")
                    row_data.append(value)
                csv_lines.append(",".join(row_data))
            
            return "\n".join(csv_lines)
        
        else:  # default table format
            result_lines = [f"Query Results for Account {formatted_customer_id}:"]
            result_lines.append("-" * 100)
            
            # Get field names and maximum widths
            fields = []
            field_widths = {}
            first_result = results['results'][0]
            
            for key, value in first_result.items():
                if isinstance(value, dict):
                    for subkey in value:
                        field = f"{key}.{subkey}"
                        fields.append(field)
                        field_widths[field] = len(field)
                else:
                    fields.append(key)
                    field_widths[key] = len(key)
            
            # Calculate maximum field widths
            for result in results['results']:
                for field in fields:
                    if "." in field:
                        parent, child = field.split(".")
                        value = str(result.get(parent, {}).get(child, ""))
                    else:
                        value = str(result.get(field, ""))
                    field_widths[field] = max(field_widths[field], len(value))
            
            # Create formatted header
            header = " | ".join(f"{field:{field_widths[field]}}" for field in fields)
            result_lines.append(header)
            result_lines.append("-" * len(header))
            
            # Add data rows
            for result in results['results']:
                row_data = []
                for field in fields:
                    if "." in field:
                        parent, child = field.split(".")
                        value = str(result.get(parent, {}).get(child, ""))
                    else:
                        value = str(result.get(field, ""))
                    row_data.append(f"{value:{field_widths[field]}}")
                result_lines.append(" | ".join(row_data))
            
            return "\n".join(result_lines)
    
    except Exception as e:
        return f"Error executing GAQL query: {str(e)}"

@mcp.tool()
async def get_ad_creatives(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes). Example: '9873186703'")
) -> str:
    """
    Get ad creative details including headlines, descriptions, and URLs.
    
    This tool retrieves the actual ad content (headlines, descriptions) 
    for review and analysis. Great for creative audits.
    
    RECOMMENDED WORKFLOW:
    1. First run list_accounts() to get available account IDs
    2. Then run this command with the desired account ID
    
    Args:
        customer_id: The Google Ads customer ID as a string (10 digits, no dashes)
        
    Returns:
        Formatted list of ad creative details
        
    Example:
        customer_id: "1234567890"
    """
    query = """
        SELECT
            ad_group_ad.ad.id,
            ad_group_ad.ad.name,
            ad_group_ad.ad.type,
            ad_group_ad.ad.final_urls,
            ad_group_ad.status,
            ad_group_ad.ad.responsive_search_ad.headlines,
            ad_group_ad.ad.responsive_search_ad.descriptions,
            ad_group.name,
            campaign.name
        FROM ad_group_ad
        WHERE ad_group_ad.status != 'REMOVED'
        ORDER BY campaign.name, ad_group.name
        LIMIT 50
    """
    
    try:
        creds = get_credentials()
        headers = get_headers(creds)
        
        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"
        
        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            return f"Error retrieving ad creatives: {response.text}"
        
        results = response.json()
        if not results.get('results'):
            return "No ad creatives found for this customer ID."
        
        # Format the results in a readable way
        output_lines = [f"Ad Creatives for Customer ID {formatted_customer_id}:"]
        output_lines.append("=" * 80)
        
        for i, result in enumerate(results['results'], 1):
            ad = result.get('adGroupAd', {}).get('ad', {})
            ad_group = result.get('adGroup', {})
            campaign = result.get('campaign', {})
            
            output_lines.append(f"\n{i}. Campaign: {campaign.get('name', 'N/A')}")
            output_lines.append(f"   Ad Group: {ad_group.get('name', 'N/A')}")
            output_lines.append(f"   Ad ID: {ad.get('id', 'N/A')}")
            output_lines.append(f"   Ad Name: {ad.get('name', 'N/A')}")
            output_lines.append(f"   Status: {result.get('adGroupAd', {}).get('status', 'N/A')}")
            output_lines.append(f"   Type: {ad.get('type', 'N/A')}")
            
            # Handle Responsive Search Ads
            rsa = ad.get('responsiveSearchAd', {})
            if rsa:
                if 'headlines' in rsa:
                    output_lines.append("   Headlines:")
                    for headline in rsa['headlines']:
                        output_lines.append(f"     - {headline.get('text', 'N/A')}")
                
                if 'descriptions' in rsa:
                    output_lines.append("   Descriptions:")
                    for desc in rsa['descriptions']:
                        output_lines.append(f"     - {desc.get('text', 'N/A')}")
            
            # Handle Final URLs
            final_urls = ad.get('finalUrls', [])
            if final_urls:
                output_lines.append(f"   Final URLs: {', '.join(final_urls)}")
            
            output_lines.append("-" * 80)
        
        return "\n".join(output_lines)
    
    except Exception as e:
        return f"Error retrieving ad creatives: {str(e)}"

@mcp.tool()
async def get_account_currency(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes). Example: '9873186703'")
) -> str:
    """
    Retrieve the default currency code used by the Google Ads account.
    
    IMPORTANT: Run this first before analyzing cost data to understand which currency
    the account uses. Cost values are always displayed in the account's currency.
    
    Args:
        customer_id: The Google Ads customer ID as a string (10 digits, no dashes)
    
    Returns:
        The account's default currency code (e.g., 'USD', 'EUR', 'GBP')
        
    Example:
        customer_id: "1234567890"
    """
    query = """
        SELECT
            customer.id,
            customer.currency_code
        FROM customer
        LIMIT 1
    """
    
    try:
        creds = get_credentials()
        
        # Force refresh if needed
        if not creds.valid:
            logger.info("Credentials not valid, attempting refresh...")
            if hasattr(creds, 'refresh_token') and creds.refresh_token:
                creds.refresh(Request())
                logger.info("Credentials refreshed successfully")
            else:
                raise ValueError("Invalid credentials and no refresh token available")
        
        headers = get_headers(creds)
        
        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"
        
        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            return f"Error retrieving account currency: {response.text}"
        
        results = response.json()
        if not results.get('results'):
            return "No account information found for this customer ID."
        
        # Extract the currency code from the results
        customer = results['results'][0].get('customer', {})
        currency_code = customer.get('currencyCode', 'Not specified')
        
        return f"Account {formatted_customer_id} uses currency: {currency_code}"
    
    except Exception as e:
        logger.error(f"Error retrieving account currency: {str(e)}")
        return f"Error retrieving account currency: {str(e)}"

@mcp.resource("gaql://reference")
def gaql_reference() -> str:
    """Google Ads Query Language (GAQL) reference documentation."""
    return """
    # Google Ads Query Language (GAQL) Reference
    
    GAQL is similar to SQL but with specific syntax for Google Ads. Here's a quick reference:
    
    ## Basic Query Structure
    ```
    SELECT field1, field2, ... 
    FROM resource_type
    WHERE condition
    ORDER BY field [ASC|DESC]
    LIMIT n
    ```
    
    ## Common Field Types
    
    ### Resource Fields
    - campaign.id, campaign.name, campaign.status
    - ad_group.id, ad_group.name, ad_group.status
    - ad_group_ad.ad.id, ad_group_ad.ad.final_urls
    - keyword.text, keyword.match_type
    
    ### Metric Fields
    - metrics.impressions
    - metrics.clicks
    - metrics.cost_micros
    - metrics.conversions
    - metrics.ctr
    - metrics.average_cpc
    
    ### Segment Fields
    - segments.date
    - segments.device
    - segments.day_of_week
    
    ## Common WHERE Clauses
    
    ### Date Ranges
    - WHERE segments.date DURING LAST_7_DAYS
    - WHERE segments.date DURING LAST_30_DAYS
    - WHERE segments.date BETWEEN '2023-01-01' AND '2023-01-31'
    
    ### Filtering
    - WHERE campaign.status = 'ENABLED'
    - WHERE metrics.clicks > 100
    - WHERE campaign.name LIKE '%Brand%'
    
    ## Tips
    - Always check account currency before analyzing cost data
    - Cost values are in micros (millionths): 1000000 = 1 unit of currency
    - Use LIMIT to avoid large result sets
    """

@mcp.prompt("google_ads_workflow")
def google_ads_workflow() -> str:
    """Provides guidance on the recommended workflow for using Google Ads tools."""
    return """
    I'll help you analyze your Google Ads account data. Here's the recommended workflow:
    
    1. First, let's list all the accounts you have access to:
       - Run the `list_accounts()` tool to get available account IDs
    
    2. Before analyzing cost data, let's check which currency the account uses:
       - Run `get_account_currency(customer_id="ACCOUNT_ID")` with your selected account
    
    3. Now we can explore the account data:
       - For campaign performance: `get_campaign_performance(customer_id="ACCOUNT_ID", days=30)`
       - For ad performance: `get_ad_performance(customer_id="ACCOUNT_ID", days=30)`
       - For ad creative review: `get_ad_creatives(customer_id="ACCOUNT_ID")`
    
    4. For custom queries, use the GAQL query tool:
       - `run_gaql(customer_id="ACCOUNT_ID", query="YOUR_QUERY", format="table")`
    
    5. Let me know if you have specific questions about:
       - Campaign performance
       - Ad performance
       - Keywords
       - Budgets
       - Conversions
    
    Important: Always provide the customer_id as a string.
    For example: customer_id="1234567890"
    """

@mcp.prompt("gaql_help")
def gaql_help() -> str:
    """Provides assistance for writing GAQL queries."""
    return """
    I'll help you write a Google Ads Query Language (GAQL) query. Here are some examples to get you started:
    
    ## Get campaign performance last 30 days
    ```
    SELECT
      campaign.id,
      campaign.name,
      campaign.status,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      metrics.conversions
    FROM campaign
    WHERE segments.date DURING LAST_30_DAYS
    ORDER BY metrics.cost_micros DESC
    ```
    
    ## Get keyword performance
    ```
    SELECT
      keyword.text,
      keyword.match_type,
      metrics.impressions,
      metrics.clicks,
      metrics.cost_micros,
      metrics.conversions
    FROM keyword_view
    WHERE segments.date DURING LAST_30_DAYS
    ORDER BY metrics.clicks DESC
    ```
    
    ## Get ads with poor performance
    ```
    SELECT
      ad_group_ad.ad.id,
      ad_group_ad.ad.name,
      campaign.name,
      ad_group.name,
      metrics.impressions,
      metrics.clicks,
      metrics.conversions
    FROM ad_group_ad
    WHERE 
      segments.date DURING LAST_30_DAYS
      AND metrics.impressions > 1000
      AND metrics.ctr < 0.01
    ORDER BY metrics.impressions DESC
    ```
    
    Once you've chosen a query, use it with:
    ```
    run_gaql(customer_id="YOUR_ACCOUNT_ID", query="YOUR_QUERY_HERE")
    ```
    
    Remember:
    - Always provide the customer_id as a string
    - Cost values are in micros (1,000,000 = 1 unit of currency)
    - Use LIMIT to avoid large result sets
    - Check the account currency before analyzing cost data
    """

@mcp.tool()
async def get_image_assets(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes). Example: '9873186703'"),
    limit: int = Field(default=50, description="Maximum number of image assets to return")
) -> str:
    """
    Retrieve all image assets in the account including their full-size URLs.
    
    This tool allows you to get details about image assets used in your Google Ads account,
    including the URLs to download the full-size images for further processing or analysis.
    
    RECOMMENDED WORKFLOW:
    1. First run list_accounts() to get available account IDs
    2. Then run this command with the desired account ID
    
    Args:
        customer_id: The Google Ads customer ID as a string (10 digits, no dashes)
        limit: Maximum number of image assets to return (default: 50)
        
    Returns:
        Formatted list of image assets with their download URLs
        
    Example:
        customer_id: "1234567890"
        limit: 100
    """
    query = f"""
        SELECT
            asset.id,
            asset.name,
            asset.type,
            asset.image_asset.full_size.url,
            asset.image_asset.full_size.height_pixels,
            asset.image_asset.full_size.width_pixels,
            asset.image_asset.file_size
        FROM
            asset
        WHERE
            asset.type = 'IMAGE'
        LIMIT {limit}
    """
    
    try:
        creds = get_credentials()
        headers = get_headers(creds)
        
        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"
        
        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            return f"Error retrieving image assets: {response.text}"
        
        results = response.json()
        if not results.get('results'):
            return "No image assets found for this customer ID."
        
        # Format the results in a readable way
        output_lines = [f"Image Assets for Customer ID {formatted_customer_id}:"]
        output_lines.append("=" * 80)
        
        for i, result in enumerate(results['results'], 1):
            asset = result.get('asset', {})
            image_asset = asset.get('imageAsset', {})
            full_size = image_asset.get('fullSize', {})
            
            output_lines.append(f"\n{i}. Asset ID: {asset.get('id', 'N/A')}")
            output_lines.append(f"   Name: {asset.get('name', 'N/A')}")
            
            if full_size:
                output_lines.append(f"   Image URL: {full_size.get('url', 'N/A')}")
                output_lines.append(f"   Dimensions: {full_size.get('widthPixels', 'N/A')} x {full_size.get('heightPixels', 'N/A')} px")
            
            file_size = image_asset.get('fileSize', 'N/A')
            if file_size != 'N/A':
                # Convert to KB for readability
                file_size_kb = int(file_size) / 1024
                output_lines.append(f"   File Size: {file_size_kb:.2f} KB")
            
            output_lines.append("-" * 80)
        
        return "\n".join(output_lines)
    
    except Exception as e:
        return f"Error retrieving image assets: {str(e)}"

@mcp.tool()
async def download_image_asset(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes). Example: '9873186703'"),
    asset_id: str = Field(description="The ID of the image asset to download"),
    output_dir: str = Field(default="./ad_images", description="Directory to save the downloaded image")
) -> str:
    """
    Download a specific image asset from a Google Ads account.
    
    This tool allows you to download the full-size version of an image asset
    for further processing, analysis, or backup.
    
    RECOMMENDED WORKFLOW:
    1. First run list_accounts() to get available account IDs
    2. Then run get_image_assets() to get available image asset IDs
    3. Finally use this command to download specific images
    
    Args:
        customer_id: The Google Ads customer ID as a string (10 digits, no dashes)
        asset_id: The ID of the image asset to download
        output_dir: Directory where the image should be saved (default: ./ad_images)
        
    Returns:
        Status message indicating success or failure of the download
        
    Example:
        customer_id: "1234567890"
        asset_id: "12345"
        output_dir: "./my_ad_images"
    """
    query = f"""
        SELECT
            asset.id,
            asset.name,
            asset.image_asset.full_size.url
        FROM
            asset
        WHERE
            asset.type = 'IMAGE'
            AND asset.id = {asset_id}
        LIMIT 1
    """
    
    try:
        creds = get_credentials()
        headers = get_headers(creds)
        
        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"
        
        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            return f"Error retrieving image asset: {response.text}"
        
        results = response.json()
        if not results.get('results'):
            return f"No image asset found with ID {asset_id}"
        
        # Extract the image URL
        asset = results['results'][0].get('asset', {})
        image_url = asset.get('imageAsset', {}).get('fullSize', {}).get('url')
        asset_name = asset.get('name', f"image_{asset_id}")
        
        if not image_url:
            return f"No download URL found for image asset ID {asset_id}"
        
        # Validate and sanitize the output directory to prevent path traversal
        try:
            # Get the base directory (current working directory)
            base_dir = Path.cwd()
            # Resolve the output directory to an absolute path
            resolved_output_dir = Path(output_dir).resolve()
            
            # Ensure the resolved path is within or under the current working directory
            # This prevents path traversal attacks like "../../../etc"
            try:
                resolved_output_dir.relative_to(base_dir)
            except ValueError:
                # If the path is not relative to base_dir, use the default safe directory
                resolved_output_dir = base_dir / "ad_images"
                logger.warning(f"Invalid output directory '{output_dir}' - using default './ad_images'")
            
            # Create output directory if it doesn't exist
            resolved_output_dir.mkdir(parents=True, exist_ok=True)
            
        except Exception as e:
            return f"Error creating output directory: {str(e)}"
        
        # Download the image
        image_response = requests.get(image_url)
        if image_response.status_code != 200:
            return f"Failed to download image: HTTP {image_response.status_code}"
        
        # Clean the filename to be safe for filesystem
        safe_name = ''.join(c for c in asset_name if c.isalnum() or c in ' ._-')
        filename = f"{asset_id}_{safe_name}.jpg"
        file_path = resolved_output_dir / filename
        
        # Save the image
        with open(file_path, 'wb') as f:
            f.write(image_response.content)
        
        return f"Successfully downloaded image asset {asset_id} to {file_path}"
    
    except Exception as e:
        return f"Error downloading image asset: {str(e)}"

@mcp.tool()
async def get_asset_usage(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes). Example: '9873186703'"),
    asset_id: str = Field(default=None, description="Optional: specific asset ID to look up (leave empty to get all image assets)"),
    asset_type: str = Field(default="IMAGE", description="Asset type to search for ('IMAGE', 'TEXT', 'VIDEO', etc.)")
) -> str:
    """
    Find where specific assets are being used in campaigns, ad groups, and ads.
    
    This tool helps you analyze how assets are linked to campaigns and ads across your account,
    which is useful for creative analysis and optimization.
    
    RECOMMENDED WORKFLOW:
    1. First run list_accounts() to get available account IDs
    2. Run get_image_assets() to see available assets
    3. Use this command to see where specific assets are used
    
    Args:
        customer_id: The Google Ads customer ID as a string (10 digits, no dashes)
        asset_id: Optional specific asset ID to look up (leave empty to get all assets of the specified type)
        asset_type: Type of asset to search for (default: 'IMAGE')
        
    Returns:
        Formatted report showing where assets are used in the account
        
    Example:
        customer_id: "1234567890"
        asset_id: "12345"
        asset_type: "IMAGE"
    """
    # Build the query based on whether a specific asset ID was provided
    where_clause = f"asset.type = '{asset_type}'"
    if asset_id:
        where_clause += f" AND asset.id = {asset_id}"
    
    # First get the assets themselves
    assets_query = f"""
        SELECT
            asset.id,
            asset.name,
            asset.type
        FROM
            asset
        WHERE
            {where_clause}
        LIMIT 100
    """
    
    # Then get the associations between assets and campaigns/ad groups
    # Try using campaign_asset instead of asset_link
    associations_query = f"""
        SELECT
            campaign.id,
            campaign.name,
            asset.id,
            asset.name,
            asset.type
        FROM
            campaign_asset
        WHERE
            {where_clause}
        LIMIT 500
    """

    # Also try ad_group_asset for ad group level information
    ad_group_query = f"""
        SELECT
            ad_group.id,
            ad_group.name,
            asset.id,
            asset.name,
            asset.type
        FROM
            ad_group_asset
        WHERE
            {where_clause}
        LIMIT 500
    """
    
    try:
        creds = get_credentials()
        headers = get_headers(creds)
        
        formatted_customer_id = format_customer_id(customer_id)
        
        # First get the assets
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"
        payload = {"query": assets_query}
        assets_response = requests.post(url, headers=headers, json=payload)
        
        if assets_response.status_code != 200:
            return f"Error retrieving assets: {assets_response.text}"
        
        assets_results = assets_response.json()
        if not assets_results.get('results'):
            return f"No {asset_type} assets found for this customer ID."
        
        # Now get the associations
        payload = {"query": associations_query}
        assoc_response = requests.post(url, headers=headers, json=payload)
        
        if assoc_response.status_code != 200:
            return f"Error retrieving asset associations: {assoc_response.text}"
        
        assoc_results = assoc_response.json()
        
        # Format the results in a readable way
        output_lines = [f"Asset Usage for Customer ID {formatted_customer_id}:"]
        output_lines.append("=" * 80)
        
        # Create a dictionary to organize asset usage by asset ID
        asset_usage = {}
        
        # Initialize the asset usage dictionary with basic asset info
        for result in assets_results.get('results', []):
            asset = result.get('asset', {})
            asset_id = asset.get('id')
            if asset_id:
                asset_usage[asset_id] = {
                    'name': asset.get('name', 'Unnamed asset'),
                    'type': asset.get('type', 'Unknown'),
                    'usage': []
                }
        
        # Add usage information from the associations
        for result in assoc_results.get('results', []):
            asset = result.get('asset', {})
            asset_id = asset.get('id')
            
            if asset_id and asset_id in asset_usage:
                campaign = result.get('campaign', {})
                ad_group = result.get('adGroup', {})
                ad = result.get('adGroupAd', {}).get('ad', {}) if 'adGroupAd' in result else {}
                asset_link = result.get('assetLink', {})
                
                usage_info = {
                    'campaign_id': campaign.get('id', 'N/A'),
                    'campaign_name': campaign.get('name', 'N/A'),
                    'ad_group_id': ad_group.get('id', 'N/A'),
                    'ad_group_name': ad_group.get('name', 'N/A'),
                    'ad_id': ad.get('id', 'N/A') if ad else 'N/A',
                    'ad_name': ad.get('name', 'N/A') if ad else 'N/A'
                }
                
                asset_usage[asset_id]['usage'].append(usage_info)
        
        # Format the output
        for asset_id, info in asset_usage.items():
            output_lines.append(f"\nAsset ID: {asset_id}")
            output_lines.append(f"Name: {info['name']}")
            output_lines.append(f"Type: {info['type']}")
            
            if info['usage']:
                output_lines.append("\nUsed in:")
                output_lines.append("-" * 60)
                output_lines.append(f"{'Campaign':<30} | {'Ad Group':<30}")
                output_lines.append("-" * 60)
                
                for usage in info['usage']:
                    campaign_str = f"{usage['campaign_name']} ({usage['campaign_id']})"
                    ad_group_str = f"{usage['ad_group_name']} ({usage['ad_group_id']})"
                    
                    output_lines.append(f"{campaign_str[:30]:<30} | {ad_group_str[:30]:<30}")
            
            output_lines.append("=" * 80)
        
        return "\n".join(output_lines)
    
    except Exception as e:
        return f"Error retrieving asset usage: {str(e)}"

@mcp.tool()
async def analyze_image_assets(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes). Example: '9873186703'"),
    days: int = Field(default=30, description="Number of days to look back (7, 30, 90, etc.)")
) -> str:
    """
    Analyze image assets with their performance metrics across campaigns.
    
    This comprehensive tool helps you understand which image assets are performing well
    by showing metrics like impressions, clicks, and conversions for each image.
    
    RECOMMENDED WORKFLOW:
    1. First run list_accounts() to get available account IDs
    2. Then run get_account_currency() to see what currency the account uses
    3. Finally run this command to analyze image asset performance
    
    Args:
        customer_id: The Google Ads customer ID as a string (10 digits, no dashes)
        days: Number of days to look back (default: 30)
        
    Returns:
        Detailed report of image assets and their performance metrics
        
    Example:
        customer_id: "1234567890"
        days: 14
    """
    # Make sure to use a valid date range format
    # Valid formats are: LAST_7_DAYS, LAST_14_DAYS, LAST_30_DAYS, etc. (with underscores)
    if days == 7:
        date_range = "LAST_7_DAYS"
    elif days == 14:
        date_range = "LAST_14_DAYS"
    elif days == 30:
        date_range = "LAST_30_DAYS"
    else:
        # Default to 30 days if not a standard range
        date_range = "LAST_30_DAYS"
        
    query = f"""
        SELECT
            asset.id,
            asset.name,
            asset.image_asset.full_size.url,
            asset.image_asset.full_size.width_pixels,
            asset.image_asset.full_size.height_pixels,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.cost_micros
        FROM
            campaign_asset
        WHERE
            asset.type = 'IMAGE'
            AND segments.date DURING LAST_30_DAYS
        ORDER BY
            metrics.impressions DESC
        LIMIT 200
    """
    
    try:
        creds = get_credentials()
        headers = get_headers(creds)
        
        formatted_customer_id = format_customer_id(customer_id)
        url = f"https://googleads.googleapis.com/{API_VERSION}/customers/{formatted_customer_id}/googleAds:search"
        
        payload = {"query": query}
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code != 200:
            return f"Error analyzing image assets: {response.text}"
        
        results = response.json()
        if not results.get('results'):
            return "No image asset performance data found for this customer ID and time period."
        
        # Group results by asset ID
        assets_data = {}
        for result in results.get('results', []):
            asset = result.get('asset', {})
            asset_id = asset.get('id')
            
            if asset_id not in assets_data:
                assets_data[asset_id] = {
                    'name': asset.get('name', f"Asset {asset_id}"),
                    'url': asset.get('imageAsset', {}).get('fullSize', {}).get('url', 'N/A'),
                    'dimensions': f"{asset.get('imageAsset', {}).get('fullSize', {}).get('widthPixels', 'N/A')} x {asset.get('imageAsset', {}).get('fullSize', {}).get('heightPixels', 'N/A')}",
                    'impressions': 0,
                    'clicks': 0,
                    'conversions': 0,
                    'cost_micros': 0,
                    'campaigns': set(),
                    'ad_groups': set()
                }
            
            # Aggregate metrics
            metrics = result.get('metrics', {})
            assets_data[asset_id]['impressions'] += int(metrics.get('impressions', 0))
            assets_data[asset_id]['clicks'] += int(metrics.get('clicks', 0))
            assets_data[asset_id]['conversions'] += float(metrics.get('conversions', 0))
            assets_data[asset_id]['cost_micros'] += int(metrics.get('costMicros', 0))
            
            # Add campaign and ad group info
            campaign = result.get('campaign', {})
            ad_group = result.get('adGroup', {})
            
            if campaign.get('name'):
                assets_data[asset_id]['campaigns'].add(campaign.get('name'))
            if ad_group.get('name'):
                assets_data[asset_id]['ad_groups'].add(ad_group.get('name'))
        
        # Format the results
        output_lines = [f"Image Asset Performance Analysis for Customer ID {formatted_customer_id} (Last {days} days):"]
        output_lines.append("=" * 100)
        
        # Sort assets by impressions (highest first)
        sorted_assets = sorted(assets_data.items(), key=lambda x: x[1]['impressions'], reverse=True)
        
        for asset_id, data in sorted_assets:
            output_lines.append(f"\nAsset ID: {asset_id}")
            output_lines.append(f"Name: {data['name']}")
            output_lines.append(f"Dimensions: {data['dimensions']}")
            
            # Calculate CTR if there are impressions
            ctr = (data['clicks'] / data['impressions'] * 100) if data['impressions'] > 0 else 0
            
            # Format metrics
            output_lines.append(f"\nPerformance Metrics:")
            output_lines.append(f"  Impressions: {data['impressions']:,}")
            output_lines.append(f"  Clicks: {data['clicks']:,}")
            output_lines.append(f"  CTR: {ctr:.2f}%")
            output_lines.append(f"  Conversions: {data['conversions']:.2f}")
            output_lines.append(f"  Cost (micros): {data['cost_micros']:,}")
            
            # Show where it's used
            output_lines.append(f"\nUsed in {len(data['campaigns'])} campaigns:")
            for campaign in list(data['campaigns'])[:5]:  # Show first 5 campaigns
                output_lines.append(f"  - {campaign}")
            if len(data['campaigns']) > 5:
                output_lines.append(f"  - ... and {len(data['campaigns']) - 5} more")
            
            # Add URL
            if data['url'] != 'N/A':
                output_lines.append(f"\nImage URL: {data['url']}")
            
            output_lines.append("-" * 100)
        
        return "\n".join(output_lines)
    
    except Exception as e:
        return f"Error analyzing image assets: {str(e)}"

@mcp.tool()
async def list_resources(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes). Example: '9873186703'")
) -> str:
    """
    List valid resources that can be used in GAQL FROM clauses.
    
    Args:
        customer_id: The Google Ads customer ID as a string
        
    Returns:
        Formatted list of valid resources
    """
    # Example query that lists some common resources
    # This might need to be adjusted based on what's available in your API version
    query = """
        SELECT
            google_ads_field.name,
            google_ads_field.category,
            google_ads_field.data_type
        FROM
            google_ads_field
        WHERE
            google_ads_field.category = 'RESOURCE'
        ORDER BY
            google_ads_field.name
    """
    
    # Use your existing run_gaql function to execute this query
    return await run_gaql(customer_id, query)


# ── Mutate tools ──────────────────────────────────────────────────────────────

@mcp.tool()
def update_ad_group_cpc(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    ad_group_id: str = Field(description="Ad group ID to update"),
    cpc_bid_micros: int = Field(description="New CPC bid in micros (e.g. 2500000 = $2.50)")
) -> dict:
    """Update the CPC bid on a single ad group."""
    client = get_google_ads_client()
    ag_service = client.get_service("AdGroupService")
    op = client.get_type("AdGroupOperation")

    ag = op.update
    ag.resource_name = ag_service.ad_group_path(format_customer_id(customer_id), ad_group_id)
    ag.cpc_bid_micros = cpc_bid_micros
    op.update_mask.paths.append("cpc_bid_micros")

    try:
        response = ag_service.mutate_ad_groups(
            customer_id=format_customer_id(customer_id),
            operations=[op]
        )
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def batch_update_ad_group_cpcs(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    updates: list = Field(description='List of {"ad_group_id": "...", "cpc_bid_micros": 2500000}')
) -> dict:
    """Update CPC bids on multiple ad groups in one API call."""
    client = get_google_ads_client()
    ag_service = client.get_service("AdGroupService")
    cid = format_customer_id(customer_id)
    operations = []

    for u in updates:
        op = client.get_type("AdGroupOperation")
        ag = op.update
        ag.resource_name = ag_service.ad_group_path(cid, u["ad_group_id"])
        ag.cpc_bid_micros = u["cpc_bid_micros"]
        op.update_mask.paths.append("cpc_bid_micros")
        operations.append(op)

    try:
        response = ag_service.mutate_ad_groups(customer_id=cid, operations=operations)
        return {"success": True, "updated": [r.resource_name for r in response.results]}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def create_keyword(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    ad_group_id: str = Field(description="Ad group ID to add the keyword to"),
    keyword_text: str = Field(description="Keyword text"),
    match_type: str = Field(description="Match type: EXACT, PHRASE, or BROAD"),
    cpc_bid_micros: int = Field(default=0, description="CPC bid in micros (ignored for negative keywords)"),
    negative: bool = Field(default=False, description="True to add as a negative keyword"),
    status: str = Field(default="PAUSED", description="ENABLED or PAUSED")
) -> dict:
    """Create a keyword (positive or negative) in an ad group."""
    client = get_google_ads_client()
    agc_service = client.get_service("AdGroupCriterionService")
    op = client.get_type("AdGroupCriterionOperation")
    cid = format_customer_id(customer_id)

    agc = op.create
    agc.ad_group = client.get_service("AdGroupService").ad_group_path(cid, ad_group_id)
    agc.keyword.text = keyword_text
    agc.keyword.match_type = getattr(client.enums.KeywordMatchTypeEnum, match_type.upper())
    agc.negative = negative
    agc.status = getattr(client.enums.AdGroupCriterionStatusEnum, status.upper())

    if not negative and cpc_bid_micros:
        agc.cpc_bid_micros = cpc_bid_micros

    try:
        response = agc_service.mutate_ad_group_criteria(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def create_campaign_negative_keyword(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(description="Campaign ID to add the negative keyword to"),
    keyword_text: str = Field(description="Keyword text to block"),
    match_type: str = Field(default="BROAD", description="Match type: EXACT, PHRASE, or BROAD")
) -> dict:
    """Add a negative keyword at the campaign level."""
    client = get_google_ads_client()
    service = client.get_service("CampaignCriterionService")
    op = client.get_type("CampaignCriterionOperation")
    cid = format_customer_id(customer_id)

    criterion = op.create
    criterion.campaign = client.get_service("CampaignService").campaign_path(cid, campaign_id)
    criterion.negative = True
    criterion.keyword.text = keyword_text
    criterion.keyword.match_type = getattr(client.enums.KeywordMatchTypeEnum, match_type.upper())

    try:
        response = service.mutate_campaign_criteria(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def create_rsa_ad(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    ad_group_id: str = Field(description="Ad group ID to create the ad in"),
    headlines: list = Field(description="List of up to 15 headline strings"),
    descriptions: list = Field(description="List of up to 4 description strings"),
    final_url: str = Field(description="Landing page URL"),
    path1: str = Field(default="", description="Optional display path 1 (shown in ad URL)"),
    path2: str = Field(default="", description="Optional display path 2 (shown in ad URL)"),
    status: str = Field(default="PAUSED", description="ENABLED or PAUSED")
) -> dict:
    """Create a Responsive Search Ad (RSA) in an ad group."""
    client = get_google_ads_client()
    ad_group_ad_service = client.get_service("AdGroupAdService")
    op = client.get_type("AdGroupAdOperation")
    cid = format_customer_id(customer_id)

    ad_group_ad = op.create
    ad_group_ad.ad_group = client.get_service("AdGroupService").ad_group_path(cid, ad_group_id)
    ad_group_ad.status = getattr(client.enums.AdGroupAdStatusEnum, status.upper())

    ad = ad_group_ad.ad
    ad.final_urls.append(final_url)
    ad.responsive_search_ad.path1 = path1
    ad.responsive_search_ad.path2 = path2

    for text in headlines[:15]:
        asset = client.get_type("AdTextAsset")
        asset.text = text
        ad.responsive_search_ad.headlines.append(asset)

    for text in descriptions[:4]:
        asset = client.get_type("AdTextAsset")
        asset.text = text
        ad.responsive_search_ad.descriptions.append(asset)

    try:
        response = ad_group_ad_service.mutate_ad_group_ads(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


# ── Additional reporting tools ────────────────────────────────────────────────

@mcp.tool()
async def get_search_terms_report(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    days: int = Field(default=30, description="Number of days to look back"),
    campaign_id: str = Field(default=None, description="Optional: filter to a single campaign ID")
) -> str:
    """
    Show which search terms triggered your ads, with clicks, cost, and conversions.
    Essential for finding new negative keywords or keyword opportunities.
    """
    where_parts = [f"segments.date DURING LAST_{days}_DAYS"]
    if campaign_id:
        where_parts.append(f"campaign.id = {campaign_id}")
    query = f"""
        SELECT
            search_term_view.search_term,
            search_term_view.status,
            campaign.id,
            campaign.name,
            ad_group.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.ctr
        FROM search_term_view
        WHERE {" AND ".join(where_parts)}
        ORDER BY metrics.impressions DESC
        LIMIT 500
    """
    return await run_gaql(customer_id, query)


@mcp.tool()
async def get_keyword_quality_scores(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(default=None, description="Optional: filter to a single campaign ID")
) -> str:
    """
    Get quality scores, expected CTR, ad relevance, and landing page experience for all keywords.
    Sort is ascending so the worst-scoring keywords appear first.
    """
    where_parts = [
        "ad_group_criterion.type = 'KEYWORD'",
        "ad_group_criterion.status != 'REMOVED'",
    ]
    if campaign_id:
        where_parts.append(f"campaign.id = {campaign_id}")
    query = f"""
        SELECT
            campaign.name,
            ad_group.name,
            ad_group_criterion.criterion_id,
            ad_group_criterion.keyword.text,
            ad_group_criterion.keyword.match_type,
            ad_group_criterion.quality_info.quality_score,
            ad_group_criterion.quality_info.search_predicted_ctr,
            ad_group_criterion.quality_info.creative_quality_score,
            ad_group_criterion.quality_info.post_click_quality_score
        FROM ad_group_criterion
        WHERE {" AND ".join(where_parts)}
        ORDER BY ad_group_criterion.quality_info.quality_score ASC
        LIMIT 1000
    """
    return await run_gaql(customer_id, query)


@mcp.tool()
async def get_geographic_performance(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    days: int = Field(default=30, description="Number of days to look back")
) -> str:
    """Get performance metrics broken down by geographic location."""
    query = f"""
        SELECT
            geographic_view.country_criterion_id,
            geographic_view.location_type,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.ctr
        FROM geographic_view
        WHERE segments.date DURING LAST_{days}_DAYS
        ORDER BY metrics.cost_micros DESC
        LIMIT 500
    """
    return await run_gaql(customer_id, query)


@mcp.tool()
async def get_device_performance(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    days: int = Field(default=30, description="Number of days to look back")
) -> str:
    """Get performance metrics broken down by device (MOBILE, DESKTOP, TABLET)."""
    query = f"""
        SELECT
            segments.device,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.ctr,
            metrics.average_cpc
        FROM campaign
        WHERE segments.date DURING LAST_{days}_DAYS
        ORDER BY segments.device, metrics.cost_micros DESC
        LIMIT 500
    """
    return await run_gaql(customer_id, query)


@mcp.tool()
async def get_hourly_performance(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    days: int = Field(default=7, description="Number of days to look back (keep ≤14 for hourly data)")
) -> str:
    """Get performance metrics broken down by hour of day — useful for ad scheduling decisions."""
    query = f"""
        SELECT
            segments.hour,
            campaign.name,
            metrics.impressions,
            metrics.clicks,
            metrics.cost_micros,
            metrics.conversions,
            metrics.ctr
        FROM campaign
        WHERE segments.date DURING LAST_{days}_DAYS
        ORDER BY segments.hour ASC
        LIMIT 500
    """
    return await run_gaql(customer_id, query)


@mcp.tool()
async def get_budget_utilization(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)")
) -> str:
    """
    Show each active campaign's daily budget vs yesterday's spend.
    Helps identify underspending or budget-limited campaigns.
    """
    query = """
        SELECT
            campaign.name,
            campaign.status,
            campaign_budget.name,
            campaign_budget.amount_micros,
            campaign_budget.period,
            campaign_budget.total_amount_micros,
            metrics.cost_micros
        FROM campaign
        WHERE segments.date DURING LAST_1_DAYS
          AND campaign.status = 'ENABLED'
        ORDER BY metrics.cost_micros DESC
        LIMIT 200
    """
    return await run_gaql(customer_id, query)


@mcp.tool()
async def get_auction_insights(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(default=None, description="Optional: filter to a single campaign ID"),
    days: int = Field(default=30, description="Number of days to look back")
) -> str:
    """Show competitor overlap rates and impression share from auction insights."""
    where_parts = [f"segments.date DURING LAST_{days}_DAYS"]
    if campaign_id:
        where_parts.append(f"campaign.id = {campaign_id}")
    query = f"""
        SELECT
            auction_insight_competitor.domain,
            campaign.name,
            metrics.auction_insight_search_overlap_rate,
            metrics.auction_insight_search_outranking_share,
            metrics.auction_insight_search_position_above_rate,
            metrics.auction_insight_search_top_impression_percentage,
            metrics.auction_insight_search_absolute_top_impression_percentage
        FROM auction_insight_competitor
        WHERE {" AND ".join(where_parts)}
        ORDER BY metrics.auction_insight_search_overlap_rate DESC
        LIMIT 200
    """
    return await run_gaql(customer_id, query)


@mcp.tool()
async def list_conversion_actions(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)")
) -> str:
    """List all conversion actions (goals) defined in the account with their IDs and settings."""
    query = """
        SELECT
            conversion_action.id,
            conversion_action.name,
            conversion_action.status,
            conversion_action.type,
            conversion_action.category,
            conversion_action.counting_type,
            conversion_action.value_settings.default_value,
            conversion_action.value_settings.always_use_default_value
        FROM conversion_action
        WHERE conversion_action.status != 'REMOVED'
        ORDER BY conversion_action.name
    """
    return await run_gaql(customer_id, query)


@mcp.tool()
async def get_recommendations(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)")
) -> str:
    """
    Fetch Google's automated recommendations for the account.
    The resource_name values returned can be passed to apply_recommendation or dismiss_recommendation.
    """
    query = """
        SELECT
            recommendation.resource_name,
            recommendation.type,
            recommendation.campaign,
            recommendation.impact.base_metrics.impressions,
            recommendation.impact.base_metrics.clicks,
            recommendation.impact.base_metrics.cost_micros,
            recommendation.impact.potential_metrics.impressions,
            recommendation.impact.potential_metrics.clicks
        FROM recommendation
        LIMIT 50
    """
    return await run_gaql(customer_id, query)


# ── Geo target lookup ────────────────────────────────────────────────────────

@mcp.tool()
def suggest_geo_targets(
    query: str = Field(description="Location name to search for, e.g. 'Brussels', 'Antwerp'"),
    country_code: str = Field(default="BE", description="ISO country code to narrow results, e.g. BE, US, GB, DE"),
    locale: str = Field(default="en", description="BCP-47 locale for result names, e.g. 'en', 'nl', 'fr'")
) -> dict:
    """
    Look up geo target constant IDs by location name.
    Use the returned id values with add_location_target.
    """
    client = get_google_ads_client()
    gtc_service = client.get_service("GeoTargetConstantService")
    request = client.get_type("SuggestGeoTargetConstantsRequest")
    request.locale = locale
    request.country_code = country_code
    request.location_names.names.append(query)

    try:
        response = gtc_service.suggest_geo_target_constants(request=request)
        results = []
        for suggestion in response.geo_target_constant_suggestions:
            gtc = suggestion.geo_target_constant
            results.append({
                "id": gtc.id,
                "name": gtc.name,
                "type": gtc.target_type,
                "canonical": gtc.canonical_name,
            })
        return {"results": results}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


# ── Campaign structure: create ────────────────────────────────────────────────

@mcp.tool()
def create_campaign_budget(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    name: str = Field(description="Budget name (must be unique in account)"),
    amount_micros: int = Field(description="Daily budget in micros — e.g. 10000000 = $10/day"),
    delivery_method: str = Field(default="STANDARD", description="STANDARD (default pacing) or ACCELERATED")
) -> dict:
    """
    Create a campaign budget. Returns the resource_name to pass into create_campaign.
    Run this before create_campaign.
    """
    client = get_google_ads_client()
    service = client.get_service("CampaignBudgetService")
    op = client.get_type("CampaignBudgetOperation")
    cid = format_customer_id(customer_id)

    budget = op.create
    budget.name = name
    budget.amount_micros = amount_micros
    budget.delivery_method = getattr(client.enums.BudgetDeliveryMethodEnum, delivery_method.upper())

    try:
        response = service.mutate_campaign_budgets(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def update_campaign_budget(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    budget_id: str = Field(description="Campaign budget ID to update"),
    amount_micros: int = Field(description="New daily budget in micros — e.g. 10000000 = $10/day")
) -> dict:
    """Update the daily amount of an existing campaign budget."""
    client = get_google_ads_client()
    cid = format_customer_id(customer_id)
    service = client.get_service("CampaignBudgetService")
    op = client.get_type("CampaignBudgetOperation")

    budget = op.update
    budget.resource_name = f"customers/{cid}/campaignBudgets/{budget_id}"
    budget.amount_micros = amount_micros
    op.update_mask.paths.append("amount_micros")

    try:
        response = service.mutate_campaign_budgets(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def create_campaign(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    name: str = Field(description="Campaign name"),
    budget_resource_name: str = Field(description="Resource name returned by create_campaign_budget"),
    advertising_channel_type: str = Field(default="SEARCH", description="SEARCH, DISPLAY, VIDEO, or SHOPPING"),
    bidding_strategy: str = Field(default="MANUAL_CPC", description="MANUAL_CPC, TARGET_CPA, TARGET_ROAS, MAXIMIZE_CONVERSIONS, or MAXIMIZE_CONVERSION_VALUE"),
    status: str = Field(default="PAUSED", description="ENABLED or PAUSED — defaults to PAUSED for safety"),
    target_cpa_micros: int = Field(default=None, description="Required when bidding_strategy=TARGET_CPA — e.g. 5000000 = $5"),
    target_roas: float = Field(default=None, description="Required when bidding_strategy=TARGET_ROAS — e.g. 3.0 = 300%")
) -> dict:
    """
    Create a new campaign. Typical flow:
    create_campaign_budget → create_campaign → create_ad_group → create_keyword / create_rsa_ad
    """
    client = get_google_ads_client()
    service = client.get_service("CampaignService")
    op = client.get_type("CampaignOperation")
    cid = format_customer_id(customer_id)

    campaign = op.create
    campaign.name = name
    campaign.campaign_budget = budget_resource_name
    campaign.advertising_channel_type = getattr(
        client.enums.AdvertisingChannelTypeEnum, advertising_channel_type.upper()
    )
    campaign.status = getattr(client.enums.CampaignStatusEnum, status.upper())
    campaign.contains_eu_political_advertising = 2  # 2 = NO (UNSPECIFIED=0, UNKNOWN=1, NO=2, YES=3)

    if advertising_channel_type.upper() == "SEARCH":
        campaign.network_settings.target_google_search = True
        campaign.network_settings.target_search_network = True
        campaign.network_settings.target_content_network = False

    strategy = bidding_strategy.upper()
    if strategy == "MANUAL_CPC":
        campaign.manual_cpc.enhanced_cpc_enabled = False
    elif strategy == "TARGET_CPA":
        if not target_cpa_micros:
            return {"success": False, "error": "target_cpa_micros is required for TARGET_CPA"}
        campaign.target_cpa.target_cpa_micros = target_cpa_micros
    elif strategy == "TARGET_ROAS":
        if not target_roas:
            return {"success": False, "error": "target_roas is required for TARGET_ROAS"}
        campaign.target_roas.target_roas = target_roas
    elif strategy == "MAXIMIZE_CONVERSIONS":
        campaign.maximize_conversions.target_cpa_micros = target_cpa_micros or 0
    elif strategy == "MAXIMIZE_CONVERSION_VALUE":
        campaign.maximize_conversion_value.target_roas = target_roas or 0

    try:
        response = service.mutate_campaigns(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def update_campaign_budget_link(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(description="Campaign ID to update"),
    budget_resource_name: str = Field(description="Budget resource name to assign, e.g. customers/123/campaignBudgets/456")
) -> dict:
    """Reassign a campaign to a different budget."""
    client = get_google_ads_client()
    cid = format_customer_id(customer_id)
    service = client.get_service("CampaignService")
    op = client.get_type("CampaignOperation")

    camp = op.update
    camp.resource_name = f"customers/{cid}/campaigns/{campaign_id}"
    camp.campaign_budget = budget_resource_name
    op.update_mask.paths.append("campaign_budget")

    try:
        response = service.mutate_campaigns(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def create_ad_group(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(description="Campaign ID to create the ad group in"),
    name: str = Field(description="Ad group name"),
    cpc_bid_micros: int = Field(default=1000000, description="Default CPC bid in micros — e.g. 1000000 = $1.00"),
    status: str = Field(default="PAUSED", description="ENABLED or PAUSED")
) -> dict:
    """Create an ad group inside a campaign."""
    client = get_google_ads_client()
    service = client.get_service("AdGroupService")
    op = client.get_type("AdGroupOperation")
    cid = format_customer_id(customer_id)

    ag = op.create
    ag.name = name
    ag.campaign = client.get_service("CampaignService").campaign_path(cid, campaign_id)
    ag.status = getattr(client.enums.AdGroupStatusEnum, status.upper())
    ag.cpc_bid_micros = cpc_bid_micros

    try:
        response = service.mutate_ad_groups(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


# ── Campaign / ad group / ad status and field updates ────────────────────────

@mcp.tool()
def update_campaign(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(description="Campaign ID to update"),
    status: str = Field(default=None, description="ENABLED, PAUSED, or REMOVED"),
    name: str = Field(default=None, description="New campaign name")
) -> dict:
    """Update a campaign's status or name. Use status=REMOVED to permanently delete it."""
    client = get_google_ads_client()
    service = client.get_service("CampaignService")
    op = client.get_type("CampaignOperation")
    cid = format_customer_id(customer_id)

    campaign = op.update
    campaign.resource_name = service.campaign_path(cid, campaign_id)
    paths = []
    if status:
        campaign.status = getattr(client.enums.CampaignStatusEnum, status.upper())
        paths.append("status")
    if name:
        campaign.name = name
        paths.append("name")
    if not paths:
        return {"success": False, "error": "Provide at least one field to update (status or name)"}
    op.update_mask.paths.extend(paths)

    try:
        response = service.mutate_campaigns(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def update_ad_group(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    ad_group_id: str = Field(description="Ad group ID to update"),
    status: str = Field(default=None, description="ENABLED, PAUSED, or REMOVED"),
    name: str = Field(default=None, description="New ad group name"),
    cpc_bid_micros: int = Field(default=None, description="New default CPC bid in micros")
) -> dict:
    """Update an ad group's status, name, or default CPC bid."""
    client = get_google_ads_client()
    service = client.get_service("AdGroupService")
    op = client.get_type("AdGroupOperation")
    cid = format_customer_id(customer_id)

    ag = op.update
    ag.resource_name = service.ad_group_path(cid, ad_group_id)
    paths = []
    if status:
        ag.status = getattr(client.enums.AdGroupStatusEnum, status.upper())
        paths.append("status")
    if name:
        ag.name = name
        paths.append("name")
    if cpc_bid_micros is not None:
        ag.cpc_bid_micros = cpc_bid_micros
        paths.append("cpc_bid_micros")
    if not paths:
        return {"success": False, "error": "Provide at least one field to update"}
    op.update_mask.paths.extend(paths)

    try:
        response = service.mutate_ad_groups(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def update_ad_status(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    ad_group_id: str = Field(description="Ad group ID that contains the ad"),
    ad_id: str = Field(description="Ad ID to update"),
    status: str = Field(description="ENABLED, PAUSED, or REMOVED")
) -> dict:
    """Pause, enable, or remove an individual ad."""
    client = get_google_ads_client()
    service = client.get_service("AdGroupAdService")
    op = client.get_type("AdGroupAdOperation")
    cid = format_customer_id(customer_id)

    ad_group_ad = op.update
    ad_group_ad.resource_name = service.ad_group_ad_path(cid, ad_group_id, ad_id)
    ad_group_ad.status = getattr(client.enums.AdGroupAdStatusEnum, status.upper())
    op.update_mask.paths.append("status")

    try:
        response = service.mutate_ad_group_ads(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


# ── Keyword update / removal ──────────────────────────────────────────────────

@mcp.tool()
def update_keyword(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    ad_group_id: str = Field(description="Ad group ID that contains the keyword"),
    criterion_id: str = Field(description="Keyword criterion ID (from get_keyword_quality_scores)"),
    status: str = Field(default=None, description="ENABLED or PAUSED"),
    cpc_bid_micros: int = Field(default=None, description="New CPC bid in micros")
) -> dict:
    """Update a keyword's status or individual CPC bid."""
    client = get_google_ads_client()
    service = client.get_service("AdGroupCriterionService")
    op = client.get_type("AdGroupCriterionOperation")
    cid = format_customer_id(customer_id)

    criterion = op.update
    criterion.resource_name = service.ad_group_criterion_path(cid, ad_group_id, criterion_id)
    paths = []
    if status:
        criterion.status = getattr(client.enums.AdGroupCriterionStatusEnum, status.upper())
        paths.append("status")
    if cpc_bid_micros is not None:
        criterion.cpc_bid_micros = cpc_bid_micros
        paths.append("cpc_bid_micros")
    if not paths:
        return {"success": False, "error": "Provide at least one field to update (status or cpc_bid_micros)"}
    op.update_mask.paths.extend(paths)

    try:
        response = service.mutate_ad_group_criteria(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def remove_keyword(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    ad_group_id: str = Field(description="Ad group ID that contains the keyword"),
    criterion_id: str = Field(description="Keyword criterion ID to remove permanently")
) -> dict:
    """Permanently remove a keyword from an ad group."""
    client = get_google_ads_client()
    service = client.get_service("AdGroupCriterionService")
    op = client.get_type("AdGroupCriterionOperation")
    cid = format_customer_id(customer_id)

    op.remove = service.ad_group_criterion_path(cid, ad_group_id, criterion_id)

    try:
        response = service.mutate_ad_group_criteria(customer_id=cid, operations=[op])
        return {"success": True, "removed": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


# ── Shared negative keyword lists ─────────────────────────────────────────────

@mcp.tool()
def create_negative_keyword_list(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    name: str = Field(description="Name for the shared negative keyword list")
) -> dict:
    """
    Create a shared negative keyword list.
    Returns shared_set_id to use with add_keywords_to_negative_list and
    attach_negative_keyword_list_to_campaign.
    """
    client = get_google_ads_client()
    service = client.get_service("SharedSetService")
    op = client.get_type("SharedSetOperation")
    cid = format_customer_id(customer_id)

    shared_set = op.create
    shared_set.name = name
    shared_set.type_ = client.enums.SharedSetTypeEnum.NEGATIVE_KEYWORDS

    try:
        response = service.mutate_shared_sets(customer_id=cid, operations=[op])
        resource_name = response.results[0].resource_name
        return {
            "success": True,
            "resource_name": resource_name,
            "shared_set_id": resource_name.split("/")[-1],
        }
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def add_keywords_to_negative_list(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    shared_set_id: str = Field(description="Shared set ID from create_negative_keyword_list"),
    keywords: list = Field(description='Keyword strings to add, e.g. ["free", "cheap", "diy"]'),
    match_type: str = Field(default="BROAD", description="BROAD, PHRASE, or EXACT")
) -> dict:
    """Add negative keywords to an existing shared negative keyword list."""
    client = get_google_ads_client()
    service = client.get_service("SharedCriterionService")
    cid = format_customer_id(customer_id)
    shared_set_resource = client.get_service("SharedSetService").shared_set_path(cid, shared_set_id)

    operations = []
    for kw_text in keywords:
        op = client.get_type("SharedCriterionOperation")
        criterion = op.create
        criterion.shared_set = shared_set_resource
        criterion.keyword.text = kw_text
        criterion.keyword.match_type = getattr(client.enums.KeywordMatchTypeEnum, match_type.upper())
        operations.append(op)

    try:
        response = service.mutate_shared_criteria(customer_id=cid, operations=operations)
        return {"success": True, "added": [r.resource_name for r in response.results]}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def attach_negative_keyword_list_to_campaign(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(description="Campaign ID to attach the list to"),
    shared_set_id: str = Field(description="Shared set ID from create_negative_keyword_list")
) -> dict:
    """Attach a shared negative keyword list to a campaign."""
    client = get_google_ads_client()
    service = client.get_service("CampaignSharedSetService")
    op = client.get_type("CampaignSharedSetOperation")
    cid = format_customer_id(customer_id)

    css = op.create
    css.campaign = client.get_service("CampaignService").campaign_path(cid, campaign_id)
    css.shared_set = client.get_service("SharedSetService").shared_set_path(cid, shared_set_id)

    try:
        response = service.mutate_campaign_shared_sets(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


# ── Asset upload ──────────────────────────────────────────────────────────────

@mcp.tool()
def upload_image_asset(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    name: str = Field(description="Asset name — must be unique in the account"),
    image_data_base64: str = Field(description="Base64-encoded image bytes. Encode with: base64.b64encode(open('img.jpg','rb').read()).decode()")
) -> dict:
    """
    Upload an image to the Google Ads asset library.
    Returns resource_name for use in ads, sitelinks, or campaign assets.
    """
    client = get_google_ads_client()
    service = client.get_service("AssetService")
    op = client.get_type("AssetOperation")
    cid = format_customer_id(customer_id)

    asset = op.create
    asset.name = name
    asset.type_ = client.enums.AssetTypeEnum.IMAGE
    asset.image_asset.data = base64.b64decode(image_data_base64)

    try:
        response = service.mutate_assets(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


# ── Targeting ─────────────────────────────────────────────────────────────────

@mcp.tool()
def remove_location_target(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(description="Campaign ID the location target belongs to"),
    criterion_id: str = Field(description="Criterion ID of the location target to remove (from get_geographic_performance or the campaign criteria)")
) -> dict:
    """Remove a location target from a campaign by its criterion ID."""
    client = get_google_ads_client()
    service = client.get_service("CampaignCriterionService")
    op = client.get_type("CampaignCriterionOperation")
    cid = format_customer_id(customer_id)

    op.remove = service.campaign_criterion_path(cid, campaign_id, criterion_id)

    try:
        response = service.mutate_campaign_criteria(customer_id=cid, operations=[op])
        return {"success": True, "removed": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def add_location_target(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(description="Campaign ID"),
    location_id: str = Field(description="Geo target constant ID — e.g. 2840=USA, 2826=UK, 2036=Australia, 2276=Germany"),
    negative: bool = Field(default=False, description="True to exclude this location"),
    bid_modifier: float = Field(default=1.0, description="Bid multiplier e.g. 1.2 = +20% (ignored for negative targets)")
) -> dict:
    """
    Add a location target or exclusion to a campaign.
    Find country/city IDs in Google's geo targets reference CSV.
    """
    client = get_google_ads_client()
    service = client.get_service("CampaignCriterionService")
    op = client.get_type("CampaignCriterionOperation")
    cid = format_customer_id(customer_id)

    criterion = op.create
    criterion.campaign = client.get_service("CampaignService").campaign_path(cid, campaign_id)
    criterion.negative = negative
    criterion.location.geo_target_constant = f"geoTargetConstants/{location_id}"
    if not negative and bid_modifier != 1.0:
        criterion.bid_modifier = bid_modifier

    try:
        response = service.mutate_campaign_criteria(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def add_device_bid_adjustment(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(description="Campaign ID"),
    device: str = Field(description="MOBILE, TABLET, DESKTOP, or CONNECTED_TV"),
    bid_modifier: float = Field(description="Multiplier — e.g. 0.5 = -50%, 1.3 = +30%, 0.0 = opt out of device")
) -> dict:
    """Set a bid adjustment for a specific device type on a campaign."""
    client = get_google_ads_client()
    service = client.get_service("CampaignCriterionService")
    op = client.get_type("CampaignCriterionOperation")
    cid = format_customer_id(customer_id)

    criterion = op.create
    criterion.campaign = client.get_service("CampaignService").campaign_path(cid, campaign_id)
    criterion.device.type_ = getattr(client.enums.DeviceEnum, device.upper())
    criterion.bid_modifier = bid_modifier

    try:
        response = service.mutate_campaign_criteria(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def add_ad_schedule(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(description="Campaign ID"),
    day_of_week: str = Field(description="MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, or SUNDAY"),
    start_hour: int = Field(description="Start hour 0–23"),
    end_hour: int = Field(description="End hour 1–24 (use 24 for end of day)"),
    bid_modifier: float = Field(default=1.0, description="Bid multiplier for this slot — e.g. 1.2 = +20%")
) -> dict:
    """Add a day-parting rule (ad schedule) to a campaign."""
    client = get_google_ads_client()
    service = client.get_service("CampaignCriterionService")
    op = client.get_type("CampaignCriterionOperation")
    cid = format_customer_id(customer_id)

    criterion = op.create
    criterion.campaign = client.get_service("CampaignService").campaign_path(cid, campaign_id)
    criterion.ad_schedule.day_of_week = getattr(client.enums.DayOfWeekEnum, day_of_week.upper())
    criterion.ad_schedule.start_hour = start_hour
    criterion.ad_schedule.start_minute = client.enums.MinuteOfHourEnum.ZERO
    criterion.ad_schedule.end_hour = end_hour
    criterion.ad_schedule.end_minute = client.enums.MinuteOfHourEnum.ZERO
    criterion.bid_modifier = bid_modifier

    try:
        response = service.mutate_campaign_criteria(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


# ── Extensions (asset-based, API v14+) ───────────────────────────────────────

@mcp.tool()
def add_sitelink_to_campaign(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(description="Campaign ID to attach the sitelink to"),
    link_text: str = Field(description="Sitelink anchor text shown in the ad (max 25 chars)"),
    final_url: str = Field(description="Landing page URL for this sitelink"),
    description1: str = Field(default="", description="Optional first description line (max 35 chars)"),
    description2: str = Field(default="", description="Optional second description line (max 35 chars)")
) -> dict:
    """Create a sitelink asset and attach it to a campaign."""
    client = get_google_ads_client()
    cid = format_customer_id(customer_id)

    asset_service = client.get_service("AssetService")
    asset_op = client.get_type("AssetOperation")
    asset = asset_op.create
    asset.name = f"Sitelink: {link_text}"
    asset.sitelink_asset.link_text = link_text
    asset.sitelink_asset.final_urls.append(final_url)
    if description1:
        asset.sitelink_asset.description1 = description1
    if description2:
        asset.sitelink_asset.description2 = description2

    try:
        asset_response = asset_service.mutate_assets(customer_id=cid, operations=[asset_op])
        asset_resource_name = asset_response.results[0].resource_name
    except GoogleAdsException as e:
        return {"success": False, "error": f"Failed to create sitelink asset: {str(e)}"}

    campaign_asset_service = client.get_service("CampaignAssetService")
    campaign_asset_op = client.get_type("CampaignAssetOperation")
    ca = campaign_asset_op.create
    ca.asset = asset_resource_name
    ca.campaign = client.get_service("CampaignService").campaign_path(cid, campaign_id)
    ca.field_type = client.enums.AssetFieldTypeEnum.SITELINK

    try:
        ca_response = campaign_asset_service.mutate_campaign_assets(customer_id=cid, operations=[campaign_asset_op])
        return {
            "success": True,
            "asset_resource_name": asset_resource_name,
            "campaign_asset_resource_name": ca_response.results[0].resource_name,
        }
    except GoogleAdsException as e:
        return {"success": False, "error": f"Asset created but failed to link: {str(e)}"}


@mcp.tool()
def add_callout_to_campaign(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    campaign_id: str = Field(description="Campaign ID to attach the callout to"),
    callout_text: str = Field(description="Callout text shown in the ad (max 25 chars)")
) -> dict:
    """Create a callout asset and attach it to a campaign."""
    client = get_google_ads_client()
    cid = format_customer_id(customer_id)

    asset_service = client.get_service("AssetService")
    asset_op = client.get_type("AssetOperation")
    asset = asset_op.create
    asset.name = f"Callout: {callout_text}"
    asset.callout_asset.callout_text = callout_text

    try:
        asset_response = asset_service.mutate_assets(customer_id=cid, operations=[asset_op])
        asset_resource_name = asset_response.results[0].resource_name
    except GoogleAdsException as e:
        return {"success": False, "error": f"Failed to create callout asset: {str(e)}"}

    campaign_asset_service = client.get_service("CampaignAssetService")
    campaign_asset_op = client.get_type("CampaignAssetOperation")
    ca = campaign_asset_op.create
    ca.asset = asset_resource_name
    ca.campaign = client.get_service("CampaignService").campaign_path(cid, campaign_id)
    ca.field_type = client.enums.AssetFieldTypeEnum.CALLOUT

    try:
        ca_response = campaign_asset_service.mutate_campaign_assets(customer_id=cid, operations=[campaign_asset_op])
        return {
            "success": True,
            "asset_resource_name": asset_resource_name,
            "campaign_asset_resource_name": ca_response.results[0].resource_name,
        }
    except GoogleAdsException as e:
        return {"success": False, "error": f"Asset created but failed to link: {str(e)}"}


# ── Recommendations ───────────────────────────────────────────────────────────

@mcp.tool()
def apply_recommendation(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    recommendation_resource_name: str = Field(description="resource_name from get_recommendations, e.g. customers/123/recommendations/456")
) -> dict:
    """Apply a Google recommendation to the account."""
    client = get_google_ads_client()
    service = client.get_service("RecommendationService")
    cid = format_customer_id(customer_id)

    op = client.get_type("ApplyRecommendationOperation")
    op.resource_name = recommendation_resource_name

    try:
        response = service.apply_recommendations(customer_id=cid, operations=[op])
        return {"success": True, "applied": [r.resource_name for r in response.results]}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def dismiss_recommendation(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    recommendation_resource_name: str = Field(description="resource_name from get_recommendations, e.g. customers/123/recommendations/456")
) -> dict:
    """Dismiss a Google recommendation so it stops appearing in the account."""
    client = get_google_ads_client()
    service = client.get_service("RecommendationService")
    cid = format_customer_id(customer_id)

    try:
        service.dismiss_recommendations(
            customer_id=cid,
            operations=[{"resource_name": recommendation_resource_name}],
        )
        return {"success": True, "dismissed": recommendation_resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


# ── Conversion tracking ───────────────────────────────────────────────────────

@mcp.tool()
def create_conversion_action(
    customer_id: str = Field(description="Google Ads customer ID (10 digits, no dashes)"),
    name: str = Field(description="Conversion action name"),
    category: str = Field(default="PURCHASE", description="PURCHASE, LEAD, SIGNUP, PAGE_VIEW, DOWNLOAD, or OTHER"),
    conversion_type: str = Field(default="WEBPAGE", description="WEBPAGE, PHONE_CALL, APP_INSTALL, IMPORT, or UPLOAD_CLICKS"),
    default_value: float = Field(default=0.0, description="Default conversion value in account currency (0 = variable)"),
    counting_type: str = Field(default="ONE_PER_CLICK", description="ONE_PER_CLICK or MANY_PER_CLICK")
) -> dict:
    """Create a new conversion action for tracking goals (purchases, leads, sign-ups, etc.)."""
    client = get_google_ads_client()
    service = client.get_service("ConversionActionService")
    op = client.get_type("ConversionActionOperation")
    cid = format_customer_id(customer_id)

    ca = op.create
    ca.name = name
    ca.type_ = getattr(client.enums.ConversionActionTypeEnum, conversion_type.upper())
    ca.category = getattr(client.enums.ConversionActionCategoryEnum, category.upper())
    ca.status = client.enums.ConversionActionStatusEnum.ENABLED
    ca.counting_type = getattr(client.enums.ConversionActionCountingTypeEnum, counting_type.upper())
    ca.value_settings.default_value = default_value
    ca.value_settings.always_use_default_value = default_value > 0

    try:
        response = service.mutate_conversion_actions(customer_id=cid, operations=[op])
        return {"success": True, "resource_name": response.results[0].resource_name}
    except GoogleAdsException as e:
        return {"success": False, "error": str(e)}


def main():
    """Entry point for uvx / console_scripts."""
    mcp.run(transport="stdio")

if __name__ == "__main__":
    main()
