import requests
from typing import Dict, List, Optional, Tuple, Union
import random
import pandas as pd
import yfinance as yf
from datetime import datetime
import re


def get_user_agent() -> str:
    """Get a not very random user agent."""
    user_agent_strings = [
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:86.1) Gecko/20100101 Firefox/86.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:86.1) Gecko/20100101 Firefox/86.1",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:82.1) Gecko/20100101 Firefox/82.1",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:86.0) Gecko/20100101 Firefox/86.0",
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.10; rv:83.0) Gecko/20100101 Firefox/83.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:84.0) Gecko/20100101 Firefox/84.0",
    ]

    return random.choice(user_agent_strings)  # nosec

# Write an abstract helper to make requests from a url with potential headers and params
def request(
    url: str, method: str = "GET", timeout: int = 10, **kwargs
) -> requests.Response:
    """Abstract helper to make requests from a url with potential headers and params.

    Parameters
    ----------
    url : str
        Url to make the request to
    method : str, optional
        HTTP method to use.  Can be "GET" or "POST", by default "GET"

    Returns
    -------
    requests.Response
        Request response object

    Raises
    ------
    ValueError
        If invalid method is passed
    """
    # We want to add a user agent to the request, so check if there are any headers
    # If there are headers, check if there is a user agent, if not add one.
    # Some requests seem to work only with a specific user agent, so we want to be able to override it.
    headers = kwargs.pop("headers", {})

    if "User-Agent" not in headers:
        headers["User-Agent"] = get_user_agent()
    if method.upper() == "GET":
        return requests.get(
            url,
            headers=headers,
            timeout=timeout,
            **kwargs,
        )
    if method.upper() == "POST":
        return requests.post(
            url,
            headers=headers,
            timeout=timeout,
            **kwargs,
        )
    raise ValueError("Method must be GET or POST")

def get_df(url: str, header: Optional[int] = None) -> pd.DataFrame:
    headers = {"User-Agent": get_user_agent()}
    html = request(url, headers=headers).text
    # use regex to replace radio button html entries
    html_clean = re.sub(r"(<span class=\"Fz\(0\)\">).*?(</span>)", "", html)
    dfs = pd.read_html(html_clean, header=header)
    return dfs