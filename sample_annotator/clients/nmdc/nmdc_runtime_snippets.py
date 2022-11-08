# todo from nmdc-runtime
import os
from datetime import datetime, timezone, timedelta

import requests
from dotenv import load_dotenv
from pydantic import BaseModel


def now(as_str=False):
    dt = datetime.now(timezone.utc)
    return dt.isoformat() if as_str else dt


def expiry_dt_from_now(days=0, hours=0, minutes=0, seconds=0):
    return now() + timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


def has_passed(dt):
    return now() > dt


class RuntimeApiSiteClient:
    def __init__(self, base_url: str, site_id: str, client_id: str, client_secret: str):
        self.base_url = base_url
        self.site_id = site_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.headers = {}
        self.token_response = None
        self.refresh_token_after = None
        self.get_token()

    def request(self, method, url_path, params_or_json_data=None):
        self.ensure_token()
        kwargs = {"url": self.base_url + url_path, "headers": self.headers}
        if isinstance(params_or_json_data, BaseModel):
            params_or_json_data = params_or_json_data.dict(exclude_unset=True)
        if method.upper() == "GET":
            kwargs["params"] = params_or_json_data
        else:
            kwargs["json"] = params_or_json_data
        return requests.request(method, **kwargs)

    def get_token(self):
        rv = requests.post(
            self.base_url + "/token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        self.token_response = rv.json()
        if "access_token" not in self.token_response:
            raise Exception(f"Getting token failed: {self.token_response}")

        self.headers["Authorization"] = f'Bearer {self.token_response["access_token"]}'
        self.refresh_token_after = expiry_dt_from_now(
            **self.token_response["expires"]
        ) - timedelta(seconds=5)

    def ensure_token(self):
        if has_passed(self.refresh_token_after):
            self.get_token()


# todo end nmdc-runtime copy/paste

# # test case
#
# load_dotenv("../../../local/.env")
#
# minting_client = RuntimeApiSiteClient(
#     base_url="https://api.dev.microbiomedata.org",
#     site_id=os.getenv('site_id'),
#     client_id=os.getenv('client_id'),
#     client_secret=os.getenv('client_secret'),
# )
#
# minting_params = {
#     "populator": "",
#     "naa": "nmdc",
#     "shoulder": "fk0",
#     "number": 3,
# }
#
# minting_response = minting_client.request(
#     "POST", "/ids/mint", params_or_json_data=minting_params
# )
#
# print(minting_response.json())
