import io
import json
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
import requests

from italian_our_world_data import (
    DataSourceError,
    discover_data,
    fetch_data,
    get_source_info,
    list_indicators,
    list_source_items,
    list_sources,
    source_info,
)
from italian_our_world_data.cli import main as cli_main


class Response:
    def __init__(self, *, text="", payload=None, content=None, status=200):
        self.text = text
        self._payload = payload
        self.content = content if content is not None else text.encode()
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status {self.status_code}")

    def json(self):
        return self._payload


class Session:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append((url, params, headers, timeout))
        return self.responses.pop(0)


class GatewayTests(unittest.TestCase):
    def test_sources_are_listed_with_gateway_functions(self):
        frame = list_sources()
        self.assertIn("istat", frame["source"].tolist())
        self.assertIn("world_bank", frame["source"].tolist())
        self.assertIn("ameco", frame["source"].tolist())
        self.assertIn("imf", frame["source"].tolist())
        self.assertIn("bis", frame["source"].tolist())
        self.assertIn("identifier_column", frame.columns)
        self.assertIn("fetch_data", source_info("istat")["example"])

    def test_source_info_supports_aliases(self):
        info = source_info("world-bank")
        self.assertEqual(info["source"], "world_bank")
        self.assertEqual(info["fetch_function"], "fetch_world_bank_data")
        self.assertEqual(info["item_name"], "indicator")
        self.assertEqual(info["identifier_column"], "indicator_id")
        self.assertEqual(info["fetch_parameter"], "indicator")
        self.assertIn("indicator", info["required"])
        self.assertEqual(get_source_info("wb")["source"], "world_bank")
        self.assertEqual(get_source_info("weo")["source"], "imf")
        self.assertEqual(get_source_info("ecfin")["source"], "ameco")

    def test_source_items_explain_identifier_columns_without_source(self):
        frame = list_source_items()
        world_bank = frame[frame["source"] == "world_bank"].iloc[0]
        self.assertEqual(world_bank["item_name"], "indicator")
        self.assertEqual(world_bank["identifier_column"], "indicator_id")
        self.assertEqual(world_bank["fetch_parameter"], "indicator")

    def test_fetch_data_dispatches_to_provider_function(self):
        session = Session(Response(text="TIME_PERIOD,OBS_VALUE\n2023,4.5\n"))
        frame = fetch_data("istat", dataflow_id="150_915", key=".......", session=session)
        self.assertEqual(frame.loc[0, "value"], 4.5)
        self.assertIn("/150_915/.......", session.calls[0][0])

    def test_fetch_data_accepts_aliases(self):
        session = Session(Response(payload=[{}, []]))
        frame = fetch_data("world-bank", indicator="NY.GDP.MKTP.CD", session=session)
        self.assertIsInstance(frame, pd.DataFrame)
        self.assertIn("/indicator/NY.GDP.MKTP.CD", session.calls[0][0])

    def test_discover_data_dispatches_to_listing_function(self):
        session = Session(Response(payload={"missioni": "https://openpnrr.it/api/v1/missioni"}))
        frame = discover_data("openpnrr", session=session)
        self.assertEqual(frame.loc[0, "resource"], "missioni")

    def test_list_indicators_dispatches_to_source_catalogue(self):
        payload = [
            {},
            [
                {
                    "id": "NY.GDP.MKTP.CD",
                    "name": "GDP",
                    "unit": "USD",
                    "source": {"value": "WDI"},
                }
            ],
        ]
        frame = list_indicators("world_bank", session=Session(Response(payload=payload)))
        self.assertEqual(frame.loc[0, "indicator_id"], "NY.GDP.MKTP.CD")

        session = Session(Response(payload={"missioni": "https://openpnrr.it/api/v1/missioni"}))
        self.assertEqual(list_source_items("openpnrr", session=session).loc[0, "resource"], "missioni")

    def test_unknown_source_has_clear_error(self):
        with self.assertRaisesRegex(ValueError, "Unknown source"):
            source_info("not-a-source")

    def test_fred_discovery_requires_search_credentials(self):
        with self.assertRaises(ValueError):
            discover_data("fred")

    def test_cli_lists_sources_and_prints_json_info(self):
        text = io.StringIO()
        with redirect_stdout(text):
            self.assertEqual(cli_main(["sources", "--format", "csv"]), 0)
        self.assertIn("source,name,category", text.getvalue())

        text = io.StringIO()
        with redirect_stdout(text):
            self.assertEqual(cli_main(["indicators", "--format", "csv"]), 0)
        self.assertIn("source,item_name,identifier_column", text.getvalue())

        text = io.StringIO()
        with redirect_stdout(text):
            self.assertEqual(cli_main(["info", "world-bank", "--format", "json"]), 0)
        payload = json.loads(text.getvalue())
        self.assertEqual(payload["source"], "world_bank")


if __name__ == "__main__":
    unittest.main()
