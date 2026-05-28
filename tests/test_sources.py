import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import geopandas  # noqa: F401
import pandas as pd
import requests

from italian_our_world_data import (
    DataSourceError,
    attach_administrative_boundaries,
    fetch_ecb_data,
    fetch_eurostat_data,
    fetch_fred_data,
    fetch_inps_data,
    fetch_istat_data,
    fetch_oecd_data,
    fetch_pnrr_data,
    fetch_world_bank_data,
    fetch_administrative_boundaries,
    fetch_administrative_boundary_metadata,
    get_inps_dataset_metadata,
    list_administrative_boundary_divisions,
    list_ecb_dataflows,
    list_eurostat_dataflows,
    list_inps_datasets,
    list_istat_dataflows,
    list_oecd_dataflows,
    list_pnrr_resources,
    list_world_bank_indicators,
    search_fred_series,
)

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


class SourceTests(unittest.TestCase):
    csv_text = "TIME_PERIOD,OBS_VALUE,FREQ\n2023,4.5,A\n"
    boundary_geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"cod_reg": 3, "den_reg": "Lombardia"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [[9.0, 45.0], [10.0, 45.0], [10.0, 46.0], [9.0, 46.0], [9.0, 45.0]]
                    ],
                },
            }
        ],
    }

    def test_administrative_boundary_divisions_are_listed(self):
        frame = list_administrative_boundary_divisions()
        self.assertIn("regioni", frame["division"].tolist())

    def test_administrative_boundary_metadata_is_loaded(self):
        session = Session(Response(payload=[{"COD_REG": "03", "DEN_REG": "Lombardia"}]))
        frame = fetch_administrative_boundary_metadata("regioni", session=session)
        self.assertEqual(frame.loc[0, "DEN_REG"], "Lombardia")
        self.assertIn("/latest/regioni.json", session.calls[0][0])

    def test_administrative_boundaries_are_geodataframes(self):
        session = Session(Response(payload=self.boundary_geojson))
        frame = fetch_administrative_boundaries("regioni", session=session)
        self.assertEqual(frame.loc[0, "den_reg"], "Lombardia")
        self.assertEqual(frame.crs.to_string(), "EPSG:4326")
        self.assertIn("geometry", frame.columns)
        self.assertIn("/latest/regioni.geo.json", session.calls[0][0])

    def test_administrative_boundary_release_can_be_pinned(self):
        session = Session(Response(payload=self.boundary_geojson))
        fetch_administrative_boundaries("regioni", release="20200101", session=session)
        self.assertIn("/20200101/regioni.geo.json", session.calls[0][0])

    def test_attach_administrative_boundaries_joins_data(self):
        session = Session(Response(payload=self.boundary_geojson))
        data = pd.DataFrame({"region_code": ["3"], "value": [10]})
        frame = attach_administrative_boundaries(
            data,
            division="regioni",
            data_key="region_code",
            boundary_key="cod_reg",
            session=session,
        )
        self.assertEqual(frame.loc[0, "value"], 10)
        self.assertEqual(frame.loc[0, "den_reg"], "Lombardia")

    def test_istat_csv_is_normalised(self):
        session = Session(Response(text=self.csv_text))
        frame = fetch_istat_data("150_915", ".......", start_period="2023", session=session)
        self.assertEqual(frame.loc[0, "value"], 4.5)
        self.assertIn("/150_915/.......", session.calls[0][0])
        self.assertEqual(session.calls[0][1]["startPeriod"], "2023")

    def test_sdmx_json_catalogues_return_dataflows(self):
        payload = {
            "data": {
                "dataflows": [
                    {"agencyID": "IT1", "id": "150_915", "version": "1.2", "name": "Work"}
                ]
            }
        }
        istat = list_istat_dataflows(session=Session(Response(payload=payload)))
        oecd = list_oecd_dataflows(session=Session(Response(payload=payload)))
        self.assertEqual(istat.loc[0, "dataflow_id"], "150_915")
        self.assertEqual(oecd.loc[0, "name"], "Work")

    def test_oecd_uses_modern_sdmx_dataflow_and_key(self):
        session = Session(Response(text=self.csv_text))
        frame = fetch_oecd_data("OECD.TEST,FLOW,", "..ITA", end_period="2024", session=session)
        self.assertEqual(frame.loc[0, "time_period"], "2023")
        self.assertIn("OECD.TEST,FLOW,", session.calls[0][0])
        self.assertEqual(session.calls[0][1]["endPeriod"], "2024")

    def test_eurostat_decodes_jsonstat(self):
        payload = {
            "id": ["geo", "time"],
            "size": [1, 2],
            "dimension": {
                "geo": {"category": {"index": {"IT": 0}}},
                "time": {"category": {"index": {"2022": 0, "2023": 1}}},
            },
            "value": {"0": 1.25, "1": 1.5},
        }
        session = Session(Response(payload=payload))
        frame = fetch_eurostat_data(
            "nama_10_gdp", filters={"geo": "IT"}, start_period="2022", session=session
        )
        self.assertEqual(frame["value"].tolist(), [1.25, 1.5])
        self.assertEqual(frame["time_period"].tolist(), ["2022", "2023"])
        self.assertEqual(session.calls[0][1]["sinceTimePeriod"], "2022")

    def test_ecb_uses_new_data_portal_endpoint(self):
        session = Session(Response(text=self.csv_text))
        frame = fetch_ecb_data("EXR", "D.USD.EUR.SP00.A", session=session)
        self.assertEqual(frame.loc[0, "value"], 4.5)
        self.assertTrue(session.calls[0][0].startswith("https://data-api.ecb.europa.eu/"))
        self.assertEqual(session.calls[0][1]["format"], "csvdata")

    def test_xml_sdmx_catalogues_return_dataflows(self):
        xml = b"""<m:Structure xmlns:m="message" xmlns:s="structure">
        <s:Dataflow agencyID="ECB" id="EXR" version="1.0"><s:Name xml:lang="de">Kurse</s:Name><s:Name xml:lang="en">Rates</s:Name></s:Dataflow>
        </m:Structure>"""
        ecb = list_ecb_dataflows(session=Session(Response(content=xml)))
        estat = list_eurostat_dataflows("nama_10_gdp", session=Session(Response(content=xml)))
        self.assertEqual(ecb.loc[0, "dataflow_id"], "EXR")
        self.assertEqual(estat.loc[0, "name"], "Rates")

    def test_world_bank_reads_all_pages(self):
        item1 = {"country": {"id": "IT", "value": "Italy"}, "indicator": {"id": "X", "value": "X"}, "date": "2023", "value": 1}
        item2 = {"country": {"id": "IT", "value": "Italy"}, "indicator": {"id": "X", "value": "X"}, "date": "2022", "value": 2}
        session = Session(Response(payload=[{"pages": 2}, [item1]]), Response(payload=[{}, [item2]]))
        frame = fetch_world_bank_data("X", session=session)
        self.assertEqual(frame["value"].tolist(), [1, 2])
        self.assertEqual(len(session.calls), 2)

    def test_world_bank_indicator_catalogue_is_listed(self):
        item = {"id": "NY.GDP", "name": "GDP", "unit": "USD", "source": {"value": "WDI"}}
        frame = list_world_bank_indicators(session=Session(Response(payload=[{}, [item]])))
        self.assertEqual(frame.loc[0, "indicator_id"], "NY.GDP")

    def test_fred_without_key_downloads_public_series_csv(self):
        session = Session(Response(text="observation_date,GDP\n2023-01-01,4.5\n"))
        frame = fetch_fred_data(
            "GDP", start_period="2023-01-01", end_period="2023-12-31", session=session
        )
        self.assertEqual(frame.loc[0, "value"], 4.5)
        self.assertEqual(frame.loc[0, "time_period"], "2023-01-01")
        self.assertIn("fredgraph.csv", session.calls[0][0])
        self.assertEqual(session.calls[0][1]["cosd"], "2023-01-01")

    def test_fred_with_key_uses_api_and_parses_observations(self):
        session = Session(Response(payload={"observations": [{"date": "2023-01-01", "value": "."}]}))
        frame = fetch_fred_data("GDP", "secret", session=session)
        self.assertTrue(pd.isna(frame.loc[0, "value"]))
        self.assertIn("/fred/series/observations", session.calls[0][0])

    def test_fred_series_search_has_explicit_key_requirement(self):
        with self.assertRaises(ValueError):
            search_fred_series("gdp", api_key=None)
        payload = {"seriess": [{"id": "GDP", "title": "GDP"}]}
        frame = search_fred_series("gdp", "secret", session=Session(Response(payload=payload)))
        self.assertEqual(frame.loc[0, "series_id"], "GDP")

    def test_inps_lists_and_loads_tabular_resource(self):
        listing = Session(Response(payload={"result": ["one", "two"]}))
        self.assertEqual(list_inps_datasets(session=listing)["dataset_id"].tolist(), ["one", "two"])
        metadata = {"result": {"resources": [{"format": "CSV", "url": "https://files/data.csv"}]}}
        download = Session(Response(payload=metadata), Response(content=b"a;b\n1;2\n", text=None))
        self.assertIn("resources", get_inps_dataset_metadata("one", session=Session(Response(payload=metadata))))
        frame = fetch_inps_data("one", session=download)
        self.assertEqual(frame.loc[0, "b"], 2)

    def test_pnrr_lists_resources_and_paginates(self):
        listing = Session(Response(payload={"missioni": "https://openpnrr.it/api/v1/missioni"}))
        self.assertEqual(list_pnrr_resources(session=listing).loc[0, "resource"], "missioni")
        session = Session(
            Response(payload={"results": [{"id": 1}], "next": "https://next"}),
            Response(payload={"results": [{"id": 2}], "next": None}),
        )
        frame = fetch_pnrr_data("missioni", fetch_all_pages=True, session=session)
        self.assertEqual(frame["id"].tolist(), [1, 2])

    def test_http_failures_are_source_errors(self):
        with self.assertRaises(DataSourceError):
            fetch_ecb_data("EXR", session=Session(Response(status=500)))


if __name__ == "__main__":
    unittest.main()
