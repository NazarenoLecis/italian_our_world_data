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
    fetch_bankitalia_exchange_rates,
    fetch_bdap_data,
    fetch_ckan_resource,
    fetch_ecb_data,
    fetch_eurostat_data,
    fetch_fred_data,
    fetch_inps_data,
    fetch_italian_open_data_resource,
    fetch_istat_data,
    fetch_lombardy_data,
    fetch_oecd_data,
    fetch_opencoesione_data,
    fetch_pnrr_data,
    fetch_socrata_data,
    fetch_world_bank_data,
    fetch_administrative_boundaries,
    fetch_administrative_boundary_metadata,
    get_bdap_dataset_metadata,
    get_ckan_dataset_metadata,
    get_ckan_resource_metadata,
    get_inps_dataset_metadata,
    get_italian_open_data_dataset_metadata,
    get_lombardy_dataset_metadata,
    get_socrata_dataset_metadata,
    list_administrative_boundary_divisions,
    list_bankitalia_currencies,
    list_bdap_datasets,
    list_ckan_datasets,
    list_ecb_dataflows,
    list_eurostat_dataflows,
    list_inps_datasets,
    list_italian_open_data_datasets,
    list_istat_dataflows,
    list_lombardy_datasets,
    list_oecd_dataflows,
    list_opencoesione_resources,
    list_pnrr_resources,
    list_socrata_datasets,
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

    def test_ckan_lists_metadata_and_downloads_resources(self):
        dataset = {
            "name": "dataset-one",
            "id": "uuid",
            "title": "Dataset One",
            "organization": {"title": "Owner"},
            "license_id": "cc-by",
            "resources": [{"format": "CSV", "url": "https://files/data.csv"}],
            "metadata_modified": "2026-01-01",
        }
        listing = Session(Response(payload={"success": True, "result": {"results": [dataset]}}))
        frame = list_ckan_datasets("https://catalogue.test", query="budget", session=listing)
        self.assertEqual(frame.loc[0, "dataset_id"], "dataset-one")
        self.assertEqual(frame.loc[0, "organization"], "Owner")
        self.assertIn("/api/3/action/package_search", listing.calls[0][0])
        self.assertEqual(listing.calls[0][1]["q"], "budget")

        metadata_session = Session(Response(payload={"success": True, "result": dataset}))
        metadata = get_ckan_dataset_metadata(
            "https://catalogue.test/api/3/action", "dataset-one", session=metadata_session
        )
        self.assertEqual(metadata["title"], "Dataset One")
        self.assertIn("/package_show", metadata_session.calls[0][0])

        resource_metadata = {"format": "JSON", "url": "https://files/data.json"}
        resource_session = Session(Response(payload={"success": True, "result": resource_metadata}))
        self.assertEqual(
            get_ckan_resource_metadata("https://catalogue.test", "res1", session=resource_session)[
                "format"
            ],
            "JSON",
        )

        download = Session(
            Response(payload={"success": True, "result": dataset}),
            Response(content=b"a;b\n1;2\n", text=None),
        )
        data = fetch_ckan_resource("https://catalogue.test", dataset_id="dataset-one", session=download)
        self.assertEqual(data.loc[0, "b"], 2)

    def test_named_ckan_wrappers_use_expected_catalogues(self):
        dataset = {"name": "one", "resources": [{"format": "CSV", "url": "https://files/data.csv"}]}
        listing = Session(Response(payload={"success": True, "result": {"results": [dataset]}}))
        self.assertEqual(list_italian_open_data_datasets(session=listing).loc[0, "dataset_id"], "one")
        self.assertIn("dati.gov.it", listing.calls[0][0])

        metadata = Session(Response(payload={"success": True, "result": dataset}))
        self.assertIn(
            "resources",
            get_italian_open_data_dataset_metadata("one", session=metadata),
        )

        download = Session(
            Response(payload={"success": True, "result": dataset}),
            Response(content=b"x,y\n3,4\n", text=None),
        )
        self.assertEqual(
            fetch_italian_open_data_resource(dataset_id="one", session=download).loc[0, "x"],
            3,
        )

        bdap_listing = Session(Response(payload={"success": True, "result": {"results": [dataset]}}))
        self.assertEqual(list_bdap_datasets(session=bdap_listing).loc[0, "dataset_id"], "one")
        self.assertIn("bdap-opendata.rgs.mef.gov.it", bdap_listing.calls[0][0])

        bdap_metadata = Session(Response(payload={"success": True, "result": dataset}))
        self.assertIn("resources", get_bdap_dataset_metadata("one", session=bdap_metadata))

        bdap_download = Session(
            Response(payload={"success": True, "result": dataset}),
            Response(content=b"x,y\n5,6\n", text=None),
        )
        self.assertEqual(fetch_bdap_data(dataset_id="one", session=bdap_download).loc[0, "y"], 6)

    def test_socrata_lists_metadata_and_fetches_rows(self):
        listing_payload = [
            {
                "id": "abcd-1234",
                "name": "Sensors",
                "assetType": "dataset",
                "category": "Environment",
                "rowsUpdatedAt": 1770000000,
            }
        ]
        listing = Session(Response(payload=listing_payload))
        frame = list_socrata_datasets("https://socrata.test", limit=1, session=listing)
        self.assertEqual(frame.loc[0, "dataset_id"], "abcd-1234")
        self.assertIn("/api/views.json", listing.calls[0][0])
        self.assertEqual(listing.calls[0][1]["limit"], 1)

        metadata = get_socrata_dataset_metadata(
            "https://socrata.test",
            "abcd-1234",
            session=Session(Response(payload={"id": "abcd-1234", "name": "Sensors"})),
        )
        self.assertEqual(metadata["name"], "Sensors")

        rows = Session(Response(payload=[{"data": "2026-02-01", "valore": "24.3"}]))
        data = fetch_socrata_data("https://socrata.test", "abcd-1234", limit=2, session=rows)
        self.assertEqual(data.loc[0, "valore"], "24.3")
        self.assertEqual(rows.calls[0][1]["$limit"], 2)

    def test_lombardy_wrappers_use_socrata_portal(self):
        listing = Session(Response(payload=[{"id": "abcd-1234", "name": "Sensors"}]))
        self.assertEqual(list_lombardy_datasets(session=listing).loc[0, "dataset_id"], "abcd-1234")
        self.assertIn("dati.lombardia.it", listing.calls[0][0])

        metadata = get_lombardy_dataset_metadata(
            "abcd-1234", session=Session(Response(payload={"id": "abcd-1234"}))
        )
        self.assertEqual(metadata["id"], "abcd-1234")

        rows = Session(Response(payload=[{"idsensore": "1", "valore": "24.3"}]))
        frame = fetch_lombardy_data("abcd-1234", limit=1, session=rows)
        self.assertEqual(frame.loc[0, "idsensore"], "1")

    def test_opencoesione_lists_resources_and_paginates(self):
        listing = Session(Response(payload={"temi": "https://opencoesione.gov.it/it/api/temi/"}))
        self.assertEqual(list_opencoesione_resources(session=listing).loc[0, "resource"], "temi")
        session = Session(
            Response(payload={"results": [{"codice": "01"}], "next": "https://next"}),
            Response(payload={"results": [{"codice": "02"}], "next": None}),
        )
        frame = fetch_opencoesione_data("temi", fetch_all_pages=True, session=session)
        self.assertEqual(frame["codice"].tolist(), ["01", "02"])

    def test_bankitalia_currencies_and_exchange_rates(self):
        currencies_payload = {
            "currencies": [
                {
                    "isoCode": "USD",
                    "name": "U.S. Dollar",
                    "graph": True,
                    "countries": [
                        {
                            "country": "UNITED STATES",
                            "countryISO": "US",
                            "validityStartDate": "1918-02-01",
                        }
                    ],
                }
            ]
        }
        currencies = list_bankitalia_currencies(session=Session(Response(payload=currencies_payload)))
        self.assertEqual(currencies.loc[0, "currency_code"], "USD")
        self.assertEqual(currencies.loc[0, "country_iso"], "US")

        daily_payload = {
            "rates": [
                {
                    "isoCode": "USD",
                    "currency": "U.S. Dollar",
                    "avgRate": "1.0545",
                    "referenceDate": "2023-01-03",
                }
            ]
        }
        session = Session(Response(payload=daily_payload))
        frame = fetch_bankitalia_exchange_rates(
            reference_date="2023-01-03",
            base_currency="EUR",
            target_currency="USD",
            session=session,
        )
        self.assertEqual(frame.loc[0, "value"], 1.0545)
        self.assertEqual(frame.loc[0, "time_period"], "2023-01-03")
        self.assertEqual(session.calls[0][1]["currencyIsoCode"], "EUR")
        self.assertEqual(session.calls[0][1]["baseCurrencyIsoCode"], "USD")

        latest_payload = {"latestRates": [{"isoCode": "USD", "eurRate": "1.1000"}]}
        latest = fetch_bankitalia_exchange_rates(
            target_currency="USD", session=Session(Response(payload=latest_payload))
        )
        self.assertEqual(latest.loc[0, "eur_rate"], 1.1)

    def test_http_failures_are_source_errors(self):
        with self.assertRaises(DataSourceError):
            fetch_ecb_data("EXR", session=Session(Response(status=500)))


if __name__ == "__main__":
    unittest.main()
