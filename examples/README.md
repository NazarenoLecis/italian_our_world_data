# Examples

These scripts demonstrate use of the installed `italian_our_world_data`
package. Source names are lowercase directory names; scripts are not package
implementation modules and are not automated tests.

Install the library from the project root before running an example:

```bash
python3 -m pip install -e .
python3 examples/ecb/retrieve_ecb_data.py
python3 examples/fred/retrieve_fred_data.py
python3 examples/pnrr/retrieve_pnrr_data.py
```

Available example groups:

| Directory | Provider or purpose |
| --- | --- |
| `ameco/` | European Commission AMECO |
| `bankitalia/` | Bank of Italy exchange rates |
| `bdap/` | OpenBDAP public-finance catalogue |
| `bis/` | Bank for International Settlements |
| `ckan/` | Generic CKAN and national dati.gov.it catalogue |
| `ecb/` | European Central Bank |
| `eurostat/` | Eurostat |
| `fred/` | FRED public CSV download |
| `gateway/` | Unified source discovery and retrieval |
| `imf/` | IMF DataMapper |
| `inps/` | INPS catalogue and data retrieval |
| `istat/` | ISTAT retrieval |
| `oecd/` | OECD Data Explorer |
| `opencoesione/` | OpenCoesione API |
| `pnrr/` | OpenPNRR |
| `sdmx/` | Shared SDMX-oriented examples |
| `socrata/` | Generic Socrata and Regione Lombardia Open Data |
| `un_population/` | UN Population Data Portal catalogue |
| `world_bank/` | World Bank |

For the public API and dataset discovery workflow, see
[docs/API.md](../docs/API.md).
