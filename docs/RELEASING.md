# Publishing And Installing

## Installation Channels

Users can install the current GitHub repository as soon as this package code
is on its public `main` branch:

```bash
python3 -m pip install "git+https://github.com/NazarenoLecis/italian_our_world_data.git"
```

Once a release is uploaded to PyPI, users can install by distribution name:

```bash
python3 -m pip install italian-our-world-data
```

The distribution name uses hyphens for installation. The Python import uses
underscores:

```python
import italian_our_world_data
```

## One-Time PyPI Setup

At the time this documentation was added, `italian-our-world-data` was not
published on PyPI. The first successful upload claims that distribution
name, provided nobody publishes it first.

1. Choose an open-source license and add a `LICENSE` file if you intend
   others to reuse or redistribute this project. This choice must be made by
   the copyright owner.
2. Create or sign in to an account at [PyPI](https://pypi.org/).
3. In PyPI, add a pending Trusted Publisher for a new project.
4. Use these publisher values:

| PyPI field | Value |
| --- | --- |
| PyPI project name | `italian-our-world-data` |
| GitHub owner | `NazarenoLecis` |
| GitHub repository | `italian_our_world_data` |
| Workflow name | `publish.yml` |
| Environment name | `pypi` |

The workflow uses OpenID Connect Trusted Publishing, so no PyPI password or
API token needs to be stored in GitHub.

## Release Procedure

1. Update `version` in `pyproject.toml` and `__version__` in
   `italian_our_world_data/__init__.py` to the same new value.
2. Commit the version change and push it to `main`.
3. On GitHub, create a release with a version tag matching that value, such
   as `v2.1.0`.
4. The `Publish package to PyPI` workflow builds, validates, tests, and
   publishes the distributions to PyPI.
5. Verify the published install in a fresh environment:

```bash
python3 -m venv /tmp/italian-our-world-data-test
/tmp/italian-our-world-data-test/bin/python -m pip install italian-our-world-data
/tmp/italian-our-world-data-test/bin/python -c "import italian_our_world_data; print(italian_our_world_data.__version__)"
```

## GitHub Tag Installation

Before PyPI publishing, or when selecting a precise source revision, users
can install a tagged repository release:

```bash
python3 -m pip install "git+https://github.com/NazarenoLecis/italian_our_world_data.git@v2.1.0"
```

Create the tag first by publishing the corresponding GitHub release.
