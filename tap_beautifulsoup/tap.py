"""BeautifulSoup tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_beautifulsoup import streams


class TapBeautifulSoup(Tap):
    """BeautifulSoup tap class."""

    name = "tap-beautifulsoup"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "source_name",
            th.StringType,
            required=True,
        ),
        th.Property(
            "site_url",
            th.StringType,
            required=True,
            default="https://sdk.meltano.com/en/latest/",
        ),
        th.Property(
            "output_folder",
            th.StringType,
            required=True,
            default="output",
        ),
        th.Property(
            "parser",
            th.StringType,
            required=True,
            default="html.parser",
            allowed_values=["html.parser"],
        ),
        th.Property(
            "download_recursively",
            th.BooleanType,
            default=True,
            description="Attempt to download all pages recursively into the output directory prior to parsing files. Set this to False if you've previously run `wget -r -A.html https://sdk.meltano.com/en/latest/`",
        ),
        th.Property(
            "find_all_kwargs",
            th.ObjectType(),
            description="This dict contains all the kwargs that should be passed to the [`find_all`](https://www.crummy.com/software/BeautifulSoup/bs4/doc/#find-all) call in order to extract text from the pages."
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.BeautifulSoupStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.GenericBSStream(self),
        ]


if __name__ == "__main__":
    TapBeautifulSoup.cli()
