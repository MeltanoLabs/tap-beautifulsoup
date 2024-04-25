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
            description="The name of the source you're scraping. This will be used as the stream name.",
        ),
        th.Property(
            "site_url",
            th.StringType,
            required=True,
            description="The site you'd like to scrape. The tap will download all pages recursively into the output directory prior to parsing files.",
            examples=["https://sdk.meltano.com/en/latest/"],
        ),
        th.Property(
            "output_folder",
            th.StringType,
            default="output",
            description="The file path of where to write the intermediate downloaded HTML files to.",
        ),
        th.Property(
            "parser",
            th.StringType,
            default="html.parser",
            allowed_values=["html.parser"],
            description="The BeautifulSoup parser to use.",
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
            default={},
            description="This dict contains all the kwargs that should be passed to the [`find_all`](https://www.crummy.com/software/BeautifulSoup/bs4/doc/#find-all) call in order to extract text from the pages.",
            examples=[{"text": True}, {"attrs": {"role": "main"}}]
        ),
        th.Property(
            "exclude_tags",
            th.ArrayType(th.StringType),
            default=[],
            description="List of tags to exclude before extracting text content of the page.",
            examples=[["style"], ["header", "footer"]]
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
