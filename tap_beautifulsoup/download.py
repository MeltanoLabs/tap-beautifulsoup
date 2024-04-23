from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


def download(base_url: str, download_folder: Path, logger=None):
    visited_paths = set()

    response = requests.get(base_url)
    response.raise_for_status()

    parsed_base_url = urlparse(response.url)
    base_url = parsed_base_url.geturl()

    def _download_recursive(url: str, logger):
        parsed_url = urlparse(url)
        path = Path(parsed_url.path)

        if not url.startswith(base_url) or path in visited_paths:
            return

        try:
            response = requests.get(url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return
        finally:
            visited_paths.add(path)

        if (
            path.suffix != ".html"
            and "text/html" not in response.headers["Content-Type"]
        ):
            return

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a", href=True)

        for link in links:
            next_url = urljoin(response.url, link["href"])
            _download_recursive(next_url, logger)

        if not path.suffix:
            path = path / "index.html"

        file_path = (
            download_folder / parsed_base_url.netloc / path.relative_to(path.anchor)
        )

        os.makedirs(file_path.parent, exist_ok=True)
        file_path.write_bytes(response.content)

    _download_recursive(base_url, logger)
