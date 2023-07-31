from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


def download(url: str, download_folder: Path | str = "output", verbose: bool = False):
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    response = requests.get(url)
    url = response.url
    parsed_url =  urlparse(url)
    base_netloc = parsed_url.netloc
    visited_urls = set()
    visited_urls.add(parsed_url.path)

    def _download_recursive(url, base_url):
        if not url.startswith(base_url):
            return
        try:
            response = requests.get(url)
            response.raise_for_status()
            url = response.url
        except requests.exceptions.RequestException as e:
            print(f"Failed to fetch {url}: {e}")
            return

        soup = BeautifulSoup(response.text, "html.parser")
        links = soup.find_all("a", href=True)

        for link in links:
            href = link["href"]
            full_url = urljoin(url, href)
            parsed_url = urlparse(full_url)

            if parsed_url.path not in visited_urls and parsed_url.netloc == base_netloc:
                visited_urls.add(parsed_url.path)
                _download_recursive(full_url, base_url)

        parsed_url = urlparse(url)
        if parsed_url.path.endswith(".html"):
            filename = os.path.join(download_folder, base_netloc, parsed_url.path[1:])
            dirname = os.path.dirname(filename)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            with open(filename, "wb") as f:
                f.write(response.content)

    _download_recursive(url, url)
