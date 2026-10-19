"""HTML selection and structure-preserving conversion for fetched pages."""

from typing import Literal, Optional, cast
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup, Tag
from markdownify import MarkdownConverter


def _code_language(element: Tag) -> str:
    for tag in (element.find("code"), element):
        if isinstance(tag, Tag):
            for name in tag.get_attribute_list("class"):
                if name.startswith("language-"):
                    return name.removeprefix("language-")
    return ""


def _resolve_link(url: str, value: str) -> Optional[str]:
    try:
        resolved = urljoin(url, value)
        if urlsplit(resolved).scheme in ("http", "https", "mailto"):
            return resolved
    except ValueError:
        pass
    return None


def convert_html(
    html: str,
    url: str,
    output_format: Literal["markdown", "text"],
    main_content_only: bool,
) -> str:
    """Select readable HTML and resolve links before converting, without rendering JS."""
    soup = BeautifulSoup(html, "html.parser")
    base = soup.find("base", href=True)
    if isinstance(base, Tag) and isinstance(href := base.get("href"), str):
        candidate = _resolve_link(url, href)
        if candidate is not None and urlsplit(candidate).scheme in ("http", "https"):
            url = candidate
    for element in soup.select(
        'head, script, style, template, noscript, [hidden], [aria-hidden="true"]'
    ):
        element.decompose()
    if main_content_only:
        for element in soup.select('nav, aside, footer, [role="navigation"]'):
            element.decompose()
        root = (
            soup.select_one('main, [role="main"]')
            or soup.find("article")
            or soup.body
            or soup
        )
    else:
        root = soup.body or soup
    for element in root.select("a[href], img[src]"):
        if element.decomposed:
            continue
        if (
            element.name == "a"
            and not element.get_text().strip(" \t\r\n\u200b")
            and element.find("img") is None
        ):
            element.decompose()
            continue
        attribute = "href" if element.name == "a" else "src"
        value = element.get(attribute)
        if isinstance(value, str):
            resolved = _resolve_link(url, value)
            if resolved is not None:
                element[attribute] = resolved
            else:
                del element[attribute]
    if output_format == "text":
        return root.get_text(separator="\n").strip()
    converter = MarkdownConverter(
        heading_style="ATX", code_language_callback=_code_language, wrap=False
    )
    return cast(str, converter.convert_soup(root)).strip()
