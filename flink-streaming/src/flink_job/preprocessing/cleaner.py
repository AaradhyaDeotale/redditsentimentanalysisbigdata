"""
Emoji-safe text cleaning for Reddit comments.
Avoids over-cleaning: keeps emoticons, repeated punctuation, subreddit slang.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# URLs (http/https/www)
_URL_RE = re.compile(
    r"https?://\S+|www\.\S+",
    re.IGNORECASE,
)

# Reddit markdown links [text](url) and bare /u/ /r/ references
_MD_LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]+\)")
_USER_SUB_RE = re.compile(r"/[ur]/\w+", re.IGNORECASE)

# HTML entities occasionally present in bodies
_HTML_ENTITY_RE = re.compile(r"&(?:amp|lt|gt|quot|#39);")

# Collapse excessive whitespace but keep newlines as spaces
_MULTI_SPACE_RE = re.compile(r"[^\S\n]+")


@dataclass
class TextCleaner:
    remove_urls: bool = True
    remove_markdown: bool = True
    lowercase: bool = False

    def clean(self, text: str) -> str:
        if not text:
            return ""

        cleaned = text
        cleaned = _HTML_ENTITY_RE.sub(" ", cleaned)

        if self.remove_markdown:
            cleaned = _MD_LINK_RE.sub(r"\1", cleaned)
            cleaned = _USER_SUB_RE.sub(" ", cleaned)

        if self.remove_urls:
            cleaned = _URL_RE.sub(" ", cleaned)

        cleaned = _MULTI_SPACE_RE.sub(" ", cleaned).strip()

        if self.lowercase:
            cleaned = cleaned.lower()

        return cleaned
