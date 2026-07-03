"""
multilingual_labeler.py
-----------------------
Language-aware weak labelling for the sentiment corpus.

VADER is English-only, so non-English comments score ~0 and get dropped as
neutral. This labeler routes each comment to a lexicon that matches its
DETECTED language (the ``language`` field the Flink pipeline already attaches):

  * ``en`` / unknown  -> VADER (unchanged behaviour)
  * de, fr, es, nl, it, pt -> per-language sentiment word lists

Emoji / emoticon sentiment is language-INDEPENDENT, so it is applied on top of
the word lists to recover signal on short non-English comments.

Compliance note: this is still LABELLING ONLY, using lexicons — exactly the
pattern the project brief permits. The deployed classifier is the Logistic
Regression we train ourselves on these labels; see [[sec:ownmodel]].

The built-in ``_SEED`` word lists work offline with zero setup (good enough for
a demo and the tests). For higher-quality labels, drop fuller lists into a
directory and pass ``lexicon_dir`` — one file per language named
``<lang>.txt`` with lines ``word<TAB>pos`` or ``word<TAB>neg`` (the NRC Emotion
Lexicon exports cleanly to this shape). File entries are merged over the seeds.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ml_model.labeling.lexicon_labeler import (
    LabelResult,
    LexiconLabeler,
    NEGATIVE,
    NEUTRAL,
    POSITIVE,
    pick_text,
)

log = logging.getLogger("ml_model.labeling.multilingual_labeler")

# Languages we carry a word list for (English is handled by VADER separately).
SUPPORTED_LANGUAGES = frozenset({"de", "fr", "es", "nl", "it", "pt"})

# ── Built-in seed word lists ─────────────────────────────────────────────────
# Small, curated positive/negative sets per language. Not exhaustive — enough
# to give real multilingual signal out of the box; extend via `lexicon_dir`.
_SEED: dict[str, dict[str, set[str]]] = {
    "de": {
        "pos": {"gut", "super", "toll", "großartig", "ausgezeichnet", "liebe",
                "wunderbar", "perfekt", "schön", "fantastisch", "genial",
                "klasse", "danke", "freude", "glücklich", "empfehlen", "spitze",
                "hervorragend", "beste", "positiv"},
        "neg": {"schlecht", "schrecklich", "furchtbar", "hasse", "mist",
                "enttäuscht", "problem", "fehler", "katastrophe", "nutzlos",
                "ärgerlich", "traurig", "schlimm", "langweilig", "kaputt",
                "dumm", "negativ", "schwach"},
    },
    "fr": {
        "pos": {"bon", "bien", "super", "génial", "excellent", "aime", "adore",
                "parfait", "magnifique", "merci", "heureux", "formidable",
                "agréable", "meilleur", "bravo", "incroyable", "satisfait",
                "top", "beau"},
        "neg": {"mauvais", "terrible", "horrible", "déteste", "nul", "problème",
                "erreur", "déçu", "catastrophe", "inutile", "triste", "pire",
                "ennuyeux", "cassé", "stupide", "affreux", "décevant"},
    },
    "es": {
        "pos": {"bueno", "bien", "genial", "excelente", "amor", "encanta",
                "perfecto", "maravilloso", "gracias", "feliz", "increíble",
                "mejor", "fantástico", "bonito", "recomiendo", "contento"},
        "neg": {"malo", "terrible", "horrible", "odio", "pésimo", "problema",
                "error", "decepcionado", "basura", "inútil", "triste", "peor",
                "aburrido", "roto", "estúpido", "feo"},
    },
    "nl": {
        "pos": {"goed", "geweldig", "mooi", "prima", "fantastisch", "houd",
                "leuk", "perfect", "bedankt", "blij", "top", "uitstekend",
                "beste", "aanrader", "fijn"},
        "neg": {"slecht", "verschrikkelijk", "vreselijk", "haat", "waardeloos",
                "probleem", "fout", "teleurgesteld", "ramp", "nutteloos",
                "verdrietig", "erger", "saai", "kapot", "dom"},
    },
    "it": {
        "pos": {"buono", "bene", "ottimo", "fantastico", "amo", "adoro",
                "perfetto", "meraviglioso", "grazie", "felice", "incredibile",
                "migliore", "bello", "consiglio", "contento"},
        "neg": {"cattivo", "terribile", "orribile", "odio", "pessimo",
                "problema", "errore", "deluso", "disastro", "inutile", "triste",
                "peggio", "noioso", "rotto", "stupido", "brutto"},
    },
    "pt": {
        "pos": {"bom", "bem", "ótimo", "excelente", "amo", "adoro", "perfeito",
                "maravilhoso", "obrigado", "feliz", "incrível", "melhor",
                "lindo", "recomendo", "contente"},
        "neg": {"ruim", "terrível", "horrível", "odeio", "péssimo", "problema",
                "erro", "decepcionado", "desastre", "inútil", "triste", "pior",
                "chato", "quebrado", "estúpido", "feio"},
    },
}

# ── Language-independent emoji / emoticon sentiment ──────────────────────────
# The tokenizer emits each emoji and emoticon as its own token, so these match
# directly against a comment's token list regardless of language.
_EMOJI_SENTIMENT: dict[str, int] = {
    # positive
    "😍": 1, "😊": 1, "😃": 1, "😄": 1, "😁": 1, "🙂": 1, "😀": 1, "🥰": 1,
    "😻": 1, "❤️": 1, "❤": 1, "👍": 1, "💯": 1, "🎉": 1, "✨": 1, "😘": 1,
    ":)": 1, ":-)": 1, ":d": 1, ":-d": 1, "=)": 1,
    # negative
    "😠": -1, "😡": -1, "😤": -1, "😢": -1, "😭": -1, "😞": -1, "😔": -1,
    "😩": -1, "👎": -1, "💔": -1, "🤮": -1, "🙁": -1,
    ":(": -1, ":-(": -1, ":'(": -1, "=(": -1,
}


def _load_lexicon_dir(path: str) -> dict[str, dict[str, set[str]]]:
    """Load optional per-language word lists from `<lang>.txt` files.

    Each line is ``word<TAB>pos`` or ``word<TAB>neg`` (case-insensitive tag);
    malformed lines are skipped. Returns {lang: {"pos": {...}, "neg": {...}}}.
    """
    loaded: dict[str, dict[str, set[str]]] = {}
    if not os.path.isdir(path):
        log.warning("lexicon_dir %s not found — using built-in seed lists only", path)
        return loaded
    for lang in SUPPORTED_LANGUAGES:
        fpath = os.path.join(path, f"{lang}.txt")
        if not os.path.isfile(fpath):
            continue
        pos: set[str] = set()
        neg: set[str] = set()
        with open(fpath, "r", encoding="utf-8") as fh:
            for line in fh:
                parts = line.strip().split("\t")
                if len(parts) != 2 or not parts[0]:
                    continue
                word, tag = parts[0].strip().lower(), parts[1].strip().lower()
                if tag.startswith("pos"):
                    pos.add(word)
                elif tag.startswith("neg"):
                    neg.add(word)
        if pos or neg:
            loaded[lang] = {"pos": pos, "neg": neg}
            log.info("Loaded %s lexicon: %d pos / %d neg", lang, len(pos), len(neg))
    return loaded


class MultilingualLexiconLabeler:
    """Language-keyed weak labeler.

    Same interface as :class:`LexiconLabeler` (``neutral_band``, ``label``,
    ``label_record``) so it is a drop-in replacement in ``label_corpus.py``.
    """

    def __init__(self, neutral_band: float = 0.05, lexicon_dir: str | None = None):
        if neutral_band < 0:
            raise ValueError("neutral_band must be >= 0")
        self._neutral_band = neutral_band
        # English (and unknown/unsupported) go through VADER, unchanged.
        self._english = LexiconLabeler(neutral_band=neutral_band)

        # Merge optional file lists over the built-in seeds.
        self._lex: dict[str, dict[str, set[str]]] = {
            lang: {"pos": set(sets["pos"]), "neg": set(sets["neg"])}
            for lang, sets in _SEED.items()
        }
        if lexicon_dir:
            for lang, sets in _load_lexicon_dir(lexicon_dir).items():
                self._lex.setdefault(lang, {"pos": set(), "neg": set()})
                self._lex[lang]["pos"] |= sets["pos"]
                self._lex[lang]["neg"] |= sets["neg"]

    @property
    def neutral_band(self) -> float:
        return self._neutral_band

    # ── scoring ──────────────────────────────────────────────────────────────
    def _score_tokens(self, tokens: list[str], language: str) -> float:
        """Compound score in [-1, 1] from word-list + emoji hits."""
        lex = self._lex.get(language)
        pos = neg = 0
        for raw in tokens:
            tok = str(raw).lower()
            if lex is not None:
                if tok in lex["pos"]:
                    pos += 1
                    continue
                if tok in lex["neg"]:
                    neg += 1
                    continue
            emo = _EMOJI_SENTIMENT.get(tok)
            if emo == 1:
                pos += 1
            elif emo == -1:
                neg += 1
        total = pos + neg
        if total == 0:
            return 0.0
        return (pos - neg) / total

    def _to_label(self, compound: float) -> str:
        if compound >= self._neutral_band:
            return POSITIVE
        if compound <= -self._neutral_band:
            return NEGATIVE
        return NEUTRAL

    # ── public API (mirrors LexiconLabeler) ──────────────────────────────────
    def label_record(self, record: dict[str, Any]) -> LabelResult:
        """Label a cleaned-comment record, routing by its detected language."""
        language = (record.get("language") or "unknown").lower()
        if language not in SUPPORTED_LANGUAGES:
            # English, unknown, or a language we have no list for -> VADER.
            return self._english.label_record(record)

        tokens = record.get("tokens")
        if not isinstance(tokens, list) or not tokens:
            tokens = pick_text(record).split()
        compound = self._score_tokens([str(t) for t in tokens], language)
        return LabelResult(label=self._to_label(compound), compound=compound,
                           source="lexicon")

    def label(self, text: str, language: str = "en") -> LabelResult:
        """Label raw text for a given language (convenience / testing)."""
        language = (language or "unknown").lower()
        if language not in SUPPORTED_LANGUAGES:
            return self._english.label(text)
        compound = self._score_tokens(text.split(), language)
        return LabelResult(label=self._to_label(compound), compound=compound,
                           source="lexicon")
