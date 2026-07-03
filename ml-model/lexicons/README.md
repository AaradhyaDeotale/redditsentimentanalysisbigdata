# Multilingual sentiment lexicons

Per-language word lists in the format expected by
`ml_model.labeling.multilingual_labeler` (`--lexicon-dir`):
one `<lang>.txt` per language, lines `word<TAB>pos` or `word<TAB>neg`.

**Source:** NRC Word-Emotion Association Lexicon (EmoLex) v0.92,
Saif M. Mohammad and Peter D. Turney, downloaded from
<https://saifmohammad.com/WebPages/NRC-Emotion-Lexicon.htm>.
The per-language files use NRC's automatic translations of the English
lexicon; only the `positive` / `negative` associations are kept.

Conversion rules: translated word lowercased; multi-word translations
skipped (the labeler matches single tokens); words that ended up in both
the positive and negative set (translation collisions) dropped.

**License:** free for research and non-commercial use. Commercial use
requires a license from NRC — see the terms on the page above.

Usage:

```bash
python -m ml_model.labeling.label_corpus \
    --input pipeline-data/cleaned_comments.jsonl \
    --output pipeline-data/labeled_comments.jsonl \
    --lexicon-dir lexicons
```
