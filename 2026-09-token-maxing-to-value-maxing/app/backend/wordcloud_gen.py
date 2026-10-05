"""Word-cloud generation — ported from the FinServ AI Gateway notebook
(`04_use_case_classifier_wordcloud`).

The notebook renders a `wordcloud.WordCloud` over the `user_prompt` text of the
governed requests. Here we run the same logic in-process over the app's prompt
corpus (`ds_usecase_detail.prompt_preview`), filtered to the caller's selection,
and return a transparent PNG (base64) styled to the app's dark palette.
"""
import base64
import io

from wordcloud import WordCloud, STOPWORDS
from matplotlib.colors import LinearSegmentedColormap

# Notebook stopword list, unioned with wordcloud's defaults + a few generic
# prompt verbs, so the cloud surfaces domain terms rather than filler.
_NOTEBOOK_STOP = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "it", "this", "that", "are", "was",
    "be", "have", "has", "had", "not", "no", "can", "will", "should",
    "would", "could", "our", "we", "i", "you", "they", "their", "its",
    "how", "what", "which", "who", "when", "where", "do", "does",
    "write", "create", "implement", "using", "use", "following",
    "need", "want", "help", "please", "given", "make", "get", "also",
}
STOPWORDS_SET = set(STOPWORDS) | _NOTEBOOK_STOP

# On-brand colormap (app categorical palette: teal → blue → violet → gold → pink).
_CMAP = LinearSegmentedColormap.from_list(
    "gatewayiq", ["#35D6BE", "#46C7FF", "#4C8DFF", "#A98CFF", "#FF7AA8", "#FFC15A"])


def generate_png_b64(text, max_words=110):
    """Render `text` to a transparent-background word-cloud PNG (base64)."""
    wc = WordCloud(
        width=1280, height=640,
        background_color=None, mode="RGBA",
        max_words=max_words,
        colormap=_CMAP,
        stopwords=STOPWORDS_SET,
        min_font_size=11, max_font_size=110,
        prefer_horizontal=0.72,
        collocations=True,
        relative_scaling=0.5,
    ).generate(text)
    buf = io.BytesIO()
    wc.to_image().save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode("ascii")
