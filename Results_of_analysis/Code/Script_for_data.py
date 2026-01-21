import re
import pandas as pd
from nltk.stem.snowball import SnowballStemmer

# import nltk
# nltk.download("stopwords")
# from nltk.corpus import stopwords
# STOP = set(stopwords.words("russian"))


STOP = set()  # <- список стоп-слов

TOKEN_RE = re.compile(r"[а-яё]+", re.IGNORECASE)
stemmer = SnowballStemmer("russian")

def rating_group(r: int) -> str:
    if r <= 2: return "low"
    if r == 3: return "mid"
    return "high"

def month_to_season(m: int) -> str:
    if m in (12, 1, 2): return "winter"
    if m in (3, 4, 5): return "spring"
    if m in (6, 7, 8): return "summer"
    return "autumn"

def tokenize(text: str) -> list[str]:
    t = (text or "").lower().replace("ё", "е")
    return TOKEN_RE.findall(t)

RAW_PATH = "all_reviews.jsonl"

raw = pd.read_json(RAW_PATH, lines=True)

# --- reviews base ---
df = raw.rename(columns={
    "review_key": "review_id",
    "restaurant_name": "org_name",
    "date": "review_date",
    "text": "review_text_raw",
})

df["org_id"] = pd.to_numeric(df["org_id"], errors="coerce").astype("Int64")
df["rating"] = pd.to_numeric(df["rating"], errors="coerce").fillna(0).astype(int)
df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce").dt.date.astype(str)

df["month"] = pd.to_datetime(df["review_date"]).dt.month.astype(int)
df["season"] = df["month"].map(month_to_season)
df["rating_group"] = df["rating"].map(rating_group)

df["city"] = "Казань"
df["org_type"] = "Кафе и рестораны"

# --- tokenize -> review_text_clean + review_tokens rows ---
token_rows = []
clean_texts = []
lengths = []

for review_id, org_id, text, rating, rg, rdate, month, season in zip(
    df["review_id"], df["org_id"], df["review_text_raw"],
    df["rating"], df["rating_group"], df["review_date"], df["month"], df["season"]
):
    all_tokens = tokenize(text)              # позиции считаем тут
    clean_tokens = []

    for pos, tok in enumerate(all_tokens):
        tok = tok.replace("ё", "е")
        is_sw = (tok in STOP) or (len(tok) < 2)
        is_sw_new = is_sw  # 2-й список стоп-слов — примените его тут

        if not is_sw_new:
            clean_tokens.append(tok)
            token_rows.append({
                "review_id": review_id,
                "org_id": int(org_id) if pd.notna(org_id) else None,
                "lemma": stemmer.stem(tok),
                "token": tok,
                "token_position": pos,
                "is_stopword": int(is_sw),
                "rating": int(rating),
                "rating_group": rg,
                "review_date": rdate,
                "is_stopword_new": int(is_sw_new),
                "month": int(month),
                "season": season,
            })

    clean_texts.append(" ".join(clean_tokens))
    lengths.append(len(clean_tokens))

df["review_text_clean"] = clean_texts
df["review_length"] = lengths

reviews = df[[
    "review_id","org_id","org_name","city","org_type",
    "author_id","author_level","review_date","rating","rating_group",
    "review_text_raw","review_text_clean","review_length",
    "source_url","scraped_at_unix","month","season"
]].copy()

review_tokens = pd.DataFrame(token_rows)

# --- org_dim ---
g = reviews.groupby(["org_id","org_name"], dropna=False)

org_dim = g.apply(lambda x: pd.Series({
    "restaurant_name": x["org_name"].iloc[0],
    "n_reviews": len(x),
    "n_authors": x["author_id"].nunique(dropna=True),
    "avg_rating": x["rating"].mean(),
    "med_text_len": x["review_length"].median(),
    "pct_1_2": 100.0 * x["rating"].isin([1,2]).mean(),
    "pct_5": 100.0 * (x["rating"] == 5).mean(),
    "pct_blank_author": 100.0 * x["author_id"].isna().mean(),
    "pct_short20": 100.0 * (x["review_length"] <= 20).mean(),
    "pct_dup_norm_text": 100.0 * x["review_text_clean"].duplicated(keep="first").mean(),
    "year_min": pd.to_datetime(x["review_date"]).dt.year.min(),
    "year_max": pd.to_datetime(x["review_date"]).dt.year.max(),
    "city": x["city"].iloc[0],
    "org_type": x["org_type"].iloc[0],
})).reset_index().drop(columns=["org_name"])

# --- lemma_stats (только low/high) ---
lh = review_tokens[review_tokens["rating_group"].isin(["low","high"])]

pivot = (lh.groupby(["lemma","rating_group"])
           .size().unstack(fill_value=0)
           .rename(columns={"low":"count_low","high":"count_high"})
           .reset_index())

pivot["total_count"] = pivot["count_low"] + pivot["count_high"]
pivot["share_low"] = pivot["count_low"] / pivot["total_count"]
pivot["share_high"] = pivot["count_high"] / pivot["total_count"]

lemma_stats = pivot[["lemma","total_count","count_low","count_high","share_low","share_high"]]

# --- season_lemma_stats / total ---
season_lemma_stats = (review_tokens
    .groupby(["season","lemma","rating_group"])
    .size().reset_index(name="cnt")
    .rename(columns={"lemma":"lemma_final"})
)

season_lemma_stats_total = (review_tokens
    .groupby(["season","lemma"])
    .size().reset_index(name="cnt_total")
    .rename(columns={"lemma":"lemma_final"})
)

# --- export (как у вас: ; и utf-8-sig) ---
reviews.to_csv("reviews.csv", sep=";", index=False, encoding="utf-8-sig")
org_dim.to_csv("org_dim.csv", sep=";", index=False, encoding="utf-8-sig")
review_tokens.to_csv("review_tokens.csv", sep=";", index=False, encoding="utf-8-sig")
lemma_stats.to_csv("lemma_stats.csv", sep=";", index=False, encoding="utf-8-sig")
season_lemma_stats.to_csv("season_lemma_stats.csv", sep=";", index=False, encoding="utf-8-sig")
season_lemma_stats_total.to_csv("season_lemma_stats_total.csv", sep=";", index=False, encoding="utf-8-sig")
