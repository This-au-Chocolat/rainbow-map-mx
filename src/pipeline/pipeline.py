from __future__ import annotations

import csv
import math
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl

ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = ROOT / "source_inegi"
OUTPUT_DIR = ROOT / "outputs"
INTERMEDIATE_DIR = ROOT / "data" / "intermediate"


@dataclass(frozen=True)
class IndicatorSpec:
    criterion_id: str
    pillar: str
    criterion_name: str
    preferred_survey: str
    priority_keywords: tuple[str, ...]
    selection_mode: str = "single"


INDICATOR_SPECS: list[IndicatorSpec] = [
    IndicatorSpec(
        criterion_id="S1",
        pillar="Pillar 3",
        criterion_name="Acceptance of same-sex cohabiting couple",
        preferred_survey="ENADIS_2022",
        priority_keywords=(
            "dos personas del mismo sexo",
            "parejas del mismo sexo",
            "vivan juntas",
            "situaciones de igualdad",
            "cohab",
        ),
    ),
    IndicatorSpec(
        criterion_id="S2",
        pillar="Pillar 3",
        criterion_name="Acceptance of LGBT / trans political leadership",
        preferred_survey="ENADIS_2022",
        priority_keywords=(
            "inclusion en decisiones de gobierno",
            "personas gays",
            "personas trans",
            "liderazgo",
            "politica",
        ),
        selection_mode="multi",
    ),
    IndicatorSpec(
        criterion_id="S3",
        pillar="Pillar 3",
        criterion_name="Reported recent discrimination (LGBT+ population)",
        preferred_survey="ENDISEG_2021",
        priority_keywords=(
            "ultimos 12 meses",
            "discriminacion",
            "menosprecio",
            "orientacion",
            "genero",
        ),
        selection_mode="multi",
    ),
    IndicatorSpec(
        criterion_id="M1",
        pillar="Pillar 4",
        criterion_name="Depressive / anxiety / stress symptoms in last 12 months",
        preferred_survey="ENDISEG_2021",
        priority_keywords=("estres", "depresion", "ansiedad", "angustia", "ultimos 12 meses"),
        selection_mode="multi",
    ),
    IndicatorSpec(
        criterion_id="M2",
        pillar="Pillar 4",
        criterion_name="Suicidal ideation or suicide attempt",
        preferred_survey="ENDISEG_2021",
        priority_keywords=("suicid", "intento", "pensado", "ultimos 12 meses"),
        selection_mode="multi",
    ),
    IndicatorSpec(
        criterion_id="M3",
        pillar="Pillar 4",
        criterion_name="Access to mental health care",
        preferred_survey="ENDISEG_2021",
        priority_keywords=("atencion psicologica", "atencion psiquiatrica", "ningun tipo de atencion"),
        selection_mode="multi",
    ),
]


def normalize_text(text: str) -> str:
    raw = unicodedata.normalize("NFKD", str(text)).encode("ascii", "ignore").decode("ascii")
    raw = re.sub(r"\s+", " ", raw).strip().lower()
    return raw


def infer_survey(path: Path) -> str:
    p = str(path).lower()
    if "enadis2022" in p:
        return "ENADIS_2022"
    if "endiseg_2021" in p:
        return "ENDISEG_2021"
    return "OTHER"


def infer_role(path: Path) -> str:
    p = str(path).lower()
    if "conjunto_de_datos" in p:
        return "microdata"
    if "diccionario" in p:
        return "dictionary"
    if "catalog" in p or "catalogo" in p:
        return "catalog"
    if "metadato" in p:
        return "metadata"
    if "modelo" in p:
        return "er_model"
    return "other"


def build_file_manifest(source_dir: Path = SOURCE_DIR) -> pl.DataFrame:
    records: list[dict[str, Any]] = []
    for path in sorted(source_dir.rglob("*")):
        if not path.is_file():
            continue
        records.append(
            {
                "path": str(path.relative_to(ROOT)),
                "file_name": path.name,
                "suffix": path.suffix.lower(),
                "survey": infer_survey(path),
                "role": infer_role(path),
            }
        )
    manifest = pl.DataFrame(records)
    return manifest.sort(["survey", "role", "path"])


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
        try:
            with path.open("r", encoding=enc, errors="replace", newline="") as f:
                reader = csv.DictReader(f)
                return [{k: (v or "") for k, v in row.items()} for row in reader]
        except Exception:
            continue
    return []


def _find_column(cols: list[str], pattern: str) -> str | None:
    for c in cols:
        if re.search(pattern, normalize_text(c)):
            return c
    return None


def build_dictionary_index(source_dir: Path = SOURCE_DIR) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    dictionary_files = list(source_dir.rglob("diccionario*.csv")) + list(
        source_dir.rglob("*diccionario*csv")
    )
    for dpath in sorted(set(dictionary_files)):
        if not dpath.is_file():
            continue
        items = _read_csv_rows(dpath)
        if not items:
            continue
        columns = list(items[0].keys())
        var_col = _find_column(columns, r"nemonic|nemonico|nemonico")
        label_col = _find_column(columns, r"nombre_campo|nombre campo")
        catalog_col = _find_column(columns, r"catalog")
        if var_col is None or label_col is None:
            continue

        for row in items:
            var_name = str(row.get(var_col, "")).strip()
            label = str(row.get(label_col, "")).strip()
            if not var_name or not label:
                continue
            rows.append(
                {
                    "survey": infer_survey(dpath),
                    "table_name": dpath.parent.parent.parent.name,
                    "dictionary_file": str(dpath.relative_to(ROOT)),
                    "variable_name": var_name,
                    "variable_label": label,
                    "catalog_name": str(row.get(catalog_col, "")).strip() if catalog_col else "",
                    "normalized_label": normalize_text(label),
                    "normalized_variable": normalize_text(var_name),
                }
            )

    df = pl.DataFrame(rows).unique(subset=["survey", "table_name", "variable_name", "variable_label"])
    return df


def discover_variable_candidates(dictionary_df: pl.DataFrame) -> pl.DataFrame:
    records: list[dict[str, Any]] = []

    for spec in INDICATOR_SPECS:
        for row in dictionary_df.to_dicts():
            label = row["normalized_label"]
            var = row["normalized_variable"]
            score = 0.0
            hits: list[str] = []
            for keyword in spec.priority_keywords:
                k = normalize_text(keyword)
                if k in label:
                    score += 2.0
                    hits.append(k)
                elif k in var:
                    score += 1.0
                    hits.append(k)

            # Additional signal for preferred survey and known variable prefixes.
            if row["survey"] == spec.preferred_survey:
                score += 0.5
            if spec.criterion_id in {"M1", "M2", "M3", "S3"} and row["survey"] == "ENDISEG_2021":
                score += 0.5
            if spec.criterion_id in {"S1", "S2"} and row["survey"] == "ENADIS_2022":
                score += 0.5

            if score <= 0:
                continue

            records.append(
                {
                    "criterion_id": spec.criterion_id,
                    "pillar": spec.pillar,
                    "criterion_name": spec.criterion_name,
                    "preferred_survey": spec.preferred_survey,
                    "survey": row["survey"],
                    "table_name": row["table_name"],
                    "variable_name": row["variable_name"],
                    "variable_label": row["variable_label"],
                    "catalog_name": row["catalog_name"],
                    "match_score": score,
                    "match_terms": "|".join(sorted(set(hits))),
                    "selection_confidence": min(1.0, score / 8.0),
                }
            )

    if not records:
        return pl.DataFrame()

    candidates = (
        pl.DataFrame(records)
        .sort(["criterion_id", "match_score", "selection_confidence"], descending=[False, True, True])
        .unique(subset=["criterion_id", "survey", "table_name", "variable_name"], keep="first")
    )
    return candidates


def select_indicator_crosswalk(candidates: pl.DataFrame) -> pl.DataFrame:
    # Deterministic assistive rules after metadata ranking.
    required_vars = {
        "S1": ["PO1_4_6", "PO3_2_2", "PO1_2_7"],
        "S2": ["PO2_3_8", "PO2_3_9", "PO1_3_7", "PO1_3_8"],
        "S3": ["P8_5", "P9_9", "P11_6_11"],
        "M1": ["P10_1_2", "P10_1_3", "P10_1_5"],
        "M2": ["P10_2", "P10_3"],
        "M3": ["P10_5_2", "P10_5_3", "P10_5_7", "P10_6_3"],
    }

    rows: list[dict[str, Any]] = []
    cand = candidates.with_columns(pl.col("variable_name").str.to_uppercase())

    for spec in INDICATOR_SPECS:
        subset = cand.filter(pl.col("criterion_id") == spec.criterion_id)
        preferred_list = required_vars[spec.criterion_id]
        chosen = subset.filter(pl.col("variable_name").is_in(preferred_list)).sort(
            "match_score", descending=True
        )

        if chosen.height == 0:
            chosen = subset.head(2 if spec.selection_mode == "multi" else 1)

        if chosen.height == 0:
            rows.append(
                {
                    "criterion_id": spec.criterion_id,
                    "pillar": spec.pillar,
                    "criterion_name": spec.criterion_name,
                    "preferred_survey": spec.preferred_survey,
                    "survey_used": "UNAVAILABLE",
                    "fallback_used": True,
                    "survey_file": "",
                    "variable_name": "",
                    "variable_label": "",
                    "response_codes_used": "",
                    "derived_variable_name": f"{spec.criterion_id.lower()}_event",
                    "target_population": "LGBT+ for ENDISEG indicators; adult population for ENADIS perception indicators",
                    "universe_definition": "Indicator-specific universe",
                    "numerator_definition": "Indicator-specific event numerator",
                    "denominator_definition": "Indicator-specific valid universe denominator",
                    "raw_metric_name": "estimate_pct",
                    "score_direction": "higher_is_better",
                    "score_formula": "score_100 = estimate_pct for positive indicators; 100 - estimate_pct for negative indicators",
                    "threshold_rule": "tertiles over state distribution (data-informed)",
                    "reliability_rule": "low if n<30 or n_eff<60 or cv>0.30 or CI width>25pp",
                    "justification": "No variable found from metadata search",
                    "selection_confidence": 0.0,
                    "notes": "Unavailable",
                }
            )
            continue

        used_survey = chosen["survey"][0]
        fallback = used_survey != spec.preferred_survey
        rows.append(
            {
                "criterion_id": spec.criterion_id,
                "pillar": spec.pillar,
                "criterion_name": spec.criterion_name,
                "preferred_survey": spec.preferred_survey,
                "survey_used": used_survey,
                "fallback_used": fallback,
                "survey_file": chosen["table_name"][0],
                "variable_name": "|".join(chosen["variable_name"].to_list()),
                "variable_label": " | ".join(chosen["variable_label"].to_list()),
                "response_codes_used": "1 = positive/yes event where applicable; 2 = no; 9/98/99 as missing",
                "derived_variable_name": f"{spec.criterion_id.lower()}_event",
                "target_population": "LGBT+ for ENDISEG indicators; adult population for ENADIS perception indicators",
                "universe_definition": "Indicator-specific universe",
                "numerator_definition": "Weighted count of indicator event",
                "denominator_definition": "Weighted count of non-missing universe",
                "raw_metric_name": "estimate_pct",
                "score_direction": "higher_is_better",
                "score_formula": "score_100 = estimate_pct for positive indicators; 100 - estimate_pct for negative indicators",
                "threshold_rule": "tertiles over state distribution (data-informed)",
                "reliability_rule": "low if n<30 or n_eff<60 or cv>0.30 or CI width>25pp",
                "justification": "Selected from automatic metadata ranking with variable-name priority checks",
                "selection_confidence": float(chosen["selection_confidence"].mean()),
                "notes": "Fallback used" if fallback else "Primary source used",
            }
        )

    return pl.DataFrame(rows)


def _find_microdata_path(table_name: str) -> Path:
    candidates = sorted(SOURCE_DIR.glob(f"{table_name}/conjunto_de_datos/*.csv"))
    if candidates:
        return candidates[0]
    # fallback by exact file name pattern
    candidates = sorted(SOURCE_DIR.rglob(f"conjunto_de_datos_{table_name}.csv"))
    if candidates:
        return candidates[0]
    raise FileNotFoundError(f"No microdata CSV found for table: {table_name}")


def _read_microdata(table_name: str) -> pl.DataFrame:
    path = _find_microdata_path(table_name)
    # INEGI CSVs may contain occasional ragged rows; keep header width stable.
    df = pl.read_csv(
        path,
        infer_schema_length=5000,
        ignore_errors=True,
        truncate_ragged_lines=True,
        encoding="latin1",
        null_values=["", " "],
    )
    rename_map = {c: c.lower() for c in df.columns}
    return df.rename(rename_map)


def _read_csv_subset_with_dict_reader(path: Path, required_cols: list[str]) -> pl.DataFrame:
    required_upper = [c.upper() for c in required_cols]
    rows: list[dict[str, Any]] = []

    with path.open("r", encoding="latin-1", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"No header found in {path}")

        header_map = {h.upper(): h for h in reader.fieldnames}
        missing = [c for c in required_upper if c not in header_map]
        if missing:
            raise KeyError(f"Missing required columns in {path.name}: {missing}")

        for row in reader:
            cleaned: dict[str, Any] = {}
            for c in required_upper:
                value = str(row.get(header_map[c], "") or "")
                cleaned[c.lower()] = value.replace("\r", "").replace("\n", "").strip().strip('"')
            rows.append(cleaned)

    return pl.DataFrame(rows)


def _first_existing(df: pl.DataFrame, options: list[str]) -> str:
    for o in options:
        if o in df.columns:
            return o
    raise KeyError(f"None of expected columns found: {options}")


def _num(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .str.replace_all(r"[\r\n\"]", "")
        .str.strip_chars()
        .cast(pl.Float64, strict=False)
    )


def _weighted_state_estimate(
    df: pl.DataFrame,
    state_col: str,
    weight_col: str,
    event_expr: pl.Expr,
    universe_expr: pl.Expr,
    criterion_id: str,
    criterion_name: str,
    pillar: str,
    survey_used: str,
    fallback_used: bool,
    positive_indicator: bool,
) -> pl.DataFrame:
    temp = df.select(
        [
            pl.col(state_col).cast(pl.Utf8).str.zfill(2).alias("state_code"),
            _num(weight_col).alias("w"),
            event_expr.cast(pl.Float64).alias("event"),
            universe_expr.cast(pl.Boolean).alias("in_u"),
        ]
    ).filter(pl.col("in_u") & pl.col("w").is_not_null() & (pl.col("w") > 0) & pl.col("state_code").is_not_null())

    grouped = temp.group_by("state_code").agg(
        [
            pl.len().alias("unweighted_n"),
            pl.sum("w").alias("sum_w"),
            (pl.col("w") * pl.col("event")).sum().alias("sum_wy"),
            (pl.col("w") ** 2).sum().alias("sum_w2"),
        ]
    )

    out_rows: list[dict[str, Any]] = []
    for r in grouped.to_dicts():
        p = float(r["sum_wy"]) / float(r["sum_w"]) if r["sum_w"] else float("nan")
        n_eff = (float(r["sum_w"]) ** 2 / float(r["sum_w2"])) if r["sum_w2"] else float("nan")
        se = math.sqrt(max(p * (1 - p), 0.0) / n_eff) if n_eff and n_eff > 0 else float("nan")
        ci_low = max(0.0, p - 1.96 * se) if not math.isnan(se) else float("nan")
        ci_high = min(1.0, p + 1.96 * se) if not math.isnan(se) else float("nan")
        cv = (se / p) if p > 0 and not math.isnan(se) else float("nan")
        low_reliability = (
            (r["unweighted_n"] < 30)
            or (not math.isnan(n_eff) and n_eff < 60)
            or (not math.isnan(cv) and cv > 0.30)
            or (not math.isnan(ci_high - ci_low) and (ci_high - ci_low) > 0.25)
        )
        reasons: list[str] = []
        if r["unweighted_n"] < 30:
            reasons.append("n<30")
        if not math.isnan(n_eff) and n_eff < 60:
            reasons.append("n_eff<60")
        if not math.isnan(cv) and cv > 0.30:
            reasons.append("cv>0.30")
        if not math.isnan(ci_high - ci_low) and (ci_high - ci_low) > 0.25:
            reasons.append("ci_width>25pp")

        estimate_pct = p * 100.0
        score_100 = estimate_pct if positive_indicator else 100.0 - estimate_pct

        out_rows.append(
            {
                "state_code": r["state_code"],
                "pillar": pillar,
                "criterion_id": criterion_id,
                "criterion_name": criterion_name,
                "survey_used": survey_used,
                "fallback_used": fallback_used,
                "estimate_pct": estimate_pct,
                "score_100": score_100,
                "low_reliability_flag": low_reliability,
                "reliability_reason": "|".join(reasons) if reasons else "",
                "unweighted_n": int(r["unweighted_n"]),
                "weighted_population_if_available": float(r["sum_w"]),
                "se_if_available": se * 100.0 if not math.isnan(se) else None,
                "ci_low_if_available": ci_low * 100.0 if not math.isnan(ci_low) else None,
                "ci_high_if_available": ci_high * 100.0 if not math.isnan(ci_high) else None,
            }
        )

    return pl.DataFrame(out_rows)


def _compute_endiseg_indicators(crosswalk: pl.DataFrame) -> pl.DataFrame:
    df = _read_microdata("conjunto_de_datos_tmodulo_endiseg_2021")

    state_col = _first_existing(df, ["ent"])
    weight_col = _first_existing(df, ["factor"])

    # LGBT+ analytical population definition.
    is_lgbt = (
        (_num("p8_1").is_in([2, 3, 4, 5, 6]))
        | (_num("p8_1a").is_in([2, 3, 4]))
        | (_num("p9_1").is_in([2, 3, 4, 5]))
        | (_num("filtro_10_5") == 1)
    )

    s3_event = (_num("p8_5") == 1) | (_num("p9_9") == 1) | (_num("p11_6_11") == 1)
    m1_event = (_num("p10_1_2") == 1) | (_num("p10_1_3") == 1) | (_num("p10_1_5") == 1)
    m2_event = (_num("p10_2") == 1) | (_num("p10_3") == 1)
    mh_need = m1_event | m2_event
    m3_access_event = (_num("p10_5_2") == 1) | (_num("p10_5_3") == 1) | (_num("p10_6_3") == 1)

    specs = [
        ("S3", "Reported recent discrimination (LGBT+ population)", "Pillar 3", s3_event, is_lgbt, False),
        (
            "M1",
            "Depressive / anxiety / stress symptoms in last 12 months",
            "Pillar 4",
            m1_event,
            is_lgbt,
            False,
        ),
        ("M2", "Suicidal ideation or suicide attempt", "Pillar 4", m2_event, is_lgbt, False),
        (
            "M3",
            "Access to mental health care",
            "Pillar 4",
            m3_access_event,
            is_lgbt & mh_need,
            True,
        ),
    ]

    frames = []
    for criterion_id, name, pillar, event_expr, universe_expr, positive in specs:
        fallback_used = bool(
            crosswalk.filter(pl.col("criterion_id") == criterion_id).select("fallback_used").item(0, 0)
        )
        frames.append(
            _weighted_state_estimate(
                df,
                state_col,
                weight_col,
                event_expr,
                universe_expr,
                criterion_id,
                name,
                pillar,
                "ENDISEG_2021",
                fallback_used,
                positive,
            )
        )

    return pl.concat(frames, how="vertical")


def _compute_enadis_indicators(crosswalk: pl.DataFrame) -> pl.DataFrame:
    tcoe_path = _find_microdata_path("tcoe_enadis2022")
    df = _read_csv_subset_with_dict_reader(
        tcoe_path,
        ["ENT", "FAC_P18", "PO1_4_6", "PO3_2_2", "PO2_3_8", "PO2_3_9", "PO1_3_7", "PO1_3_8"],
    )
    state_col = "ent"
    weight_col = "fac_p18"

    s1_event = (_num("po1_4_6") == 1) | (_num("po3_2_2") == 1)
    s2_event = (
        (_num("po2_3_8") == 1)
        | (_num("po2_3_9") == 1)
        | (_num("po1_3_7") == 1)
        | (_num("po1_3_8") == 1)
    )
    universe = pl.lit(True)

    frames = []
    for criterion_id, name, event_expr in [
        ("S1", "Acceptance of same-sex cohabiting couple", s1_event),
        ("S2", "Acceptance of LGBT / trans political leadership", s2_event),
    ]:
        fallback_used = bool(
            crosswalk.filter(pl.col("criterion_id") == criterion_id).select("fallback_used").item(0, 0)
        )
        frames.append(
            _weighted_state_estimate(
                df,
                state_col,
                weight_col,
                event_expr,
                universe,
                criterion_id,
                name,
                "Pillar 3",
                "ENADIS_2022",
                fallback_used,
                True,
            )
        )

    return pl.concat(frames, how="vertical")


def _add_state_names(df: pl.DataFrame) -> pl.DataFrame:
    catalog_path = ROOT / "dashboard_base" / "data" / "processed" / "catalogo_entidades_estandar.csv"
    if not catalog_path.exists():
        return df.with_columns(pl.col("state_code").alias("state_name"))

    states = (
        pl.read_csv(catalog_path)
        .with_columns(
            pl.col("cvegeo").cast(pl.Utf8).str.zfill(2).alias("state_code"),
            pl.col("nom_ent").alias("state_name"),
        )
        .select(["state_code", "state_name"])
    )
    return df.join(states, on="state_code", how="left")


def _apply_thresholds(df: pl.DataFrame) -> pl.DataFrame:
    parts = []
    for criterion in sorted(df["criterion_id"].unique().to_list()):
        sub = df.filter(pl.col("criterion_id") == criterion)
        q1 = float(sub["score_100"].quantile(0.333, interpolation="nearest"))
        q2 = float(sub["score_100"].quantile(0.666, interpolation="nearest"))

        tagged = sub.with_columns(
            pl.when(pl.col("score_100") <= q1)
            .then(pl.lit("low"))
            .when(pl.col("score_100") <= q2)
            .then(pl.lit("medium"))
            .otherwise(pl.lit("high"))
            .alias("threshold_band"),
            pl.when(pl.col("score_100") <= q1)
            .then(pl.lit(0))
            .when(pl.col("score_100") <= q2)
            .then(pl.lit(1))
            .otherwise(pl.lit(2))
            .alias("optional_score_0_1_2"),
            pl.lit(f"Tertiles: <= {q1:.2f} low, <= {q2:.2f} medium, > {q2:.2f} high").alias(
                "notes"
            ),
        )
        parts.append(tagged)

    return pl.concat(parts, how="vertical")


def run_full_pipeline() -> dict[str, pl.DataFrame]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    INTERMEDIATE_DIR.mkdir(parents=True, exist_ok=True)

    manifest = build_file_manifest()
    dictionaries = build_dictionary_index()
    candidates = discover_variable_candidates(dictionaries)
    crosswalk = select_indicator_crosswalk(candidates)

    enadis_scores = _compute_enadis_indicators(crosswalk)
    endiseg_scores = _compute_endiseg_indicators(crosswalk)
    scores_long = pl.concat([enadis_scores, endiseg_scores], how="vertical")
    scores_long = _add_state_names(scores_long)
    scores_long = _apply_thresholds(scores_long)

    metadata = crosswalk.select(
        [
            "criterion_id",
            "pillar",
            "criterion_name",
            "preferred_survey",
            "survey_used",
            "fallback_used",
            "survey_file",
            "variable_name",
            "variable_label",
            "response_codes_used",
            "derived_variable_name",
            "target_population",
            "universe_definition",
            "numerator_definition",
            "denominator_definition",
            "raw_metric_name",
            "score_direction",
            "score_formula",
            "threshold_rule",
            "reliability_rule",
            "justification",
            "selection_confidence",
            "notes",
        ]
    )

    # Required long-table output schema.
    state_scores_long = scores_long.select(
        [
            "state_code",
            "state_name",
            "pillar",
            "criterion_id",
            "criterion_name",
            "survey_used",
            "fallback_used",
            "estimate_pct",
            "score_100",
            "threshold_band",
            "optional_score_0_1_2",
            "low_reliability_flag",
            "reliability_reason",
            "unweighted_n",
            "weighted_population_if_available",
            "se_if_available",
            "ci_low_if_available",
            "ci_high_if_available",
            "notes",
        ]
    )

    state_scores_wide = state_scores_long.select(
        [
            "state_code",
            "state_name",
            "criterion_id",
            "score_100",
            "estimate_pct",
            "low_reliability_flag",
        ]
    ).pivot(
        values=["score_100", "estimate_pct", "low_reliability_flag"],
        index=["state_code", "state_name"],
        columns="criterion_id",
        aggregate_function="first",
    )

    quality_flags = state_scores_long.select(
        [
            "state_code",
            "state_name",
            "criterion_id",
            "low_reliability_flag",
            "reliability_reason",
            "unweighted_n",
            "se_if_available",
            "ci_low_if_available",
            "ci_high_if_available",
        ]
    )

    manifest.write_parquet(INTERMEDIATE_DIR / "file_manifest.parquet")
    dictionaries.write_parquet(INTERMEDIATE_DIR / "dictionary_index.parquet")
    candidates.write_parquet(INTERMEDIATE_DIR / "variable_candidates.parquet")

    state_scores_long.write_parquet(OUTPUT_DIR / "state_scores_long.parquet")
    state_scores_wide.write_parquet(OUTPUT_DIR / "state_scores_wide.parquet")
    metadata.write_parquet(OUTPUT_DIR / "indicator_metadata.parquet")
    quality_flags.write_parquet(OUTPUT_DIR / "quality_flags.parquet")
    crosswalk.write_parquet(OUTPUT_DIR / "variable_crosswalk.parquet")

    return {
        "file_manifest": manifest,
        "dictionary_index": dictionaries,
        "variable_candidates": candidates,
        "variable_crosswalk": crosswalk,
        "state_scores_long": state_scores_long,
        "state_scores_wide": state_scores_wide,
        "indicator_metadata": metadata,
        "quality_flags": quality_flags,
    }
