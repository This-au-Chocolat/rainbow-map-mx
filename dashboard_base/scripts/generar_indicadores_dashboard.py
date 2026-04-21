from __future__ import annotations

from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[2]
OUTPUTS = ROOT / "outputs"
PROCESSED = ROOT / "dashboard_base" / "data" / "processed"

NEGATIVE_CRITERIA = {"S3", "M1", "M2"}
POSITIVE_CRITERIA = {"S1", "S2", "M3"}
ALL_CRITERIA = ["S1", "S2", "S3", "M1", "M2", "M3"]


def build_standardized_long() -> pl.DataFrame:
    long_df = pl.read_parquet(OUTPUTS / "state_scores_long.parquet")

    if long_df.is_empty():
        raise ValueError("outputs/state_scores_long.parquet no contiene filas")

    rows: list[dict[str, object]] = []
    for criterion in ALL_CRITERIA:
        sub = long_df.filter(pl.col("criterion_id") == criterion)
        if sub.is_empty():
            raise ValueError(f"Falta criterio en long table: {criterion}")

        n_ref = int(round(float(sub["unweighted_n"].median())))

        # Promedio nacional ponderado para estabilizar estados con n pequeno.
        p_nat = float(
            sub.select(
                (
                    (pl.col("estimate_pct") * pl.col("weighted_population_if_available")).sum()
                    / pl.col("weighted_population_if_available").sum()
                ).alias("p_nat")
            ).item(0, 0)
        )

        for rec in sub.to_dicts():
            n = int(rec["unweighted_n"])
            p_raw = float(rec["estimate_pct"])

            # Estandarizacion a n comun por criterio via mezcla con promedio nacional.
            shrink = min(1.0, n / max(n_ref, 1))
            p_std = (shrink * p_raw) + ((1.0 - shrink) * p_nat)

            if criterion in NEGATIVE_CRITERIA:
                score_std = 100.0 - p_std
            else:
                score_std = p_std

            rows.append(
                {
                    "state_code": rec["state_code"],
                    "state_name": rec["state_name"],
                    "criterion_id": criterion,
                    "estimate_pct_std": p_std,
                    "score_100_std": score_std,
                    "unweighted_n": n,
                    "n_ref": n_ref,
                    "low_reliability_flag": bool(rec["low_reliability_flag"]),
                    "reliability_reason": rec.get("reliability_reason", ""),
                    "survey_used": rec["survey_used"],
                }
            )

    return pl.DataFrame(rows)


def build_dashboard_table(std_long: pl.DataFrame) -> pl.DataFrame:
    catalog = (
        pl.read_csv(PROCESSED / "catalogo_entidades_estandar.csv")
        .with_columns(pl.col("cve_ent").cast(pl.Utf8).str.zfill(2))
        .select(["cve_ent", "nom_ent", "nombre_normalizado"])
    )

    pivot_est = std_long.select(
        ["state_code", "criterion_id", "estimate_pct_std", "low_reliability_flag"]
    ).pivot(
        values=["estimate_pct_std", "low_reliability_flag"],
        index=["state_code"],
        on="criterion_id",
        aggregate_function="first",
    )

    out = (
        pivot_est.rename({"state_code": "cve_ent"})
        .join(catalog, on="cve_ent", how="left")
        .with_columns(
            [
                pl.col("estimate_pct_std_S1").alias("S1_aceptacion_convivencia_pareja"),
                pl.col("estimate_pct_std_S2").alias("S2_aceptacion_liderazgo_lgbt_trans"),
                pl.col("estimate_pct_std_S3").alias("S3_discriminacion_reportada"),
                pl.col("estimate_pct_std_M1").alias("M1_sintomas_depresivos_ansiosos"),
                pl.col("estimate_pct_std_M2").alias("M2_ideacion_intento_suicida"),
                pl.col("estimate_pct_std_M3").alias("M3_acceso_salud_mental"),
            ]
        )
        .with_columns(
            [
                # Pilar 3 combina 2 positivos y 1 negativo invertido.
                pl.mean_horizontal(
                    [
                        pl.col("S1_aceptacion_convivencia_pareja"),
                        pl.col("S2_aceptacion_liderazgo_lgbt_trans"),
                        100.0 - pl.col("S3_discriminacion_reportada"),
                    ]
                ).alias("pilar_3_score"),
                # Pilar 4 combina 2 negativos invertidos y 1 positivo.
                pl.mean_horizontal(
                    [
                        100.0 - pl.col("M1_sintomas_depresivos_ansiosos"),
                        100.0 - pl.col("M2_ideacion_intento_suicida"),
                        pl.col("M3_acceso_salud_mental"),
                    ]
                ).alias("pilar_4_score"),
                (
                    pl.col("low_reliability_flag_S1").cast(pl.Int8)
                    + pl.col("low_reliability_flag_S2").cast(pl.Int8)
                    + pl.col("low_reliability_flag_S3").cast(pl.Int8)
                    + pl.col("low_reliability_flag_M1").cast(pl.Int8)
                    + pl.col("low_reliability_flag_M2").cast(pl.Int8)
                    + pl.col("low_reliability_flag_M3").cast(pl.Int8)
                ).alias("low_reliability_count")
            ]
        )
        .with_columns(pl.mean_horizontal(["pilar_3_score", "pilar_4_score"]).alias("rainbow_score"))
    )

    q1 = out.select(pl.col("rainbow_score").quantile(0.333, interpolation="nearest")).item()
    q2 = out.select(pl.col("rainbow_score").quantile(0.666, interpolation="nearest")).item()

    out = out.with_columns(
        [
            pl.when(pl.col("rainbow_score") <= q1)
            .then(pl.lit("rezago"))
            .when(pl.col("rainbow_score") <= q2)
            .then(pl.lit("avance"))
            .otherwise(pl.lit("lider"))
            .alias("category_status"),
            pl.when(pl.col("low_reliability_count") > 0)
            .then(
                pl.concat_str(
                    [
                        pl.lit("Estandarizado por n comun. Incluye "),
                        pl.col("low_reliability_count").cast(pl.Utf8),
                        pl.lit(" indicador(es) con bandera de baja confiabilidad."),
                    ]
                )
            )
            .otherwise(pl.lit("Estandarizado por n comun. Sin banderas de baja confiabilidad."))
            .alias("notes"),
        ]
    )

    return out.select(
        [
            "cve_ent",
            "nom_ent",
            "nombre_normalizado",
            "S1_aceptacion_convivencia_pareja",
            "S2_aceptacion_liderazgo_lgbt_trans",
            "S3_discriminacion_reportada",
            "M1_sintomas_depresivos_ansiosos",
            "M2_ideacion_intento_suicida",
            "M3_acceso_salud_mental",
            "pilar_3_score",
            "pilar_4_score",
            "rainbow_score",
            "category_status",
            "notes",
            "low_reliability_count",
        ]
    ).sort("cve_ent")


def main() -> None:
    std_long = build_standardized_long()
    dashboard = build_dashboard_table(std_long)
    out_path = PROCESSED / "indicadores_dashboard_estatal.csv"
    dashboard.write_csv(out_path, float_precision=4)
    print(f"Archivo generado: {out_path}")
    print(dashboard.head(5))


if __name__ == "__main__":
    main()
