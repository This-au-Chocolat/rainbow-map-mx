from __future__ import annotations

from pathlib import Path
import json
import random

import shapefile  # pyshp
import matplotlib.pyplot as plt


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "conjunto_de_datos"
OUT_SUMMARY = BASE_DIR / "resumen_mg2025.json"
OUT_PNG = BASE_DIR / "visualizacion_basica_mg2025.png"


def field_names(reader: shapefile.Reader) -> list[str]:
    # The first DBF field is a deletion marker, so we skip it.
    return [f[0] for f in reader.fields[1:]]


def value_counts(reader: shapefile.Reader, field: str) -> dict[str, int]:
    names = field_names(reader)
    if field not in names:
        return {}
    idx = names.index(field)
    counts: dict[str, int] = {}
    for rec in reader.iterRecords():
        value = rec[idx]
        key = str(value).strip() if value is not None else "<NULL>"
        if key == "":
            key = "<EMPTY>"
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda x: (-x[1], x[0])))


def summarize_layer(shp_path: Path) -> dict:
    r = shapefile.Reader(str(shp_path), encoding="latin1")
    names = field_names(r)
    summary = {
        "file": shp_path.name,
        "shape_type": r.shapeTypeName,
        "feature_count": len(r),
        "bbox": r.bbox,
        "fields": names,
    }

    # Common useful categorical summaries.
    for candidate in ["AMBITO", "PLANO", "CVE_ENT"]:
        if candidate in names:
            counts = value_counts(r, candidate)
            summary[f"counts_{candidate}"] = counts

    if "NOMGEO" in names and shp_path.name == "00ent.shp":
        idx = names.index("NOMGEO")
        entidades = sorted({str(rec[idx]).strip() for rec in r.iterRecords()})
        summary["entidades"] = entidades

    return summary


def draw_poly_shape(ax, shp, edgecolor="black", linewidth=0.2, alpha=1.0):
    points = shp.points
    if not points:
        return
    parts = list(shp.parts) + [len(points)]
    for i in range(len(parts) - 1):
        segment = points[parts[i] : parts[i + 1]]
        if len(segment) < 2:
            continue
        xs = [p[0] for p in segment]
        ys = [p[1] for p in segment]
        ax.plot(xs, ys, color=edgecolor, linewidth=linewidth, alpha=alpha)


def create_basic_plot() -> None:
    r_ent = shapefile.Reader(str(DATA_DIR / "00ent.shp"), encoding="latin1")
    r_lpr = shapefile.Reader(str(DATA_DIR / "00lpr.shp"), encoding="latin1")

    fig, ax = plt.subplots(figsize=(8, 8), dpi=180)

    # Draw state boundaries as base map.
    for shp in r_ent.iterShapes():
        draw_poly_shape(ax, shp, edgecolor="#1f2937", linewidth=0.6, alpha=0.9)

    # Plot a random sample of rural locality points to keep rendering light.
    all_points = [s.points[0] for s in r_lpr.iterShapes() if s.points]
    sample_size = min(50000, len(all_points))
    random.seed(42)
    sample = random.sample(all_points, sample_size) if sample_size else []
    if sample:
        xs = [p[0] for p in sample]
        ys = [p[1] for p in sample]
        ax.scatter(xs, ys, s=0.3, c="#d94633", alpha=0.35, linewidths=0)

    ax.set_title("Marco Geoestadistico Integrado 2025\nEstados + muestra de localidades rurales puntuales")
    ax.set_aspect("equal", adjustable="box")
    ax.set_xlabel("X (metros, CCL ITRF2008)")
    ax.set_ylabel("Y (metros, CCL ITRF2008)")
    ax.grid(True, linewidth=0.2, alpha=0.3)

    plt.tight_layout()
    fig.savefig(OUT_PNG)
    plt.close(fig)


def main() -> None:
    shp_files = sorted(DATA_DIR.glob("*.shp"))
    summaries = [summarize_layer(path) for path in shp_files]

    result = {
        "dataset": "Marco Geoestadistico Integrado 2025",
        "layers": summaries,
    }

    OUT_SUMMARY.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    create_basic_plot()

    print(f"Resumen guardado en: {OUT_SUMMARY}")
    print(f"Visualizacion guardada en: {OUT_PNG}")


if __name__ == "__main__":
    main()
