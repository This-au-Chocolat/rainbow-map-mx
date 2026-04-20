from __future__ import annotations

import csv
import json
import sys
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import shapefile  # pyshp
from pyproj import Transformer


@dataclass(frozen=True)
class Config:
    root: Path

    @property
    def shp_path(self) -> Path:
        return self.root / "source_inegi" / "conjunto_de_datos" / "00ent.shp"

    @property
    def out_geojson(self) -> Path:
        return self.root / "dashboard_base" / "data" / "processed" / "mexico_entidades_4326.geojson"

    @property
    def out_geojson_web(self) -> Path:
        return self.root / "dashboard_base" / "data" / "processed" / "mexico_entidades_4326_web.geojson"

    @property
    def out_indicadores_csv(self) -> Path:
        return self.root / "dashboard_base" / "data" / "processed" / "indicadores_demo_estatal.csv"

    @property
    def out_catalogo_csv(self) -> Path:
        return self.root / "dashboard_base" / "data" / "processed" / "catalogo_entidades_estandar.csv"


def strip_accents(text: str) -> str:
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def normalize_name(name: str) -> str:
    cleaned = strip_accents(name).lower().strip()
    cleaned = cleaned.replace(" de ignacio de la llave", "")
    cleaned = cleaned.replace(" de ocampo", "")
    cleaned = cleaned.replace(" de zaragoza", "")
    cleaned = cleaned.replace(" ", "_")
    return cleaned


def reproject_ring(ring: Iterable[tuple[float, float]], transformer: Transformer) -> list[list[float]]:
    xs, ys = zip(*ring)
    lon, lat = transformer.transform(xs, ys)
    return [[float(x), float(y)] for x, y in zip(lon, lat)]


def point_line_distance(point: list[float], start: list[float], end: list[float]) -> float:
    x, y = point
    x1, y1 = start
    x2, y2 = end
    if x1 == x2 and y1 == y2:
        return ((x - x1) ** 2 + (y - y1) ** 2) ** 0.5
    num = abs((y2 - y1) * x - (x2 - x1) * y + x2 * y1 - y2 * x1)
    den = ((y2 - y1) ** 2 + (x2 - x1) ** 2) ** 0.5
    return num / den


def simplify_rdp(points: list[list[float]], tolerance: float) -> list[list[float]]:
    if len(points) < 3:
        return points
    start = points[0]
    end = points[-1]
    max_dist = -1.0
    idx = -1
    for i in range(1, len(points) - 1):
        dist = point_line_distance(points[i], start, end)
        if dist > max_dist:
            max_dist = dist
            idx = i
    if max_dist > tolerance and idx != -1:
        left = simplify_rdp(points[: idx + 1], tolerance)
        right = simplify_rdp(points[idx:], tolerance)
        return left[:-1] + right
    return [start, end]


def simplify_ring(ring: list[list[float]], tolerance: float = 0.01, precision: int = 5) -> list[list[float]]:
    if len(ring) < 4:
        return ring
    is_closed = ring[0] == ring[-1]
    core = ring[:-1] if is_closed else ring
    simplified = simplify_rdp(core, tolerance)
    if len(simplified) < 3:
        simplified = core[:3]
    if is_closed:
        simplified = simplified + [simplified[0]]
    return [[round(x, precision), round(y, precision)] for x, y in simplified]


def shape_to_geojson_geometry(shape: shapefile.Shape, transformer: Transformer, simplify: bool = False) -> dict:
    points = shape.points
    parts = list(shape.parts) + [len(points)]
    rings = []
    for i in range(len(parts) - 1):
        start = parts[i]
        end = parts[i + 1]
        ring = points[start:end]
        if len(ring) < 4:
            continue
        reproj = reproject_ring(ring, transformer)
        if simplify:
            reproj = simplify_ring(reproj)
        rings.append(reproj)

    if not rings:
        return {"type": "Polygon", "coordinates": []}

    # Shapefile polygon parts can encode multiple polygons and holes.
    # For a robust prototype, we serialize as MultiPolygon using one ring per polygon part.
    multipolygon = [[ring] for ring in rings]
    return {"type": "MultiPolygon", "coordinates": multipolygon}


def demo_indicators(cve_ent: str) -> dict:
    # Deterministic pseudo-data for UI validation; replace with real indicators later.
    num = int(cve_ent)

    # Pilar 1: Existencia de la ley
    l1 = (num + 1) % 3
    l2 = (num * 2) % 3
    l3 = (num + 2) % 3

    # Pilar 2: Cumplimiento y agujeros legales
    c1 = (num + 2) % 3
    c2 = (num * 2 + 1) % 3
    c3 = (num + 1) % 3

    # Pilar 3: Percepcion social (escala 0-2 para prototipo)
    s1 = (num + 0) % 3
    s2 = (num + 1) % 3
    s3 = (num * 2 + 2) % 3

    # Pilar 4: Salud mental (M1 y M2 son prevalencia: mayor = peor)
    m1 = (num + 2) % 3
    m2 = (num * 2 + 1) % 3
    m3 = (num + 0) % 3

    p1_score = l1 + l2 + l3
    p2_score = c1 + c2 + c3
    p3_score = s1 + s2 + (2 - s3)  # S3 es adverso: mayor discriminacion, menor puntaje
    p4_score = (2 - m1) + (2 - m2) + m3  # M1/M2 adversos; M3 favorable
    rainbow_score = round(((p1_score + p2_score + p3_score + p4_score) / 24.0) * 100, 1)

    category_status = "lider" if rainbow_score >= 67 else ("avance" if rainbow_score >= 40 else "rezago")

    return {
        "L1_proteccion_antidiscriminacion": l1,
        "L2_reconocimiento_parejas": l2,
        "L3_identidad_documentos": l3,
        "C1_mecanismo_denuncia_sancion": c1,
        "C2_prohibicion_ecosig": c2,
        "C3_delitos_odio_registro_protocolo": c3,
        "S1_aceptacion_convivencia_pareja": s1,
        "S2_aceptacion_liderazgo_lgbt_trans": s2,
        "S3_discriminacion_reportada": s3,
        "M1_sintomas_depresivos_ansiosos": m1,
        "M2_ideacion_intento_suicida": m2,
        "M3_acceso_salud_mental": m3,
        "pilar_1_score": p1_score,
        "pilar_2_score": p2_score,
        "pilar_3_score": p3_score,
        "pilar_4_score": p4_score,
        "rainbow_score": rainbow_score,
        "category_status": category_status,
        "notes": "Dato de prueba para prototipo base alineado a pilares L-C-S-M.",
    }


def main() -> None:
    sys.setrecursionlimit(50000)
    root = Path(__file__).resolve().parents[2]
    cfg = Config(root)

    if not cfg.shp_path.exists():
        raise FileNotFoundError(f"No se encontro shapefile: {cfg.shp_path}")

    cfg.out_geojson.parent.mkdir(parents=True, exist_ok=True)

    reader = shapefile.Reader(str(cfg.shp_path), encoding="latin1")
    fields = [f[0] for f in reader.fields[1:]]

    required = {"CVE_ENT", "NOMGEO", "CVEGEO"}
    missing = required - set(fields)
    if missing:
        raise ValueError(f"Faltan campos esperados en 00ent.shp: {sorted(missing)}")

    idx_cve_ent = fields.index("CVE_ENT")
    idx_nomgeo = fields.index("NOMGEO")
    idx_cvegeo = fields.index("CVEGEO")

    # Source CRS from metadata/prj: Lambert Conformal Conic ITRF2008.
    src_proj4 = "+proj=lcc +lat_1=17.5 +lat_2=29.5 +lat_0=12 +lon_0=-102 +x_0=2500000 +y_0=0 +ellps=GRS80 +units=m +no_defs"
    transformer = Transformer.from_crs(src_proj4, "EPSG:4326", always_xy=True)

    features = []
    features_web = []
    catalog_rows = []
    indicator_rows = []

    for shape_rec in reader.iterShapeRecords():
        rec = shape_rec.record
        cve_ent = str(rec[idx_cve_ent]).zfill(2)
        nom_ent = str(rec[idx_nomgeo]).strip()
        cvegeo = str(rec[idx_cvegeo]).strip()
        nombre_normalizado = normalize_name(nom_ent)

        geometry = shape_to_geojson_geometry(shape_rec.shape, transformer, simplify=False)
        geometry_web = shape_to_geojson_geometry(shape_rec.shape, transformer, simplify=True)
        demo = demo_indicators(cve_ent)

        feature = {
            "type": "Feature",
            "properties": {
                "cve_ent": cve_ent,
                "cvegeo": cvegeo,
                "nom_ent": nom_ent,
                "nombre_normalizado": nombre_normalizado,
            },
            "geometry": geometry,
        }
        features.append(feature)

        feature_web = {
            "type": "Feature",
            "properties": {
                "cve_ent": cve_ent,
                "cvegeo": cvegeo,
                "nom_ent": nom_ent,
                "nombre_normalizado": nombre_normalizado,
            },
            "geometry": geometry_web,
        }
        features_web.append(feature_web)

        catalog_rows.append({
            "cve_ent": cve_ent,
            "nom_ent": nom_ent,
            "nombre_normalizado": nombre_normalizado,
            "cvegeo": cvegeo,
        })

        indicator_rows.append({
            "cve_ent": cve_ent,
            "nom_ent": nom_ent,
            "nombre_normalizado": nombre_normalizado,
            **demo,
        })

    geojson = {
        "type": "FeatureCollection",
        "name": "mexico_entidades_4326",
        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
        "features": sorted(features, key=lambda f: f["properties"]["cve_ent"]),
    }

    geojson_web = {
        "type": "FeatureCollection",
        "name": "mexico_entidades_4326_web",
        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
        "features": sorted(features_web, key=lambda f: f["properties"]["cve_ent"]),
    }

    with cfg.out_geojson.open("w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))

    with cfg.out_geojson_web.open("w", encoding="utf-8") as f:
        json.dump(geojson_web, f, ensure_ascii=False, separators=(",", ":"))

    catalog_fields = ["cve_ent", "nom_ent", "nombre_normalizado", "cvegeo"]
    with cfg.out_catalogo_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=catalog_fields)
        writer.writeheader()
        writer.writerows(sorted(catalog_rows, key=lambda r: r["cve_ent"]))

    indicator_fields = [
        "cve_ent",
        "nom_ent",
        "nombre_normalizado",
        "L1_proteccion_antidiscriminacion",
        "L2_reconocimiento_parejas",
        "L3_identidad_documentos",
        "C1_mecanismo_denuncia_sancion",
        "C2_prohibicion_ecosig",
        "C3_delitos_odio_registro_protocolo",
        "S1_aceptacion_convivencia_pareja",
        "S2_aceptacion_liderazgo_lgbt_trans",
        "S3_discriminacion_reportada",
        "M1_sintomas_depresivos_ansiosos",
        "M2_ideacion_intento_suicida",
        "M3_acceso_salud_mental",
        "pilar_1_score",
        "pilar_2_score",
        "pilar_3_score",
        "pilar_4_score",
        "rainbow_score",
        "category_status",
        "notes",
    ]
    with cfg.out_indicadores_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=indicator_fields)
        writer.writeheader()
        writer.writerows(sorted(indicator_rows, key=lambda r: r["cve_ent"]))

    print(f"Features exportadas: {len(features)}")
    print(f"GeoJSON: {cfg.out_geojson}")
    print(f"GeoJSON web: {cfg.out_geojson_web}")
    print(f"Catalogo: {cfg.out_catalogo_csv}")
    print(f"Indicadores demo: {cfg.out_indicadores_csv}")


if __name__ == "__main__":
    main()
