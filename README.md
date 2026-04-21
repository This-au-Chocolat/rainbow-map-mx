# Atlas LGBTIQ+ MX - Pipeline y Dashboard Base (Estado Actual)

Fecha de actualizacion: 2026-04-20

## 1) Que contiene este repositorio

Este repositorio implementa un flujo reproducible para construir indicadores estatales (32 entidades) de:

- Pilar 3 (Percepcion social): `S1`, `S2`, `S3`
- Pilar 4 (Salud mental): `M1`, `M2`, `M3`

El alcance actual cubre:

- Inventario de archivos locales ENADIS 2022 y ENDISEG 2021.
- Descubrimiento automatico de variables desde diccionarios.
- Construccion de indicadores ponderados por entidad.
- Flags de confiabilidad.
- Export de tablas para consumo posterior.
- Dashboard base de mapa (Leaflet) conectado a los resultados procesados.

No incluye aun una capa de producto final ni autenticacion, APIs o despliegue.

## 2) Estructura principal

- `src/pipeline/`
  - `pipeline.py`: inventario, discovery, construccion de indicadores, scoring y exports.
  - `run_pipeline.py`: entrada para corrida completa.
- `dashboard_base/scripts/`
  - `preparar_base_mapa.py`: utilitario base de mapa.
  - `generar_indicadores_dashboard.py`: construye feed del dashboard desde `outputs/`.
- `dashboard_base/web/`
  - `index.html`: layout del dashboard.
  - `app.js`: logica de mapa, selector de indicador y detalle por entidad.
  - `styles.css`: estilos de interfaz.
- `notebooks/`
  - `00_setup_and_project_structure.ipynb`
  - `01_file_inventory_and_metadata_scan.ipynb`
  - `02_variable_discovery.ipynb`
  - `03_population_definition_and_harmonization.ipynb`
  - `04_indicator_construction.ipynb`
  - `05_scoring_thresholds_and_quality_flags.ipynb`
  - `06_exports_and_methodology_summary.ipynb`
- `source_inegi/`
  - Fuentes locales ENADIS/ENDISEG (microdatos, catalogos, diccionarios y metadatos).
- `data/intermediate/`
  - Artefactos intermedios del pipeline.
- `outputs/`
  - Tablas finales de indicadores por estado.
- `dashboard_base/data/processed/`
  - Base geoespacial web y feed CSV del dashboard.

## 3) Estado actual del pipeline (verificado)

Tablas finales generadas:

- `outputs/state_scores_long.parquet` -> `(192, 19)`
- `outputs/state_scores_wide.parquet` -> `(32, 20)`
- `outputs/indicator_metadata.parquet` -> `(6, 23)`
- `outputs/quality_flags.parquet` -> `(192, 9)`
- `outputs/variable_crosswalk.parquet` -> `(6, 23)`

Cobertura:

- 32 entidades federativas.
- 6 criterios completos: `S1`, `S2`, `S3`, `M1`, `M2`, `M3`.

Artefactos intermedios:

- `data/intermediate/file_manifest.parquet`
- `data/intermediate/dictionary_index.parquet`
- `data/intermediate/variable_candidates.parquet`

## 4) Regla de fuente por criterio

Implementado actualmente:

- `S1`, `S2` -> ENADIS 2022
- `S3`, `M1`, `M2`, `M3` -> ENDISEG 2021

Detalle metodologico de seleccion y confianza:

- `outputs/variable_crosswalk.parquet`
- `outputs/indicator_metadata.parquet`

## 5) Feed del dashboard y semantica

El dashboard consume:

- `dashboard_base/data/processed/indicadores_dashboard_estatal.csv`

Este archivo se genera con:

```bash
python dashboard_base/scripts/generar_indicadores_dashboard.py
```

Columnas principales del feed:

- `S1_aceptacion_convivencia_pareja`
- `S2_aceptacion_liderazgo_lgbt_trans`
- `S3_discriminacion_reportada` (prevalencia; alto = peor)
- `M1_sintomas_depresivos_ansiosos` (prevalencia; alto = peor)
- `M2_ideacion_intento_suicida` (prevalencia; alto = peor)
- `M3_acceso_salud_mental` (alto = mejor)
- `pilar_3_score` (score orientado mejor-es-mayor)
- `pilar_4_score` (score orientado mejor-es-mayor)
- `rainbow_score` (promedio de pilares 3 y 4)
- `category_status` (terciles del score compuesto)
- `low_reliability_count`, `notes`

## 6) Como correr todo desde cero

Los comandos de esta seccion estan escritos para ser portables. No dependen de rutas locales especificas.

### 6.0 Requisitos

- Python 3.11+
- Git

Para regenerar indicadores tambien necesitas tener las fuentes en `source_inegi/` (ENADIS 2022 y ENDISEG 2021).

### 6.1 Inicio rapido (solo visualizar el mapa)

Si solo quieres ver el dashboard con el CSV ya incluido en el repo:

```bash
cd dashboard_base
python3 -m http.server 8000
```

Abre `http://localhost:8000/web/`.

### 6.2 Clonar y preparar entorno

```bash
git clone <URL_DEL_REPO>
cd rainbow-map-mx
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e .
```

Nota: si no usas instalacion editable, puedes instalar dependencias manualmente desde `pyproject.toml`.

### 6.3 Pipeline de indicadores

```bash
python src/pipeline/run_pipeline.py
```

### 6.4 Feed del dashboard

```bash
python dashboard_base/scripts/generar_indicadores_dashboard.py
```

### 6.5 Levantar dashboard local

Importante: correr el servidor desde `dashboard_base` para que las rutas relativas funcionen.

```bash
cd dashboard_base
python3 -m http.server 8000
```

Abrir:

- `http://localhost:8000/web/`

## 7) Notebooks

La suite de notebooks existe para trazabilidad metodologica y reproduccion paso a paso.

Ejecucion headless (opcional):

```bash
python -m pip install jupyter ipykernel nbconvert nbclient
jupyter nbconvert --to notebook --execute notebooks/00_setup_and_project_structure.ipynb --output-dir notebooks
```

Si quieres ejecutar toda la suite, repite `nbconvert --execute` para `01` a `06`.

## 8) Consideraciones y limites conocidos

- El dashboard es una capa de visualizacion base; no es producto final.
- `S3`, `M1`, `M2` se muestran como prevalencias (alto = peor), pero los scores de pilares invierten correctamente esas dimensiones para mantener interpretacion uniforme en puntajes agregados.
- Existe una estandarizacion por tamano muestral comun por estado en el feed del dashboard para reducir inestabilidad entre entidades con `n` bajo.
- Si se detectan discrepancias sustantivas en algun indicador (ej. S3), la revision debe hacerse en el pipeline base (universos/filtros/denominadores) antes de ajustar visualizacion.

## 9) Estado actual de frontend

Actualmente el mapa incluye:

- Selector de indicador.
- Leyenda textual de interpretacion (alto=mejor o alto=peor segun indicador).
- Detalle por entidad al hacer clic.
- Secciones de hallazgos y grafica de pilares.

No incluye en esta version:

- Barra lateral de referencia vertical.
- Ranking nacional debajo del mapa.

(Se removieron para mantener una interfaz estable y limpia.)
