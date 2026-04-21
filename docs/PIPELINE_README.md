# Pipeline ENADIS 2022 + ENDISEG 2021 (Pilares 3 y 4)

## Objetivo
Pipeline reproducible para construir indicadores estatales (32 entidades) de:
- Pilar 3 (Percepcion social): S1, S2, S3
- Pilar 4 (Salud mental): M1, M2, M3

Todos los puntajes finales se reportan en escala 0-100 con orientacion `higher_is_better`.

## Alcance metodologico
- Insumos estrictamente locales en `source_inegi/`.
- Descubrimiento automatico de variables desde diccionarios y metadatos.
- Regla de prioridad por fuente:
  - ENADIS 2022: S1, S2
  - ENDISEG 2021: S3, M1, M2, M3
- Fallback explicitado via `fallback_used` en metadatos y tablas finales.
- Estimaciones ponderadas estatales con diagnosticos de incertidumbre:
  - `unweighted_n`
  - `se_if_available`
  - `ci_low_if_available`
  - `ci_high_if_available`
  - bandera de baja confiabilidad

## Estructura de proyecto
- `notebooks/`: flujo analitico y documentacion ejecutable
- `src/pipeline/`: funciones de inventario, discovery, construccion de indicadores y export
- `data/intermediate/`: artefactos intermedios (manifest, indice de diccionario, candidatos)
- `outputs/`: tablas finales para integracion posterior
- `docs/`: metodologia y contratos de datos

## Instalacion con uv
```bash
cd /home/this/upy/estancia2
uv sync
uv run python -m ipykernel install --user --name atlas-lgbtiq-pipeline
```

## Ejecucion completa
```bash
cd /home/this/upy/estancia2
uv run python src/pipeline/run_pipeline.py
```

## Salidas obligatorias
- `outputs/state_scores_long.parquet`
- `outputs/state_scores_wide.parquet`
- `outputs/indicator_metadata.parquet`
- `outputs/quality_flags.parquet`
- `outputs/variable_crosswalk.parquet`

## Reglas de calidad y confiabilidad
`low_reliability_flag = True` si se cumple al menos una condicion:
- `unweighted_n < 30`
- `n_eff < 60` (aprox. Kish)
- `cv > 0.30`
- ancho del IC95 mayor a 25 puntos porcentuales

## Umbrales interpretativos
Se aplica una regla empirica reproducible por criterio:
- tercil inferior: `low`
- tercil medio: `medium`
- tercil superior: `high`

Se exporta tambien `optional_score_0_1_2` para compatibilidad.

## Limitaciones documentadas
- El descubrimiento automatico utiliza coincidencia de texto sobre diccionarios; siempre revisar candidatos en `data/intermediate/variable_candidates.parquet`.
- Algunas etiquetas ENADIS usan codificacion de caracteres no UTF-8; se normaliza a ASCII para matching.
- En variables de actitud tipo Likert, el evento positivo se aproxima con codigos favorables directos (`1`) y se documenta en `variable_crosswalk`.
