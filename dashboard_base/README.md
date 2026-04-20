# Base Dashboard Geoespacial - Rainbow Map Mexico (Prototipo Estatal)

Este entregable construye la base tecnica para un dashboard de derechos LGBT+ por entidad federativa en Mexico, listo para escalar con mas indicadores, tablas y pestanas.

## A. Diagnostico Tecnico

### Capa usada y por que
- Capa seleccionada: `source_inegi/conjunto_de_datos/00ent.shp`.
- Motivo: representa las 32 entidades federativas, que es el nivel requerido para la primera fase del dashboard tipo policy map.
- Campos esperados y usados: `CVE_ENT`, `NOMGEO`, `CVEGEO`.

### Reproyeccion
- Origen: Lambert Conformal Conic ITRF2008 (metros), adecuado para cartografia oficial INEGI.
- Para dashboard web/BI conviene reproyectar a `EPSG:4326` (lat/lon), por compatibilidad con Leaflet, Mapbox, Azure Maps y la mayoria de visuales geoespaciales.
- Decision aplicada: reproyeccion a `EPSG:4326`.

### Formato final recomendado
- Trabajo interno/QA: GeoJSON completo (`mexico_entidades_4326.geojson`).
- Visualizacion web: GeoJSON simplificado (`mexico_entidades_4326_web.geojson`) para rendimiento.
- Tabla de union: CSV con claves robustas (`catalogo_entidades_estandar.csv`).
- Indicadores: CSV modular (`indicadores_demo_estatal.csv`).

## B. Preparacion de Datos Geograficos

### Script implementado
- Archivo: `dashboard_base/scripts/preparar_base_mapa.py`.
- Flujo:
1. Carga `source_inegi/conjunto_de_datos/00ent.shp`.
2. Inspecciona y valida columnas requeridas.
3. Selecciona campos de union: `CVE_ENT`, `NOMGEO`, `CVEGEO`.
4. Estandariza nombre a `nombre_normalizado` (sin acentos, minusculas, guion bajo).
5. Reproyecta de LCC ITRF2008 a `EPSG:4326`.
6. Exporta:
- GeoJSON completo.
- GeoJSON simplificado para web (RDP + redondeo).
- Catalogo de entidades estandarizado.
- Tabla demo de indicadores.

### Comando de ejecucion
```bash
cd /home/this/upy/estancia2
/home/this/upy/estancia2/.venv/bin/python dashboard_base/scripts/preparar_base_mapa.py
```

### Salidas generadas
- `dashboard_base/data/processed/mexico_entidades_4326.geojson` (~39 MB)
- `dashboard_base/data/processed/mexico_entidades_4326_web.geojson` (~220 KB)
- `dashboard_base/data/processed/catalogo_entidades_estandar.csv`
- `dashboard_base/data/processed/indicadores_demo_estatal.csv`

## C. Estructura de Datos para Indicadores

Tabla base para union por estado: `indicadores_demo_estatal.csv`.

Columnas:
- `cve_ent`
- `nom_ent`
- `nombre_normalizado`
- `L1_proteccion_antidiscriminacion`
- `L2_reconocimiento_parejas`
- `L3_identidad_documentos`
- `C1_mecanismo_denuncia_sancion`
- `C2_prohibicion_ecosig`
- `C3_delitos_odio_registro_protocolo`
- `S1_aceptacion_convivencia_pareja`
- `S2_aceptacion_liderazgo_lgbt_trans`
- `S3_discriminacion_reportada`
- `M1_sintomas_depresivos_ansiosos`
- `M2_ideacion_intento_suicida`
- `M3_acceso_salud_mental`
- `pilar_1_score`
- `pilar_2_score`
- `pilar_3_score`
- `pilar_4_score`
- `rainbow_score`
- `category_status`
- `notes`

Notas:
- En el prototipo actual, los valores son ficticios y deterministas (solo para probar visualizacion/uniones).
- Escala de subindicadores demo: 0-2.
- Escala de `rainbow_score`: 0-100 (normalizado a partir de los cuatro pilares).
- La union principal debe ser por `cve_ent` (texto de 2 digitos, con cero a la izquierda).
- `nom_ent` y `nombre_normalizado` quedan como soporte para QA y reconciliacion semantica.

## D. Diseno Base del Dashboard

Estructura inicial implementada:
- Encabezado institucional con contexto de derechos humanos.
- Mapa coropletico principal por entidad.
- Panel lateral de detalle por estado (clic).
- Selector de indicador.
- Tooltip en hover.
- Leyenda dinamica.
- Seccion de placeholders para futuras pestanas/tablas.

Paleta visual:
- Base institucional sobria (azules/grises/verdes suaves).
- Uso moderado de colores de estado/categoria.
- Evita saturacion de arcoiris para mantener legibilidad y tono policy dashboard.

Archivos UI:
- `dashboard_base/web/index.html`
- `dashboard_base/web/styles.css`
- `dashboard_base/web/app.js`

## E. Implementacion

### Opcion implementada: Web (Leaflet)

Funcionalidades ya operativas:
- Muestra 32 entidades federativas.
- Coropleta por indicador numerico o categorico.
- Hover tooltip con:
- nombre estado
- clave
- valor
- categoria
- Clic en entidad para detalle lateral.
- Leyenda dinamica segun indicador.

### Ejecutar dashboard
```bash
cd /home/this/upy/estancia2/dashboard_base
python3 -m http.server 8090
```
Luego abrir:
- `http://127.0.0.1:8090/web/index.html`

### Ruta Power BI (recomendada en paralelo)
1. Importar `catalogo_entidades_estandar.csv` y `indicadores_demo_estatal.csv`.
2. Mantener `cve_ent` como texto (no numerico) para no perder ceros iniciales.
3. Para mapa:
- Opcion A: Shape Map con TopoJSON estatal (si preparan conversion posterior desde GeoJSON).
- Opcion B: Azure Maps (preferible para mantenimiento y capas futuras).
4. Crear relacion 1:1 por `cve_ent` entre catalogo e indicadores.
5. Configurar tooltip fields: `nom_ent`, `cve_ent`, `rainbow_score`, `category_status`.
6. Crear slicers por subindicadores `L*`, `C*`, `S*`, `M*` y por `category_status`.

## F. Escalabilidad

Buenas practicas para crecer sin romper:
- Mantener `cve_ent` como clave canonica en todas las tablas.
- Definir un diccionario de datos versionado para cada indicador.
- Separar capas:
- `geo` (geometrias)
- `dimensiones` (catalogos)
- `hechos` (indicadores por periodo)
- Agregar columna `periodo` en tablas futuras para series temporales.
- Validar integridad de union en cada carga:
- 32 claves unicas en geografia.
- 32 claves unicas por periodo en hechos.
- Prever capas futuras (municipios/AGEB) con jerarquia:
- `cve_ent` -> `cve_mun` -> `cve_ageb`.
- Evitar uniones por nombre como clave principal.

## G. Entregables Esperados (Cumplidos)

1. Propuesta tecnica documentada: este `README`.
2. Flujo de transformacion shapefile: `dashboard_base/scripts/preparar_base_mapa.py`.
3. Estructura de tabla de indicadores: `dashboard_base/data/processed/indicadores_demo_estatal.csv`.
4. Version minima funcional del dashboard:
- `dashboard_base/web/index.html`
- `dashboard_base/web/styles.css`
- `dashboard_base/web/app.js`
5. Recomendaciones de continuidad y ruta Power BI: secciones E/F.

## Errores Comunes y Como Evitarlos

- `cve_ent` sin cero inicial (`1` en vez de `01`): rompe la union.
- Union por nombre (`nom_ent`) en lugar de clave: genera mismatches por acentos/variantes.
- No reproyectar a `EPSG:4326` para web: mapa desalineado o no visible.
- Usar geometria pesada sin simplificar: tiempos altos de carga y mala UX.
- No validar cobertura completa: debe haber 32 entidades en geometria y en indicadores.
- Mezclar tipos de dato en clave (`texto` vs `numero`): relaciones rotas en BI.

## Suposiciones Documentadas

- El shapefile `00ent.shp` incluye `CVE_ENT`, `NOMGEO`, `CVEGEO` (confirmado en esta corrida).
- El shapefile `source_inegi/conjunto_de_datos/00ent.shp` incluye `CVE_ENT`, `NOMGEO`, `CVEGEO` (confirmado en esta corrida).
- `CVE_ENT` es unica por entidad y coincide con codificacion oficial INEGI (01-32).
- Los indicadores actuales son placeholders y seran sustituidos por datos reales en siguientes fases.
