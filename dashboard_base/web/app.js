const INDICATOR_META = {
  rainbow_score: { label: "Indice compuesto (0-100)", type: "numeric" },
  pilar_3_score: { label: "Pilar 3: Percepcion social", type: "numeric" },
  pilar_4_score: { label: "Pilar 4: Salud mental", type: "numeric" },
  S1_aceptacion_convivencia_pareja: { label: "S1 Aceptacion convivencia", type: "numeric" },
  S2_aceptacion_liderazgo_lgbt_trans: { label: "S2 Aceptacion liderazgo", type: "numeric" },
  S3_discriminacion_reportada: { label: "S3 Prevalencia de discriminacion", type: "numeric" },
  M1_sintomas_depresivos_ansiosos: { label: "M1 Prevalencia sintomas depresivos/ansiosos", type: "numeric" },
  M2_ideacion_intento_suicida: { label: "M2 Prevalencia ideacion/intento suicida", type: "numeric" },
  M3_acceso_salud_mental: { label: "M3 Acceso salud mental", type: "numeric" },
  category_status: { label: "Estatus global compuesto", type: "categorical" }
};

const INDICATOR_DEFINITIONS = {
  rainbow_score:
    "Score compuesto (0-100) calculado como promedio de Pilar 3 y Pilar 4. Alto = mejor desempeno relativo.",
  pilar_3_score:
    "Promedio de S1, S2 y (100 - S3). Resume percepcion social: mayor aceptacion y menor discriminacion.",
  pilar_4_score:
    "Promedio de (100 - M1), (100 - M2) y M3. Resume salud mental con orientacion mejor-es-mayor.",
  S1_aceptacion_convivencia_pareja:
    "Porcentaje estandarizado de aceptacion a que dos personas del mismo sexo vivan juntas en pareja. Alto = mejor.",
  S2_aceptacion_liderazgo_lgbt_trans:
    "Porcentaje estandarizado de aceptacion al liderazgo politico de personas LGBT/trans. Alto = mejor.",
  S3_discriminacion_reportada:
    "Prevalencia estandarizada de discriminacion reportada en los ultimos 12 meses. Alto = peor (mas discriminacion).",
  M1_sintomas_depresivos_ansiosos:
    "Prevalencia estandarizada de sintomas depresivos/ansiosos. Alto = peor (mas afectacion).",
  M2_ideacion_intento_suicida:
    "Prevalencia estandarizada de ideacion o intento suicida. Alto = peor (mayor riesgo).",
  M3_acceso_salud_mental:
    "Porcentaje estandarizado de acceso a atencion en salud mental cuando se requiere. Alto = mejor.",
  category_status:
    "Clasificacion relativa por terciles del score compuesto: rezago, avance o lider."
};

const PILLAR_INFO = {
  rainbow_score: {
    title: "Indice compuesto (0-100)",
    tag: "Resumen",
    content:
      "Promedio simple de Pilar 3 (Percepcion social) y Pilar 4 (Salud mental). Permite una lectura rapida del desempeno relativo de cada entidad."
  },
  pilar_3_score: {
    title: "Pilar 3: Percepcion social",
    tag: "Sociedad",
    content:
      "Integra S1, S2 y S3. Un puntaje mayor implica mayor aceptacion social y menor discriminacion reportada para poblacion LGBT+."
  },
  pilar_4_score: {
    title: "Pilar 4: Salud mental",
    tag: "Bienestar",
    content:
      "Integra M1, M2 y M3 con orientacion mejor-es-mayor. Refleja menor prevalencia de condiciones de riesgo y mejor acceso a atencion."
  }
};

const NUMERIC_INDICATORS = new Set(
  Object.entries(INDICATOR_META)
    .filter(([, v]) => v.type === "numeric")
    .map(([k]) => k)
);

const CATEGORICAL_PALETTES = {
  category_status: {
    lider: "#1d7874",
    avance: "#679436",
    rezago: "#cc5a71"
  }
};

const POSITIVE_COLORS = [
  "#fff5f5",
  "#fdd7c1",
  "#f7b267",
  "#f0ea84",
  "#90be6d",
  "#43aa8b",
  "#1d7874"
];

const NEGATIVE_COLORS = [
  "#f2fbf6",
  "#d6f5e3",
  "#a8e4c4",
  "#f0ea84",
  "#f7b267",
  "#f28482",
  "#d1495b"
];

const NEGATIVE_INDICATORS = new Set([
  "S3_discriminacion_reportada",
  "M1_sintomas_depresivos_ansiosos",
  "M2_ideacion_intento_suicida"
]);

const GLOBAL_STATUS_LABELS = {
  lider: "Favorable",
  avance: "Intermedio",
  rezago: "Atencion prioritaria"
};

const DETAIL_METRICS = [
  "rainbow_score",
  "pilar_3_score",
  "pilar_4_score",
  "S1_aceptacion_convivencia_pareja",
  "S2_aceptacion_liderazgo_lgbt_trans",
  "S3_discriminacion_reportada",
  "M1_sintomas_depresivos_ansiosos",
  "M2_ideacion_intento_suicida",
  "M3_acceso_salud_mental"
];

const state = {
  geojson: null,
  indicatorsByCve: new Map(),
  selectedIndicator: "rainbow_score",
  selectedCve: null,
  layer: null,
  map: null,
  hasFitted: false
};

async function loadData() {
  const [geoRes, csvRes] = await Promise.all([
    fetch("../data/processed/mexico_entidades_4326_web.geojson"),
    fetch("../data/processed/indicadores_dashboard_estatal.csv")
  ]);

  state.geojson = await geoRes.json();
  const csvText = await csvRes.text();
  const rows = parseCsv(csvText);

  rows.forEach((row) => {
    const parsed = { ...row };
    NUMERIC_INDICATORS.forEach((key) => {
      parsed[key] = Number(row[key]);
    });
    parsed.low_reliability_count = Number(row.low_reliability_count || 0);
    state.indicatorsByCve.set(row.cve_ent, parsed);
  });
}

function parseCsv(text) {
  const lines = text.trim().split(/\r?\n/);
  const headers = lines[0].split(",");
  return lines.slice(1).map((line) => {
    const values = line.split(",");
    const obj = {};
    headers.forEach((h, i) => {
      obj[h] = values[i] ?? "";
    });
    return obj;
  });
}

function getNumericBreaks(values) {
  // Escala fija para comparabilidad: 0-100.
  return [0, 20, 40, 60, 80, 100.00001];
}

function clamp01(x) {
  return Math.max(0, Math.min(1, x));
}

function hexToRgb(hex) {
  const n = hex.replace("#", "");
  const v = parseInt(n, 16);
  return {
    r: (v >> 16) & 255,
    g: (v >> 8) & 255,
    b: v & 255
  };
}

function rgbToHex({ r, g, b }) {
  const toHex = (c) => c.toString(16).padStart(2, "0");
  return `#${toHex(r)}${toHex(g)}${toHex(b)}`;
}

function lerpColor(aHex, bHex, t) {
  const a = hexToRgb(aHex);
  const b = hexToRgb(bHex);
  const u = clamp01(t);
  return rgbToHex({
    r: Math.round(a.r + (b.r - a.r) * u),
    g: Math.round(a.g + (b.g - a.g) * u),
    b: Math.round(a.b + (b.b - a.b) * u)
  });
}

function colorForNumeric(value, indicator) {
  const t = clamp01(Number(value) / 100);
  // Semaforo continuo con punto medio amarillo.
  const anchors = NEGATIVE_INDICATORS.has(indicator)
    ? ["#1d7874", "#f0ea84", "#d1495b"] // bajo=mejor, alto=peor
    : ["#d1495b", "#f0ea84", "#1d7874"]; // bajo=peor, alto=mejor

  if (t <= 0.5) {
    return lerpColor(anchors[0], anchors[1], t / 0.5);
  }
  return lerpColor(anchors[1], anchors[2], (t - 0.5) / 0.5);
}

function styleFeature(feature, breaks) {
  const cve = feature.properties.cve_ent;
  const data = state.indicatorsByCve.get(cve);
  const indicator = state.selectedIndicator;

  let fillColor = "#d9e2ec";
  if (data) {
    if (NUMERIC_INDICATORS.has(indicator)) {
      fillColor = colorForNumeric(Number(data[indicator]), indicator);
    } else {
      const palette = CATEGORICAL_PALETTES[indicator] || {};
      fillColor = palette[data[indicator]] || "#bcccdc";
    }
  }

  return {
    fillColor,
    weight: state.selectedCve === cve ? 2.5 : 1,
    opacity: 1,
    color: state.selectedCve === cve ? "#102a43" : "#486581",
    fillOpacity: 0.88
  };
}

function updateLegend(breaks) {
  const legend = document.getElementById("legend");
  const indicator = state.selectedIndicator;
  const label = INDICATOR_META[indicator]?.label || indicator;

  if (NUMERIC_INDICATORS.has(indicator)) {
    let html = `<strong>${label}</strong><br/>`;
    const legendHint = NEGATIVE_INDICATORS.has(indicator)
      ? "Alto = peor (mas prevalencia)"
      : "Alto = mejor";
    html += `<div class="legend-note">${legendHint}</div>`;
    legend.innerHTML = html;
    return;
  }

  const palette = CATEGORICAL_PALETTES[indicator] || {};
  let html = `<strong>${label}</strong><br/>`;
  Object.entries(palette).forEach(([entryLabel, color]) => {
    html += `<div class="legend-item"><span class="legend-swatch" style="background:${color}"></span>${entryLabel}</div>`;
  });
  legend.innerHTML = html;
}

function updateIndicatorDescription() {
  const el = document.getElementById("indicatorDescription");
  const key = state.selectedIndicator;
  const label = INDICATOR_META[key]?.label || key;
  const definition = INDICATOR_DEFINITIONS[key] || "Sin descripcion disponible para este indicador.";
  el.innerHTML = `<strong>Que mide ${label}:</strong><p>${definition}</p>`;
}


function metricRow(label, value) {
  if (!Number.isFinite(value)) return `<dt>${label}</dt><dd>Sin dato</dd>`;
  return `<dt>${label}</dt><dd>${value.toFixed(2)}</dd>`;
}

function classifyIndicatorValue(indicator, value) {
  if (!Number.isFinite(value)) return "Sin clasificacion";

  if (NEGATIVE_INDICATORS.has(indicator)) {
    if (value <= 33.333) return "Favorable";
    if (value <= 66.666) return "Intermedio";
    return "Alerta";
  }

  if (NUMERIC_INDICATORS.has(indicator)) {
    if (value <= 33.333) return "Atencion prioritaria";
    if (value <= 66.666) return "Intermedio";
    return "Favorable";
  }

  return "Sin clasificacion";
}

function updateDetail(cveEnt) {
  const detail = document.getElementById("stateDetail");
  if (!cveEnt) {
    detail.innerHTML = "<p>Sin seleccion.</p>";
    return;
  }

  const row = state.indicatorsByCve.get(cveEnt);
  if (!row) {
    detail.innerHTML = `<p>No hay datos para la entidad ${cveEnt}.</p>`;
    return;
  }

  const metricRows = DETAIL_METRICS.map((key) => metricRow(INDICATOR_META[key].label, row[key])).join("");
  const globalStatusLabel = GLOBAL_STATUS_LABELS[row.category_status] || row.category_status;

  detail.innerHTML = `
    <h3>${row.nom_ent}</h3>
    <dl>
      <dt>Clave entidad</dt><dd>${row.cve_ent}</dd>
      <dt>Nombre normalizado</dt><dd>${row.nombre_normalizado}</dd>
      ${metricRows}
      <dt>Estatus global compuesto</dt><dd>${globalStatusLabel}</dd>
      <dt>Banderas de baja confiabilidad</dt><dd>${row.low_reliability_count}</dd>
      <dt>Notas</dt><dd>${row.notes}</dd>
    </dl>
  `;
}

function renderLayer() {
  if (state.layer) {
    state.layer.remove();
  }

  const indicator = state.selectedIndicator;
  const numericValues = Array.from(state.indicatorsByCve.values()).map((r) => Number(r[indicator]));
  const breaks = getNumericBreaks(numericValues);

  state.layer = L.geoJSON(state.geojson, {
    style: (feature) => styleFeature(feature, breaks),
    onEachFeature: (feature, layer) => {
      const cve = feature.properties.cve_ent;
      const data = state.indicatorsByCve.get(cve);
      const value = data ? data[indicator] : "sin dato";
      const label = INDICATOR_META[indicator]?.label || indicator;

      const printable = Number.isFinite(value) ? Number(value).toFixed(2) : value;
      const indicatorClass = data && Number.isFinite(Number(value))
        ? classifyIndicatorValue(indicator, Number(value))
        : "Sin clasificacion";
      const globalStatus = data
        ? (GLOBAL_STATUS_LABELS[data.category_status] || data.category_status)
        : "sin dato";
      layer.bindTooltip(`
        <strong>${feature.properties.nom_ent}</strong><br/>
        Clave: ${cve}<br/>
        Indicador: ${label}<br/>
        Valor: ${printable}<br/>
        Categoria indicador: ${indicatorClass}<br/>
        Estatus global: ${globalStatus}
      `);

      layer.on("click", () => {
        state.selectedCve = cve;
        updateDetail(cve);
        renderLayer();
        renderPillarChart(cve);
      });

      layer.on("mouseover", () => {
        layer.setStyle({ weight: 2, color: "#243b53" });
      });

      layer.on("mouseout", () => {
        state.layer.resetStyle(layer);
      });
    }
  }).addTo(state.map);

  if (!state.hasFitted) {
    state.map.fitBounds(state.layer.getBounds(), {
      padding: [28, 28],
      maxZoom: 6,
      animate: false
    });
    state.hasFitted = true;
  }

  updateLegend(breaks);
}

function renderPillarChart(cveEnt = null) {
  const container = document.getElementById("pillarChart");
  const bars = ["rainbow_score", "pilar_3_score", "pilar_4_score"];

  let data;
  if (cveEnt && state.indicatorsByCve.has(cveEnt)) {
    data = state.indicatorsByCve.get(cveEnt);
  } else {
    data = {};
    bars.forEach((metric) => {
      const values = Array.from(state.indicatorsByCve.values()).map((r) => r[metric]);
      data[metric] = values.reduce((a, b) => a + b, 0) / values.length;
    });
  }

  container.innerHTML = "";
  bars.forEach((metric) => {
    const value = Number(data[metric] || 0);
    const label = INDICATOR_META[metric].label;

    const row = document.createElement("div");
    row.className = "pillar-row";
    row.innerHTML = `
      <div class="pillar-label">${label}</div>
      <div class="pillar-bar-wrapper">
        <div class="pillar-bar-bg">
          <div class="pillar-bar-fill" style="width: ${value.toFixed(1)}%"></div>
        </div>
        <div class="pillar-value">${value.toFixed(1)}%</div>
      </div>
    `;

    row.addEventListener("click", () => showPillarInfo(metric));
    container.appendChild(row);
  });
}

function showPillarInfo(metricKey) {
  const infoPanel = document.getElementById("pillarInfoPanel");
  const info = PILLAR_INFO[metricKey];

  if (!info) {
    infoPanel.innerHTML = '<div class="info-placeholder"><p>Sin descripcion disponible.</p></div>';
    return;
  }

  infoPanel.innerHTML = `
    <div class="info-content">
      <span class="info-tag">${info.tag}</span>
      <h3>${info.title}</h3>
      <p>${info.content}</p>
    </div>
  `;
}

function initMap() {
  state.map = L.map("map", {
    zoomControl: true,
    attributionControl: false,
    minZoom: 4,
    maxZoom: 8
  });

  renderLayer();
}

function bindControls() {
  const select = document.getElementById("indicatorSelect");
  select.addEventListener("change", (e) => {
    state.selectedIndicator = e.target.value;
    updateIndicatorDescription();
    renderLayer();
    if (state.selectedCve) {
      updateDetail(state.selectedCve);
    }
  });
}

function setLastUpdate() {
  const now = new Date();
  document.getElementById("lastUpdate").textContent = `Actualizacion: ${now.toLocaleString("es-MX")}`;
}

async function bootstrap() {
  await loadData();
  setLastUpdate();
  bindControls();
  updateIndicatorDescription();
  initMap();
  renderPillarChart();
}

bootstrap().catch((err) => {
  document.body.innerHTML = `<pre style="padding:1rem">Error inicializando dashboard:\n${String(err)}</pre>`;
  console.error(err);
});
