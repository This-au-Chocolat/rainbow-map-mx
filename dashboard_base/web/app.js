const INDICATOR_META = {
  rainbow_score: { label: "Indice compuesto (0-100)", type: "numeric" },
  pilar_1_score: { label: "Pilar 1: Existencia de la ley", type: "numeric" },
  pilar_2_score: { label: "Pilar 2: Cumplimiento", type: "numeric" },
  pilar_3_score: { label: "Pilar 3: Percepcion social", type: "numeric" },
  pilar_4_score: { label: "Pilar 4: Salud mental", type: "numeric" },
  L1_proteccion_antidiscriminacion: { label: "L1 Proteccion antidiscriminacion", type: "numeric" },
  L2_reconocimiento_parejas: { label: "L2 Reconocimiento de parejas", type: "numeric" },
  L3_identidad_documentos: { label: "L3 Identidad en documentos", type: "numeric" },
  C1_mecanismo_denuncia_sancion: { label: "C1 Denuncia y sancion", type: "numeric" },
  C2_prohibicion_ecosig: { label: "C2 Prohibicion ECOSIG", type: "numeric" },
  C3_delitos_odio_registro_protocolo: { label: "C3 Delitos de odio", type: "numeric" },
  S1_aceptacion_convivencia_pareja: { label: "S1 Aceptacion convivencia", type: "numeric" },
  S2_aceptacion_liderazgo_lgbt_trans: { label: "S2 Aceptacion liderazgo", type: "numeric" },
  S3_discriminacion_reportada: { label: "S3 Discriminacion reportada", type: "numeric" },
  M1_sintomas_depresivos_ansiosos: { label: "M1 Sintomas depresivos/ansiosos", type: "numeric" },
  M2_ideacion_intento_suicida: { label: "M2 Ideacion/intento suicida", type: "numeric" },
  M3_acceso_salud_mental: { label: "M3 Acceso salud mental", type: "numeric" },
  category_status: { label: "Estatus global", type: "categorical" }
};

const PILLAR_INFO = {
  pilar_1_score: {
    title: "Pilar 1: Existencia de la ley",
    tag: "Marco Normativo",
    content: "Por hacer"
  },
  pilar_2_score: {
    title: "Pilar 2: Cumplimiento",
    tag: "Implementación",
    content: "Por hacer"
  },
  pilar_3_score: {
    title: "Pilar 3: Percepción Social",
    tag: "Sociedad",
    content: "Se observa una brecha crítica entre la aceptación teórica y la realidad vivida. Aunque hay apertura al liderazgo, la discriminación persiste en espacios públicos. Es imperativo desarrollar políticas que combatan activamente el estigma cotidiano."
  },
  pilar_4_score: {
    title: "Pilar 4: Salud Mental",
    tag: "Bienestar",
    content: "Se identifica un pico de vulnerabilidad extrema en jóvenes (15-24 años) con tasas altas de ideación suicida. Existe una negligencia institucional donde más del 50% presenta síntomas pero el acceso a atención profesional es casi inexistente."
  }
};

const NUMERIC_INDICATORS = new Set(
  Object.entries(INDICATOR_META)
    .filter(([, v]) => v.type === "numeric")
    .map(([k]) => k)
);

const CATEGORICAL_PALETTES = {
  marriage_equality: {
    si: "#2a9d8f",
    parcial: "#e9c46a",
    no: "#e76f51"
  },
  anti_discrimination_law: {
    si: "#2a9d8f",
    no: "#e76f51"
  },
  gender_identity_recognition: {
    administrativo: "#2a9d8f",
    judicial: "#f4a261",
    no: "#e76f51"
  },
  adoption_rights: {
    si: "#2a9d8f",
    no: "#e76f51",
    parcial: "#e9c46a"
  },
  hate_crime_legislation: {
    tipificado: "#2a9d8f",
    no_tipificado: "#e76f51"
  },
  category_status: {
    lider: "#1d7874",
    avance: "#679436",
    rezago: "#cc5a71"
  }
};

const COLORS_SCORE = [
  "#f7fbff",
  "#deebf7",
  "#c6dbef",
  "#9ecae1",
  "#6baed6",
  "#3182bd",
  "#08519c"
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
    fetch("../data/processed/indicadores_demo_estatal.csv")
  ]);

  state.geojson = await geoRes.json();
  const csvText = await csvRes.text();
  const rows = parseCsv(csvText);
  rows.forEach((row) => {
    const numericRow = { ...row };
    NUMERIC_INDICATORS.forEach((key) => {
      numericRow[key] = Number(row[key]);
    });

    state.indicatorsByCve.set(row.cve_ent, {
      ...numericRow
    });
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
  const sorted = values.filter((v) => Number.isFinite(v)).sort((a, b) => a - b);
  if (!sorted.length) return [0, 20, 40, 60, 80, 100];
  const min = sorted[0];
  const max = sorted[sorted.length - 1];
  const step = (max - min) / 5 || 1;
  return [min, min + step, min + 2 * step, min + 3 * step, min + 4 * step, max + 0.00001];
}

function colorForNumeric(value, breaks) {
  for (let i = 0; i < breaks.length - 1; i += 1) {
    if (value >= breaks[i] && value < breaks[i + 1]) {
      return COLORS_SCORE[i + 1] || COLORS_SCORE[COLORS_SCORE.length - 1];
    }
  }
  return COLORS_SCORE[0];
}

function styleFeature(feature, breaks) {
  const cve = feature.properties.cve_ent;
  const data = state.indicatorsByCve.get(cve);
  const indicator = state.selectedIndicator;

  let fillColor = "#d9e2ec";
  if (data) {
    if (NUMERIC_INDICATORS.has(indicator)) {
      fillColor = colorForNumeric(Number(data[indicator]), breaks);
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
    for (let i = 0; i < breaks.length - 1; i += 1) {
      const from = breaks[i].toFixed(1);
      const to = breaks[i + 1].toFixed(1);
      html += `<div class="legend-item"><span class="legend-swatch" style="background:${COLORS_SCORE[i + 1]}"></span>${from} - ${to}</div>`;
    }
    legend.innerHTML = html;
    return;
  }

  const palette = CATEGORICAL_PALETTES[indicator] || {};
  let html = `<strong>${label}</strong><br/>`;
  Object.entries(palette).forEach(([label, color]) => {
    html += `<div class="legend-item"><span class="legend-swatch" style="background:${color}"></span>${label}</div>`;
  });
  legend.innerHTML = html;
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

  detail.innerHTML = `
    <h3>${row.nom_ent}</h3>
    <dl>
      <dt>Clave entidad</dt><dd>${row.cve_ent}</dd>
      <dt>Nombre normalizado</dt><dd>${row.nombre_normalizado}</dd>
      <dt>Indice compuesto</dt><dd>${row.rainbow_score}</dd>
      <dt>Pilar 1 score</dt><dd>${row.pilar_1_score}</dd>
      <dt>Pilar 2 score</dt><dd>${row.pilar_2_score}</dd>
      <dt>Pilar 3 score</dt><dd>${row.pilar_3_score}</dd>
      <dt>Pilar 4 score</dt><dd>${row.pilar_4_score}</dd>
      <dt>L1 Proteccion antidiscriminacion</dt><dd>${row.L1_proteccion_antidiscriminacion}</dd>
      <dt>L2 Reconocimiento de parejas</dt><dd>${row.L2_reconocimiento_parejas}</dd>
      <dt>L3 Identidad en documentos</dt><dd>${row.L3_identidad_documentos}</dd>
      <dt>C1 Denuncia y sancion</dt><dd>${row.C1_mecanismo_denuncia_sancion}</dd>
      <dt>C2 Prohibicion ECOSIG</dt><dd>${row.C2_prohibicion_ecosig}</dd>
      <dt>C3 Delitos de odio</dt><dd>${row.C3_delitos_odio_registro_protocolo}</dd>
      <dt>S1 Aceptacion convivencia</dt><dd>${row.S1_aceptacion_convivencia_pareja}</dd>
      <dt>S2 Aceptacion liderazgo</dt><dd>${row.S2_aceptacion_liderazgo_lgbt_trans}</dd>
      <dt>S3 Discriminacion reportada</dt><dd>${row.S3_discriminacion_reportada}</dd>
      <dt>M1 Sintomas depresivos/ansiosos</dt><dd>${row.M1_sintomas_depresivos_ansiosos}</dd>
      <dt>M2 Ideacion/intento suicida</dt><dd>${row.M2_ideacion_intento_suicida}</dd>
      <dt>M3 Acceso salud mental</dt><dd>${row.M3_acceso_salud_mental}</dd>
      <dt>Estatus global</dt><dd>${row.category_status}</dd>
      <dt>notes</dt><dd>${row.notes}</dd>
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
      const category = data ? data.category_status : "sin dato";
      const label = INDICATOR_META[indicator]?.label || indicator;

      layer.bindTooltip(`
        <strong>${feature.properties.nom_ent}</strong><br/>
        Clave: ${cve}<br/>
        Indicador: ${label}<br/>
        Valor: ${value}<br/>
        Categoria: ${category}
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
  const pillars = ["pilar_1_score", "pilar_2_score", "pilar_3_score", "pilar_4_score"];
  
  let data;
  if (cveEnt && state.indicatorsByCve.has(cveEnt)) {
    data = state.indicatorsByCve.get(cveEnt);
  } else {
    // Calculate national average
    data = {};
    pillars.forEach(p => {
      const values = Array.from(state.indicatorsByCve.values()).map(r => r[p]);
      data[p] = values.reduce((a, b) => a + b, 0) / values.length;
    });
  }

  container.innerHTML = "";
  pillars.forEach(p => {
    const value = data[p].toFixed(1);
    const label = INDICATOR_META[p].label.split(":")[1].trim();
    
    const row = document.createElement("div");
    row.className = "pillar-row";
    row.innerHTML = `
      <div class="pillar-label">${label}</div>
      <div class="pillar-bar-wrapper">
        <div class="pillar-bar-bg">
          <div class="pillar-bar-fill" style="width: ${value}%"></div>
        </div>
        <div class="pillar-value">${value}%</div>
      </div>
    `;
    
    row.addEventListener("click", () => showPillarInfo(p));
    container.appendChild(row);
  });
}

function showPillarInfo(pilarKey) {
  const infoPanel = document.getElementById("pillarInfoPanel");
  const info = PILLAR_INFO[pilarKey];
  
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
  initMap();
  renderPillarChart(); // Render national average by default
}

bootstrap().catch((err) => {
  // Keep failure visible in UI for easier debugging during prototyping.
  document.body.innerHTML = `<pre style="padding:1rem">Error inicializando dashboard:\n${String(err)}</pre>`;
  console.error(err);
});
