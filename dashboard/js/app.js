/**
 * Personalism in Dictatorships — Interactive Data Explorer
 *
 * Pure vanilla JS dashboard: no build step, no frameworks.
 * Reads data/personalism.json and renders overview charts, country profiles,
 * indicator cards, and a filterable data table.
 */

// ============================================================================
// State
// ============================================================================

let DATA = null;            // raw JSON payload
let allLeaders = [];        // flattened leader records with scores
let indicatorMeta = [];     // indicator metadata from JSON
let irtResult = null;       // IRT estimation output

const POWER_INDICATORS = ['term_limits_removed', 'president_for_life', 'appointment_monopoly', 'family_in_govt', 'family_in_govt_binary'];
const CULT_INDICATORS = ['currency_portrait', 'oath_to_person', 'places_named', 'places_named_binary', 'grandiose_titles', 'grandiose_titles_binary'];

// Colors for indicators in IRT plots
const ITEM_COLORS = ['#2563EB', '#DC2626', '#D97706'];
const TEST_COLOR  = '#374151';

// ============================================================================
// Data loading & preprocessing
// ============================================================================

async function loadData() {
    try {
        const resp = await fetch('data/personalism.json');
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        DATA = await resp.json();
    } catch (e) {
        console.error('Failed to load data:', e);
        document.querySelector('main').innerHTML =
            `<p style="padding:2rem;color:#c00">Failed to load data. Make sure <code>data/personalism.json</code> exists.</p>`;
        return;
    }

    indicatorMeta = DATA.metadata.indicators || [];

    // Flatten to leader-level records
    allLeaders = [];
    for (const country of DATA.countries) {
        for (const leader of country.leaders) {
            const indicators = leader.indicators || {};
            const indicatorKeys = indicatorMeta.map(m => m.key);

            // Compute composite score (proportion of present indicators)
            let coded = 0, present = 0;
            for (const k of indicatorKeys) {
                const v = indicators[k];
                if (v === 1 || v === 0) {
                    coded++;
                    if (v === 1) present++;
                }
            }
            const score = coded > 0 ? present / coded : null;

            // Compute dimension sub-scores
            const powerKeys = indicatorKeys.filter(k => {
                const m = indicatorMeta.find(x => x.key === k);
                return m && m.dimension === 'power';
            });
            const cultKeys = indicatorKeys.filter(k => {
                const m = indicatorMeta.find(x => x.key === k);
                return m && m.dimension === 'cult';
            });

            const powerScore = subscoreFor(indicators, powerKeys);
            const cultScore = subscoreFor(indicators, cultKeys);

            allLeaders.push({
                iso3: country.iso3,
                country: country.name,
                leader: leader.name,
                startYear: leader.start_year,
                endYear: leader.end_year,
                indicators,
                score,
                powerScore,
                cultScore,
            });
        }
    }

    // Sort by score descending
    allLeaders.sort((a, b) => (b.score || 0) - (a.score || 0));

    renderAll();
}

function subscoreFor(indicators, keys) {
    let coded = 0, present = 0;
    for (const k of keys) {
        const v = indicators[k];
        if (v === 1 || v === 0) {
            coded++;
            if (v === 1) present++;
        }
    }
    return coded > 0 ? present / coded : null;
}

// ============================================================================
// Rendering
// ============================================================================

function renderAll() {
    renderStats();
    renderMissingness();
    renderBarChart();
    renderPrevalenceChart();
    renderScatterChart();
    renderCountryGrid();
    renderIndicatorGrid();
    runIRT();
    renderDataTable();
    setupEventListeners();
}

// --- Stats ---
function renderStats() {
    const countries = new Set(allLeaders.map(l => l.iso3));
    document.getElementById('stat-countries').textContent = countries.size;
    document.getElementById('stat-leaders').textContent = allLeaders.length;
    document.getElementById('stat-indicators').textContent = indicatorMeta.length;

    const minY = Math.min(...allLeaders.map(l => l.startYear));
    const maxY = Math.max(...allLeaders.map(l => l.endYear));
    document.getElementById('stat-year-range').textContent = `${minY}–${maxY}`;
}

// --- Missingness / Data Coverage ---
function renderMissingness() {
    const container = document.getElementById('missingness-chart');
    if (!container) return;

    const universe = DATA.metadata.universe || {};
    const totalArchigos = universe.total_archigos || allLeaders.length;
    const withWiki      = universe.with_wikipedia || totalArchigos;
    const withWikidata  = universe.with_wikidata || allLeaders.length;

    // Per-indicator coverage within matched set
    const rows = [
        { label: 'Archigos → Wikipedia link', coded: withWiki, total: totalArchigos },
        { label: 'Wikipedia → Wikidata QID', coded: withWikidata, total: withWiki },
    ];

    for (const meta of indicatorMeta) {
        let coded = 0;
        for (const l of allLeaders) {
            const v = l.indicators[meta.key];
            if (v === 0 || v === 1) coded++;
        }
        rows.push({ label: meta.label, coded, total: allLeaders.length, indicator: true });
    }

    // Overall
    rows.unshift({ label: 'Overall coverage (Archigos → coded)', coded: withWikidata, total: totalArchigos, overall: true });

    container.innerHTML = rows.map(r => {
        const pct = r.total > 0 ? (r.coded / r.total * 100).toFixed(1) : '—';
        const barPct = r.total > 0 ? (r.coded / r.total * 100) : 0;
        const missing = r.total - r.coded;
        const missingPct = r.total > 0 ? (missing / r.total * 100).toFixed(1) : '0';
        const cssClass = r.overall ? 'miss-overall' : r.indicator ? 'miss-indicator' : 'miss-pipeline';
        return `
            <div class="miss-row ${cssClass}">
                <span class="miss-label">${esc(r.label)}</span>
                <div class="miss-track">
                    <div class="miss-fill" style="width:${barPct}%"></div>
                </div>
                <span class="miss-stats">
                    <span class="miss-coded">${r.coded.toLocaleString()}</span>/<span class="miss-total">${r.total.toLocaleString()}</span>
                    <span class="miss-pct">(${pct}%)</span>
                    <span class="miss-missing" title="Missing">${missing > 0 ? missing.toLocaleString() + ' missing' : '✓ complete'}</span>
                </span>
            </div>`;
    }).join('');
}

// --- Bar chart: composite scores ---
function renderBarChart() {
    const container = document.getElementById('bar-chart');
    const sorted = [...allLeaders].sort((a, b) => (b.score || 0) - (a.score || 0));

    container.innerHTML = sorted.map(l => {
        const pct = ((l.score || 0) * 100).toFixed(0);
        const color = scoreColor(l.score || 0);
        return `
            <div class="bar-row">
                <span class="bar-label" title="${esc(l.leader)} (${esc(l.country)})">${esc(l.leader)}</span>
                <div class="bar-track">
                    <div class="bar-fill" style="width:${pct}%;background:${color}"></div>
                </div>
                <span class="bar-value">${pct}%</span>
            </div>`;
    }).join('');
}

// --- Prevalence chart ---
function renderPrevalenceChart() {
    const container = document.getElementById('prevalence-chart');

    const prevalences = indicatorMeta.map(meta => {
        let coded = 0, present = 0;
        for (const l of allLeaders) {
            const v = l.indicators[meta.key];
            if (v === 1 || v === 0) {
                coded++;
                if (v === 1) present++;
            }
        }
        return { ...meta, prevalence: coded > 0 ? present / coded : 0, coded, present };
    }).sort((a, b) => b.prevalence - a.prevalence);

    container.innerHTML = prevalences.map(p => {
        const pct = (p.prevalence * 100).toFixed(0);
        const color = p.dimension === 'power' ? 'var(--power-color)' : 'var(--cult-color)';
        return `
            <div class="bar-row">
                <span class="bar-label" title="${esc(p.label)}">${esc(p.label)}</span>
                <div class="bar-track">
                    <div class="bar-fill" style="width:${pct}%;background:${color}"></div>
                </div>
                <span class="bar-value">${pct}%</span>
            </div>`;
    }).join('');
}

// --- Scatter chart: power vs cult ---
function renderScatterChart() {
    const container = document.getElementById('scatter-chart');
    const padding = 20; // px from edges

    let html = `<div class="scatter-container">`;
    html += `<span class="axis-label axis-label-x">Power concentration →</span>`;
    html += `<span class="axis-label axis-label-y">Personality cult →</span>`;

    for (const l of allLeaders) {
        if (l.powerScore === null || l.cultScore === null) continue;
        const x = padding + l.powerScore * (100 - 2 * padding);
        const y = padding + l.cultScore * (100 - 2 * padding);
        html += `<div class="scatter-point" style="left:${x}%;bottom:${y}%">
            <span class="tooltip">${esc(l.leader)} (${esc(l.country)})</span>
        </div>`;
    }

    html += `</div>`;
    container.innerHTML = html;
}

// --- Country grid ---
function renderCountryGrid(search = '', sortBy = 'name') {
    const container = document.getElementById('country-grid');

    // Group leaders by country
    const countryMap = new Map();
    for (const l of allLeaders) {
        if (!countryMap.has(l.iso3)) {
            countryMap.set(l.iso3, { iso3: l.iso3, name: l.country, leaders: [] });
        }
        countryMap.get(l.iso3).leaders.push(l);
    }

    let countries = [...countryMap.values()];

    // Compute max score per country
    for (const c of countries) {
        c.maxScore = Math.max(...c.leaders.map(l => l.score || 0));
    }

    // Filter
    if (search) {
        const term = search.toLowerCase();
        countries = countries.filter(c =>
            c.name.toLowerCase().includes(term) ||
            c.iso3.toLowerCase().includes(term) ||
            c.leaders.some(l => l.leader.toLowerCase().includes(term))
        );
    }

    // Sort
    if (sortBy === 'score-desc') countries.sort((a, b) => b.maxScore - a.maxScore);
    else if (sortBy === 'score-asc') countries.sort((a, b) => a.maxScore - b.maxScore);
    else countries.sort((a, b) => a.name.localeCompare(b.name));

    container.innerHTML = countries.map(c => {
        const leadersHtml = c.leaders.map(l => {
            const pct = ((l.score || 0) * 100).toFixed(0);
            const dotsHtml = indicatorMeta.map(m => {
                const v = l.indicators[m.key];
                const cls = v === 1 ? 'present' : 'absent';
                const symbol = v === 1 ? '✓' : '·';
                return `<span class="indicator-dot ${cls}" title="${esc(m.label)}"><span class="dot-tooltip">${esc(m.label)}</span>${symbol}</span>`;
            }).join('');

            return `
                <div class="leader-row">
                    <span>
                        <span class="leader-name">${esc(l.leader)}</span>
                        <span class="leader-years">${l.startYear}–${l.endYear}</span>
                    </span>
                    <span class="leader-score" style="color:${scoreColor(l.score || 0)}">${pct}%</span>
                </div>
                <div class="indicator-dots">${dotsHtml}</div>`;
        }).join('');

        const topScore = ((c.maxScore || 0) * 100).toFixed(0);
        return `
            <article class="country-card">
                <h3>${esc(c.name)}<span class="country-iso">${c.iso3}</span></h3>
                <span class="country-score" style="background:${scoreColor(c.maxScore || 0, true)}">${topScore}%</span>
                ${leadersHtml}
            </article>`;
    }).join('');
}

// --- Indicator grid ---
function renderIndicatorGrid() {
    const container = document.getElementById('indicator-grid');

    container.innerHTML = indicatorMeta.map(meta => {
        let coded = 0, present = 0;
        const presentLeaders = [];
        for (const l of allLeaders) {
            const v = l.indicators[meta.key];
            if (v === 1 || v === 0) {
                coded++;
                if (v === 1) { present++; presentLeaders.push(l.leader); }
            }
        }
        const prevalence = coded > 0 ? present / coded : 0;
        const pct = (prevalence * 100).toFixed(0);
        const color = meta.dimension === 'power' ? 'var(--power-color)' : 'var(--cult-color)';
        const dimLabel = meta.dimension === 'power' ? 'Power Concentration' : 'Personality Cult';

        return `
            <article class="indicator-card">
                <h3>${esc(meta.label)}</h3>
                <span class="indicator-dimension ${meta.dimension}">${dimLabel}</span>
                <p class="indicator-desc">${esc(meta.description || '')}</p>
                <p class="indicator-source">Source: ${esc(meta.source || 'TBD')}</p>
                <div class="indicator-bar-track">
                    <div class="indicator-bar-fill" style="width:${pct}%;background:${color}"></div>
                </div>
                <p class="indicator-prevalence">${present} of ${coded} leaders (${pct}%)</p>
            </article>`;
    }).join('');
}

// --- Data table ---
function renderDataTable(filter = '') {
    const headerRow = document.getElementById('table-header');
    const tbody = document.getElementById('table-body');
    const indicatorKeys = indicatorMeta.map(m => m.key);

    // Header
    headerRow.innerHTML = `
        <th>Country</th>
        <th>Leader</th>
        <th>Years</th>
        <th>Score</th>
        ${indicatorMeta.map(m => `<th title="${esc(m.label)}">${abbreviate(m.label)}</th>`).join('')}
    `;

    // Rows
    let leaders = allLeaders;
    if (filter) {
        const term = filter.toLowerCase();
        leaders = leaders.filter(l =>
            l.country.toLowerCase().includes(term) ||
            l.leader.toLowerCase().includes(term) ||
            l.iso3.toLowerCase().includes(term)
        );
    }

    tbody.innerHTML = leaders.map(l => {
        const pct = ((l.score || 0) * 100).toFixed(0);
        const cells = indicatorKeys.map(k => {
            const v = l.indicators[k];
            if (v === 1) return `<td><span class="cell-1">1</span></td>`;
            if (v === 0) return `<td><span class="cell-0">0</span></td>`;
            return `<td>—</td>`;
        }).join('');

        return `<tr>
            <td>${esc(l.country)}</td>
            <td>${esc(l.leader)}</td>
            <td>${l.startYear}–${l.endYear}</td>
            <td><strong>${pct}%</strong></td>
            ${cells}
        </tr>`;
    }).join('');
}

// ============================================================================
// IRT Analysis
// ============================================================================

function runIRT() {
    const indicatorKeys = indicatorMeta.map(m => m.key);
    // Build N × J binary matrix
    const dataMatrix = allLeaders.map(l =>
        indicatorKeys.map(k => {
            const v = l.indicators[k];
            return (v === 0 || v === 1) ? v : null;
        })
    );

    const model = new IRT2PL();
    irtResult = model.fit(dataMatrix);

    // Attach theta to each leader
    for (let i = 0; i < allLeaders.length; i++) {
        allLeaders[i].theta   = irtResult.persons[i].theta;
        allLeaders[i].thetaSE = irtResult.persons[i].se;
    }

    renderIRTFitCards();
    renderICCPlot();
    renderInfoPlot();
    renderItemParamsTable();
    renderPatternTable();
    renderRanking();
}

function renderIRTFitCards() {
    const container = document.getElementById('irt-fit-cards');
    if (!container || !irtResult) return;
    const f = irtResult.fit;
    container.innerHTML = [
        { label: 'Log-likelihood', value: f.logLik.toFixed(1) },
        { label: 'AIC', value: f.aic.toFixed(1) },
        { label: 'BIC', value: f.bic.toFixed(1) },
        { label: 'Marginal reliability', value: f.reliability.toFixed(3) },
        { label: 'EM iterations', value: f.iterations },
        { label: 'Items × Persons', value: `${f.nItems} × ${f.nObs}` },
    ].map(c => `
        <div class="stat-card">
            <span class="stat-number">${c.value}</span>
            <span class="stat-label">${c.label}</span>
        </div>
    `).join('');
}

// --- SVG helpers ---

function svgPlot({ width = 700, height = 340, marginL = 60, marginR = 20, marginT = 15, marginB = 45,
                   xMin = -4, xMax = 4, yMin = 0, yMax = 1,
                   xLabel = 'θ', yLabel = '', xTicks, yTicks, curves, legend }) {

    const pW = width - marginL - marginR;
    const pH = height - marginT - marginB;
    const xScale = (v) => marginL + (v - xMin) / (xMax - xMin) * pW;
    const yScale = (v) => marginT + pH - (v - yMin) / (yMax - yMin) * pH;

    if (!xTicks) xTicks = [-4, -3, -2, -1, 0, 1, 2, 3, 4];
    if (!yTicks) {
        const step = yMax <= 1 ? 0.25 : yMax <= 2 ? 0.5 : 1;
        yTicks = [];
        for (let v = yMin; v <= yMax + step * 0.01; v += step) yTicks.push(parseFloat(v.toFixed(2)));
    }

    let svg = `<svg viewBox="0 0 ${width} ${height}" class="irt-svg" xmlns="http://www.w3.org/2000/svg">`;

    // Light grid lines
    for (const yt of yTicks) {
        svg += `<line x1="${marginL}" y1="${yScale(yt)}" x2="${width - marginR}" y2="${yScale(yt)}" stroke="#e5e5e5" stroke-width="0.7"/>`;
    }

    // Axes
    svg += `<line x1="${marginL}" y1="${yScale(yMin)}" x2="${width - marginR}" y2="${yScale(yMin)}" stroke="#888" stroke-width="1"/>`;
    svg += `<line x1="${marginL}" y1="${yScale(yMin)}" x2="${marginL}" y2="${yScale(yMax)}" stroke="#888" stroke-width="1"/>`;

    // X ticks + labels
    for (const xt of xTicks) {
        const x = xScale(xt);
        svg += `<line x1="${x}" y1="${yScale(yMin)}" x2="${x}" y2="${yScale(yMin) + 5}" stroke="#888" stroke-width="1"/>`;
        svg += `<text x="${x}" y="${yScale(yMin) + 18}" text-anchor="middle" class="tick-label">${xt}</text>`;
    }
    // Y ticks + labels
    for (const yt of yTicks) {
        const y = yScale(yt);
        svg += `<line x1="${marginL - 5}" y1="${y}" x2="${marginL}" y2="${y}" stroke="#888" stroke-width="1"/>`;
        svg += `<text x="${marginL - 8}" y="${y + 4}" text-anchor="end" class="tick-label">${yt}</text>`;
    }

    // Axis labels
    svg += `<text x="${marginL + pW / 2}" y="${height - 4}" text-anchor="middle" class="axis-label-svg">${xLabel}</text>`;
    if (yLabel) {
        svg += `<text x="14" y="${marginT + pH / 2}" text-anchor="middle" class="axis-label-svg" transform="rotate(-90 14 ${marginT + pH / 2})">${yLabel}</text>`;
    }

    // Curves
    for (const curve of curves) {
        const pts = curve.points.map(p => `${xScale(p.x).toFixed(1)},${yScale(p.y).toFixed(1)}`).join(' ');
        const dash = curve.dashed ? ' stroke-dasharray="6,3"' : '';
        const sw = curve.strokeWidth || 2;
        svg += `<polyline points="${pts}" fill="none" stroke="${curve.color}" stroke-width="${sw}"${dash}/>`;
    }

    // Legend
    if (legend && legend.length) {
        const lx = marginL + 12, ly = marginT + 10;
        for (let i = 0; i < legend.length; i++) {
            const y = ly + i * 18;
            const dash = legend[i].dashed ? ' stroke-dasharray="5,3"' : '';
            svg += `<line x1="${lx}" y1="${y}" x2="${lx + 20}" y2="${y}" stroke="${legend[i].color}" stroke-width="2"${dash}/>`;
            svg += `<text x="${lx + 25}" y="${y + 4}" class="legend-label">${esc(legend[i].label)}</text>`;
        }
    }

    svg += '</svg>';
    return svg;
}

// --- ICC Plot ---
function renderICCPlot() {
    const container = document.getElementById('icc-plot');
    if (!container || !irtResult) return;

    const curves = irtResult.items.map((item, j) => {
        const pts = IRT2PL.iccCurve(item.a, item.b);
        return {
            points: pts.map(p => ({ x: p.theta, y: p.p })),
            color: ITEM_COLORS[j] || '#666',
            label: indicatorMeta[j]?.label || `Item ${j + 1}`,
        };
    });

    container.innerHTML = svgPlot({
        yLabel: 'P(X = 1 | θ)',
        curves,
        legend: curves.map(c => ({ label: c.label, color: c.color })),
    });
}

// --- Information Plot ---
function renderInfoPlot() {
    const container = document.getElementById('info-plot');
    if (!container || !irtResult) return;

    const yMaxVals = [];
    const curves = irtResult.items.map((item, j) => {
        const pts = IRT2PL.itemInfoCurve(item.a, item.b);
        yMaxVals.push(Math.max(...pts.map(p => p.info)));
        return {
            points: pts.map(p => ({ x: p.theta, y: p.info })),
            color: ITEM_COLORS[j] || '#666',
            label: indicatorMeta[j]?.label || `Item ${j + 1}`,
        };
    });

    // Test information
    const testPts = IRT2PL.testInfoCurve(irtResult.items);
    yMaxVals.push(Math.max(...testPts.map(p => p.info)));
    curves.push({
        points: testPts.map(p => ({ x: p.theta, y: p.info })),
        color: TEST_COLOR,
        dashed: true,
        strokeWidth: 2.5,
        label: 'Test information',
    });

    const yMax = Math.ceil(Math.max(...yMaxVals) * 4) / 4;

    container.innerHTML = svgPlot({
        yMin: 0, yMax,
        yLabel: 'Information',
        curves,
        legend: curves.map(c => ({ label: c.label, color: c.color, dashed: c.dashed })),
    });
}

// --- Item Parameters Table ---
function renderItemParamsTable() {
    const container = document.getElementById('item-params-table');
    if (!container || !irtResult) return;

    let html = `<table class="irt-table">
        <thead><tr>
            <th>Indicator</th><th>Dimension</th>
            <th>a (discrim.)</th><th>SE(a)</th>
            <th>b (difficulty)</th><th>SE(b)</th>
            <th>Prevalence</th>
        </tr></thead><tbody>`;

    irtResult.items.forEach((item, j) => {
        const meta = indicatorMeta[j];
        const dimLabel = meta?.dimension === 'power' ? 'Power' : 'Cult';
        const dimClass = meta?.dimension || '';
        let coded = 0, present = 0;
        for (const l of allLeaders) {
            const v = l.indicators[meta.key];
            if (v === 0 || v === 1) { coded++; if (v === 1) present++; }
        }
        const prev = coded > 0 ? (present / coded * 100).toFixed(1) : '—';
        html += `<tr>
            <td><span class="item-color-dot" style="background:${ITEM_COLORS[j]}"></span>${esc(meta?.label || '')}</td>
            <td><span class="indicator-dimension ${dimClass}">${dimLabel}</span></td>
            <td>${item.a.toFixed(3)}</td><td>${isNaN(item.se_a) ? '—' : item.se_a.toFixed(3)}</td>
            <td>${item.b.toFixed(3)}</td><td>${isNaN(item.se_b) ? '—' : item.se_b.toFixed(3)}</td>
            <td>${present}/${coded} (${prev}%)</td>
        </tr>`;
    });

    html += '</tbody></table>';
    container.innerHTML = html;
}

// --- Response Pattern Distribution ---
function renderPatternTable() {
    const container = document.getElementById('pattern-table');
    if (!container || !irtResult) return;

    const indicatorKeys = indicatorMeta.map(m => m.key);
    const patternMap = new Map();

    for (let i = 0; i < allLeaders.length; i++) {
        const pattern = indicatorKeys.map(k => allLeaders[i].indicators[k] || 0).join('');
        if (!patternMap.has(pattern)) {
            patternMap.set(pattern, { count: 0, theta: irtResult.persons[i].theta, se: irtResult.persons[i].se, values: indicatorKeys.map(k => allLeaders[i].indicators[k] || 0) });
        }
        patternMap.get(pattern).count++;
    }

    const patterns = [...patternMap.entries()].sort((a, b) => b[1].theta - a[1].theta);

    let html = `<table class="irt-table">
        <thead><tr>
            <th>Pattern</th>
            ${indicatorMeta.map(m => `<th>${abbreviate(m.label)}</th>`).join('')}
            <th>Freq</th><th>%</th><th>θ (EAP)</th><th>SE</th>
        </tr></thead><tbody>`;

    for (const [key, p] of patterns) {
        const pct = (p.count / allLeaders.length * 100).toFixed(1);
        const cells = p.values.map(v => v === 1 ? '<td><span class="cell-1">1</span></td>' : '<td><span class="cell-0">0</span></td>').join('');
        html += `<tr>
            <td class="pattern-key">${key}</td>
            ${cells}
            <td>${p.count}</td>
            <td>${pct}%</td>
            <td>${p.theta.toFixed(3)}</td>
            <td>${p.se.toFixed(3)}</td>
        </tr>`;
    }

    html += '</tbody></table>';
    container.innerHTML = html;
}

// --- Leader Ranking by Theta ---
function renderRanking(filter = '', showAll = false) {
    const container = document.getElementById('ranking-table');
    if (!container || !irtResult) return;

    let leaders = [...allLeaders].sort((a, b) => (b.theta || 0) - (a.theta || 0));

    if (filter) {
        const term = filter.toLowerCase();
        leaders = leaders.filter(l =>
            l.leader.toLowerCase().includes(term) ||
            l.country.toLowerCase().includes(term)
        );
    }

    const total = leaders.length;
    const display = showAll ? leaders : leaders.slice(0, 60);

    // Compute percentiles from full sorted list
    const thetaMin = Math.min(...allLeaders.map(l => l.theta || -3));
    const thetaMax = Math.max(...allLeaders.map(l => l.theta || -3));
    const thetaRange = thetaMax - thetaMin || 1;

    let html = `<table class="irt-table ranking">
        <thead><tr>
            <th>#</th><th>Leader</th><th>Country</th><th>Years</th>
            <th>θ</th><th>SE</th><th>Pattern</th><th class="rank-bar-header">Latent score</th>
        </tr></thead><tbody>`;

    const indicatorKeys = indicatorMeta.map(m => m.key);

    display.forEach((l, idx) => {
        const rank = idx + 1;
        const pattern = indicatorKeys.map(k => l.indicators[k] || 0).join('');
        const barPct = thetaRange > 0 ? ((l.theta - thetaMin) / thetaRange * 100).toFixed(1) : 0;
        const barColor = scoreColor((l.theta - thetaMin) / thetaRange);

        html += `<tr>
            <td>${rank}</td>
            <td>${esc(l.leader)}</td>
            <td>${esc(l.country)}</td>
            <td>${l.startYear}–${l.endYear}</td>
            <td class="mono">${(l.theta || 0).toFixed(3)}</td>
            <td class="mono">${(l.thetaSE || 0).toFixed(3)}</td>
            <td class="pattern-key">${pattern}</td>
            <td>
                <div class="rank-bar-track">
                    <div class="rank-bar-fill" style="width:${barPct}%;background:${barColor}"></div>
                </div>
            </td>
        </tr>`;
    });

    html += '</tbody></table>';
    if (!showAll && total > 60) {
        html += `<p class="ranking-note">${total - 60} additional leaders not shown. Check "Show all" to display complete ranking.</p>`;
    }
    container.innerHTML = html;
}

// ============================================================================
// CSV Export
// ============================================================================

function exportCSV() {
    const indicatorKeys = indicatorMeta.map(m => m.key);
    const headers = ['country', 'iso3', 'leader', 'start_year', 'end_year', 'score', 'theta', 'theta_se', ...indicatorKeys];

    const rows = allLeaders.map(l => {
        const base = [l.country, l.iso3, l.leader, l.startYear, l.endYear, ((l.score || 0) * 100).toFixed(1),
                      (l.theta || 0).toFixed(4), (l.thetaSE || 0).toFixed(4)];
        const vals = indicatorKeys.map(k => {
            const v = l.indicators[k];
            return v === 1 ? '1' : v === 0 ? '0' : '';
        });
        return [...base, ...vals].map(v => `"${String(v).replace(/"/g, '""')}"`).join(',');
    });

    const csv = [headers.join(','), ...rows].join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);

    const a = document.createElement('a');
    a.href = url;
    a.download = 'personalism_data.csv';
    a.click();
    URL.revokeObjectURL(url);
}

// ============================================================================
// Event listeners
// ============================================================================

function setupEventListeners() {
    // Tab switching
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.tab').forEach(t => { t.classList.remove('active'); t.setAttribute('aria-selected', 'false'); });
            document.querySelectorAll('.tab-content').forEach(tc => tc.classList.remove('active'));
            tab.classList.add('active');
            tab.setAttribute('aria-selected', 'true');
            document.getElementById(`tab-${tab.dataset.tab}`).classList.add('active');
        });
    });

    // Country search
    const countrySearch = document.getElementById('country-search');
    const sortBy = document.getElementById('sort-by');
    let searchTimeout;
    if (countrySearch) {
        countrySearch.addEventListener('input', () => {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => renderCountryGrid(countrySearch.value, sortBy.value), 200);
        });
    }
    if (sortBy) {
        sortBy.addEventListener('change', () => renderCountryGrid(countrySearch.value, sortBy.value));
    }

    // Table search
    const tableSearch = document.getElementById('table-search');
    let tableTimeout;
    if (tableSearch) {
        tableSearch.addEventListener('input', () => {
            clearTimeout(tableTimeout);
            tableTimeout = setTimeout(() => renderDataTable(tableSearch.value), 200);
        });
    }

    // CSV export
    const exportBtn = document.getElementById('export-csv');
    if (exportBtn) exportBtn.addEventListener('click', exportCSV);

    // Ranking search & toggle
    const rankSearch = document.getElementById('ranking-search');
    const rankShowAll = document.getElementById('ranking-show-all');
    let rankTimeout;
    if (rankSearch) {
        rankSearch.addEventListener('input', () => {
            clearTimeout(rankTimeout);
            rankTimeout = setTimeout(() => renderRanking(rankSearch.value, rankShowAll?.checked), 200);
        });
    }
    if (rankShowAll) {
        rankShowAll.addEventListener('change', () => renderRanking(rankSearch?.value || '', rankShowAll.checked));
    }
}

// ============================================================================
// Utilities
// ============================================================================

function esc(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function scoreColor(score, bg = false) {
    // score 0–1 → gray → yellow → red
    if (score <= 0.33) {
        const t = score / 0.33;
        return bg
            ? interpolateColor('#F3F4F6', '#FEF3C7', t)
            : interpolateColor('#9CA3AF', '#D97706', t);
    } else {
        const t = (score - 0.33) / 0.67;
        return bg
            ? interpolateColor('#FEF3C7', '#FEE2E2', t)
            : interpolateColor('#D97706', '#DC2626', t);
    }
}

function interpolateColor(c1, c2, t) {
    const r1 = parseInt(c1.slice(1, 3), 16), g1 = parseInt(c1.slice(3, 5), 16), b1 = parseInt(c1.slice(5, 7), 16);
    const r2 = parseInt(c2.slice(1, 3), 16), g2 = parseInt(c2.slice(3, 5), 16), b2 = parseInt(c2.slice(5, 7), 16);
    const r = Math.round(r1 + (r2 - r1) * t);
    const g = Math.round(g1 + (g2 - g1) * t);
    const b = Math.round(b1 + (b2 - b1) * t);
    return `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`;
}

function abbreviate(label) {
    // Short abbreviations for table headers
    const abbrevs = {
        'Currency Portrait': 'Curr',
        'Term Limits Removed': 'Term',
        'President for Life': 'PfL',
        'Loyalty Oath': 'Oath',
        'Appointment Monopoly': 'Appt',
        'Family in Government': 'Fam',
        'Places Named After': 'Name',
        'Grandiose Titles': 'Title',
    };
    return abbrevs[label] || label.slice(0, 4);
}

// ============================================================================
// Init
// ============================================================================

document.addEventListener('DOMContentLoaded', loadData);
