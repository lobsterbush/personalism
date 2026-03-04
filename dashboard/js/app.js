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

const POWER_INDICATORS = ['term_limits_removed', 'president_for_life', 'appointment_monopoly', 'family_in_govt', 'family_in_govt_binary'];
const CULT_INDICATORS = ['currency_portrait', 'oath_to_person', 'places_named', 'places_named_binary', 'grandiose_titles', 'grandiose_titles_binary'];

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
    renderBarChart();
    renderPrevalenceChart();
    renderScatterChart();
    renderCountryGrid();
    renderIndicatorGrid();
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
// CSV Export
// ============================================================================

function exportCSV() {
    const indicatorKeys = indicatorMeta.map(m => m.key);
    const headers = ['country', 'iso3', 'leader', 'start_year', 'end_year', 'score', ...indicatorKeys];

    const rows = allLeaders.map(l => {
        const base = [l.country, l.iso3, l.leader, l.startYear, l.endYear, ((l.score || 0) * 100).toFixed(1)];
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
