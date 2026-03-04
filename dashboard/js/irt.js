/**
 * 2PL IRT Model — Marginal Maximum Likelihood via EM (Bock–Aitkin)
 *
 * Estimates item parameters (discrimination a, difficulty b) for binary
 * response data, plus EAP person parameter estimates (theta).
 *
 * Designed for the personalism dashboard: small J (3 items), moderate N (~600).
 */

// ============================================================================
// Utilities
// ============================================================================

function normalPDF(x) {
    return Math.exp(-0.5 * x * x) / Math.sqrt(2 * Math.PI);
}

// ============================================================================
// IRT2PL class
// ============================================================================

class IRT2PL {
    /**
     * @param {Object} options
     * @param {number} options.nQuad  – quadrature points (default 41)
     * @param {number} options.maxIter – max EM iterations (default 300)
     * @param {number} options.tol – convergence tolerance on ΔlogLik (default 0.0005)
     */
    constructor(options = {}) {
        this.nQuad   = options.nQuad   || 41;
        this.maxIter = options.maxIter || 300;
        this.tol     = options.tol     || 0.0005;
        this.bounds  = { aMin: 0.15, aMax: 5.0, bMin: -5.0, bMax: 5.0 };
        this._setupQuadrature();
    }

    _setupQuadrature() {
        const K   = this.nQuad;
        const lo  = -4, hi = 4;
        const step = (hi - lo) / (K - 1);
        this.quadTheta   = new Float64Array(K);
        this.quadWeights = new Float64Array(K);
        for (let k = 0; k < K; k++) {
            const t = lo + k * step;
            this.quadTheta[k]   = t;
            this.quadWeights[k] = normalPDF(t) * step;
        }
    }

    // --- Probability (ICC) ---------------------------------------------------

    static prob(theta, a, b) {
        const z = a * (theta - b);
        if (z >  35) return 1 - 1e-15;
        if (z < -35) return 1e-15;
        return 1.0 / (1.0 + Math.exp(-z));
    }

    // --- Fit model ------------------------------------------------------------

    /**
     * @param {number[][]} data – N × J matrix, values 0 | 1 | null
     * @returns {IRTResult}
     */
    fit(data) {
        const N = data.length;
        const J = data[0].length;
        const K = this.nQuad;
        const qt = this.quadTheta;
        const qw = this.quadWeights;

        // --- Initialise item parameters ---
        const a = new Float64Array(J);
        const b = new Float64Array(J);
        for (let j = 0; j < J; j++) {
            let s = 0, n = 0;
            for (let i = 0; i < N; i++) {
                if (data[i][j] != null) { s += data[i][j]; n++; }
            }
            const p = n > 0 ? Math.max(0.02, Math.min(0.98, s / n)) : 0.5;
            a[j] = 1.0;
            b[j] = -Math.log(p / (1 - p));
        }

        let logLik = -Infinity;
        let iter   = 0;
        let lastF  = new Float64Array(K);

        // --- EM loop ---
        for (iter = 0; iter < this.maxIter; iter++) {
            // E-step: expected sufficient statistics
            const r = Array.from({ length: J }, () => new Float64Array(K));
            const fk = new Float64Array(K);  // expected count at quad point
            let newLL = 0;

            for (let i = 0; i < N; i++) {
                // Likelihood at each quad point
                const post = new Float64Array(K);
                for (let k = 0; k < K; k++) {
                    let ll = 0;
                    for (let j = 0; j < J; j++) {
                        if (data[i][j] == null) continue;
                        const p = IRT2PL.prob(qt[k], a[j], b[j]);
                        ll += data[i][j] * Math.log(p) + (1 - data[i][j]) * Math.log(1 - p);
                    }
                    post[k] = Math.exp(ll) * qw[k];
                }
                let marg = 0;
                for (let k = 0; k < K; k++) marg += post[k];
                newLL += Math.log(marg + 1e-300);

                // Accumulate expected statistics
                for (let k = 0; k < K; k++) {
                    const w = post[k] / (marg + 1e-300);
                    fk[k] += w;
                    for (let j = 0; j < J; j++) {
                        if (data[i][j] != null) r[j][k] += w * data[i][j];
                    }
                }
            }

            lastF = fk;

            // Convergence check (log-likelihood)
            if (iter > 0 && Math.abs(newLL - logLik) < this.tol) {
                logLik = newLL;
                iter++;
                break;
            }
            logLik = newLL;

            // M-step: Newton–Raphson per item
            for (let j = 0; j < J; j++) {
                for (let nr = 0; nr < 15; nr++) {
                    let ga = 0, gb = 0;
                    let Haa = 0, Hbb = 0, Hab = 0;

                    for (let k = 0; k < K; k++) {
                        const p  = IRT2PL.prob(qt[k], a[j], b[j]);
                        const q  = 1 - p;
                        const res = r[j][k] - fk[k] * p;
                        const td  = qt[k] - b[j];

                        ga  += res * td;
                        gb  -= res * a[j];
                        Haa -= fk[k] * p * q * td * td;
                        Hbb -= fk[k] * p * q * a[j] * a[j];
                        Hab += fk[k] * p * q * a[j] * td;
                    }

                    const det = Haa * Hbb - Hab * Hab;
                    if (Math.abs(det) < 1e-14) break;

                    let da = -(Hbb * ga - Hab * gb) / det;
                    let db = -(-Hab * ga + Haa * gb) / det;

                    // Damped step
                    const norm = Math.sqrt(da * da + db * db);
                    if (norm > 1.0) { da /= norm; db /= norm; }

                    a[j] = Math.max(this.bounds.aMin, Math.min(this.bounds.aMax, a[j] + da));
                    b[j] = Math.max(this.bounds.bMin, Math.min(this.bounds.bMax, b[j] + db));

                    if (Math.abs(da) < 1e-4 && Math.abs(db) < 1e-4) break;
                }
            }
        }

        // --- Person parameters (EAP) ---
        const persons = [];
        for (let i = 0; i < N; i++) {
            const post = new Float64Array(K);
            for (let k = 0; k < K; k++) {
                let ll = 0;
                for (let j = 0; j < J; j++) {
                    if (data[i][j] == null) continue;
                    const p = IRT2PL.prob(qt[k], a[j], b[j]);
                    ll += data[i][j] * Math.log(p) + (1 - data[i][j]) * Math.log(1 - p);
                }
                post[k] = Math.exp(ll) * qw[k];
            }
            let marg = 0;
            for (let k = 0; k < K; k++) marg += post[k];

            let eap = 0, eap2 = 0;
            for (let k = 0; k < K; k++) {
                const w = post[k] / (marg + 1e-300);
                eap  += w * qt[k];
                eap2 += w * qt[k] * qt[k];
            }
            persons.push({
                theta: eap,
                se:    Math.sqrt(Math.max(0, eap2 - eap * eap)),
            });
        }

        // --- Item standard errors (from observed information) ---
        const items = [];
        for (let j = 0; j < J; j++) {
            let Haa = 0, Hbb = 0, Hab = 0;
            for (let k = 0; k < K; k++) {
                const p  = IRT2PL.prob(qt[k], a[j], b[j]);
                const q  = 1 - p;
                const td = qt[k] - b[j];
                Haa -= lastF[k] * p * q * td * td;
                Hbb -= lastF[k] * p * q * a[j] * a[j];
                Hab += lastF[k] * p * q * a[j] * td;
            }
            const det = Haa * Hbb - Hab * Hab;
            const se_a = Math.abs(det) > 1e-12 ? Math.sqrt(Math.abs(Hbb / det)) : NaN;
            const se_b = Math.abs(det) > 1e-12 ? Math.sqrt(Math.abs(Haa / det)) : NaN;
            items.push({ a: a[j], b: b[j], se_a, se_b });
        }

        // --- Marginal reliability ---
        const thetaVals = persons.map(p => p.theta);
        const meanTheta = thetaVals.reduce((s, v) => s + v, 0) / N;
        const varTheta  = thetaVals.reduce((s, v) => s + (v - meanTheta) ** 2, 0) / N;
        const meanPSD2  = persons.reduce((s, p) => s + p.se * p.se, 0) / N;
        const reliability = varTheta > 0 ? 1 - meanPSD2 / varTheta : 0;

        // --- Model fit ---
        const nParams = 2 * J;
        return {
            items,
            persons,
            fit: {
                logLik,
                nParams,
                nObs:       N,
                nItems:     J,
                aic:        -2 * logLik + 2 * nParams,
                bic:        -2 * logLik + Math.log(N) * nParams,
                iterations: iter,
                reliability,
            },
        };
    }

    // --- Curve generators for plotting ---------------------------------------

    /** ICC data points for one item */
    static iccCurve(a, b, lo = -4, hi = 4, nPts = 201) {
        const pts = [];
        const step = (hi - lo) / (nPts - 1);
        for (let i = 0; i < nPts; i++) {
            const t = lo + i * step;
            pts.push({ theta: t, p: IRT2PL.prob(t, a, b) });
        }
        return pts;
    }

    /** Item information at a single theta */
    static itemInfo(a, b, theta) {
        const p = IRT2PL.prob(theta, a, b);
        return a * a * p * (1 - p);
    }

    /** Item information curve */
    static itemInfoCurve(a, b, lo = -4, hi = 4, nPts = 201) {
        const pts = [];
        const step = (hi - lo) / (nPts - 1);
        for (let i = 0; i < nPts; i++) {
            const t = lo + i * step;
            pts.push({ theta: t, info: IRT2PL.itemInfo(a, b, t) });
        }
        return pts;
    }

    /** Test information curve (sum of item information) */
    static testInfoCurve(items, lo = -4, hi = 4, nPts = 201) {
        const pts = [];
        const step = (hi - lo) / (nPts - 1);
        for (let i = 0; i < nPts; i++) {
            const t = lo + i * step;
            let info = 0;
            for (const item of items) info += IRT2PL.itemInfo(item.a, item.b, t);
            pts.push({ theta: t, info, se: 1 / Math.sqrt(Math.max(info, 1e-10)) });
        }
        return pts;
    }
}
