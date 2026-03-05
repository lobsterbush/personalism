# Codebook: Manifest Indicators of Personalism in Dictatorships

## Version 0.2

## Conceptual Framework

This codebook defines observable ("manifest") indicators of the latent trait of **personalism** in authoritarian regimes. Following the critique in Morgenbesser (this paper), we adopt a **family resemblance** concept structure: no single indicator is necessary or sufficient for personalism. Instead, the accumulation of indicators reveals the degree to which a dictator has personalized political power.

We distinguish between two overlapping but conceptually separable dimensions:

1. **Power concentration indicators** — the dictator's formal and informal control over political institutions, appointments, and the security apparatus.
2. **Personality cult indicators** — the symbolic elevation of the dictator through propaganda, public imagery, and state-sponsored veneration.

Our theoretical expectation (following Morgenbesser) is that personality cults are a downstream manifestation of power concentration, but we code both dimensions to let the measurement model adjudicate their empirical relationship.

## Unit of Observation

**Country-leader-year**. Each observation corresponds to a single country in a single calendar year under a specific authoritarian leader. The dataset covers all non-democratic regimes from 1946 to the present.

Leader identification follows Archigos (Goemans et al.) for start/end dates.

---

## Indicator Definitions

### A. Power Concentration Indicators

#### A1. `term_limits_removed`
- **Definition:** The leader has amended or abolished constitutional term limits that previously constrained their tenure.
- **Coding:** 1 = term limits removed or extended beyond original provision during current leader's tenure; 0 = original term limits remain; NA = no constitutional term limits existed prior.
- **Source:** Constitute Project API; secondary validation from Comparative Constitutions Project.
- **Expected coverage:** ~85% of authoritarian regime-years post-1950.
- **Notes:** Code the *change*, not the absence. A country that never had term limits is NA, not 1.

#### A2. `president_for_life`
- **Definition:** The leader holds an official title designating them as ruler for life (e.g., "President for Life," "Eternal Leader," "Supreme Leader for Life").
- **Coding:** 1 = holds life-tenure title; 0 = does not.
- **Source:** Constitute Project (for constitutional provisions); Wikidata (for honorific titles); secondary sources.
- **Expected coverage:** ~90%.
- **Notes:** This indicator has declined in popularity since the 1970s (Kailitz & Stockemer 2017), making it a less discriminating indicator for recent decades.

#### A3. `family_in_government`
- **Definition:** The leader has appointed immediate family members (spouse, children, siblings, parents) to senior government, military, or party positions.
- **Coding:** Count of unique family members in senior positions. For binary models, threshold at ≥2.
- **Source:** Wikidata SPARQL (kinship + office-holding relations); supplemented by country expert coding.
- **Expected coverage:** ~70% (Wikidata coverage varies by country prominence).
- **Notes:** "Senior positions" = cabinet minister, military general/chief, party secretary/chairman, provincial governor, head of intelligence service. This is one of the strongest discriminators between personalism (power concentration) and personality cult alone.

#### A4. `appointment_monopoly`
- **Definition:** The leader exercises de facto unilateral control over appointments to senior military, judicial, and administrative positions, bypassing or overriding formal institutional procedures.
- **Coding:** 1 = leader controls appointments; 0 = institutional procedures constrain appointments.
- **Source:** Constitutional text (Constitute Project) for de jure; US State Department Country Reports and Freedom House narratives for de facto.
- **Expected coverage:** ~60% (requires qualitative assessment for many cases).

#### A5. `purges_of_elites`
- **Definition:** The leader has conducted purges (imprisonment, execution, forced exile, or dismissal) of senior party, military, or government officials who posed potential challenges.
- **Coding:** Count of documented purge events per leader-year. For binary, threshold at ≥1 in any given 5-year window.
- **Source:** News archives (GDELT, LexisNexis); Wikipedia entries for individual leaders; secondary literature.
- **Expected coverage:** ~50% (event data is inherently incomplete for closed regimes).

#### A6. `security_apparatus_control`
- **Definition:** The leader has established personal control over the security services (secret police, presidential guard, intelligence agencies), often creating parallel or competing agencies loyal to them personally.
- **Coding:** 1 = leader has created personal security apparatus or restructured existing one under direct personal control; 0 = security services operate under institutional authority.
- **Source:** Greitens (2016) dataset; US State Dept reports; secondary sources.
- **Expected coverage:** ~55%.

### B. Personality Cult Indicators

#### B1. `currency_portrait`
- **Definition:** A banknote currently in circulation features a portrait of the living ruler.
- **Coding:** 1 = at least one denomination features the living ruler; 0 = no denomination features them.
- **Source:** Numista banknote catalog (scrapable); supplemented by World Banknote catalog.
- **Expected coverage:** ~95% (banknote data is comprehensive).
- **Notes:** Critical to distinguish living from deceased rulers. Many countries feature founding fathers or historical leaders; this only codes *living* rulers. Monarchs on currency are common and expected — consider coding this indicator separately for monarchies vs. other regime types.

#### B2. `stamps_portrait`
- **Definition:** Postage stamps issued during the leader's tenure feature their portrait.
- **Coding:** Proportion of stamp issues in a year featuring the leader (continuous); for binary, threshold at >20% of total issues.
- **Source:** Colnect.com stamp catalog; Scott catalog.
- **Expected coverage:** ~80%.
- **Notes:** Frequency matters more than mere presence. Democracies occasionally feature living leaders; what distinguishes personalist regimes is the *proportion*.

#### B3. `places_named_after_leader`
- **Definition:** Major geographic features, cities, institutions (airports, universities, stadiums) are named or renamed after the living ruler.
- **Coding:** Count of major named features. For binary, threshold at ≥2 non-trivial namings.
- **Source:** Wikidata SPARQL ("named after" relation); OpenStreetMap; Wikipedia.
- **Expected coverage:** ~75%.
- **Notes:** Code both renamings (existing places renamed) and new namings. "Non-trivial" excludes small streets or minor buildings.

#### B4. `grandiose_titles`
- **Definition:** The leader holds or is addressed by honorary/grandiose titles beyond their official office (e.g., "Father of the Nation," "Brotherly Leader and Guide of the Revolution," "Sun of the Nation," "Brilliant Comrade").
- **Coding:** Count of distinct honorific titles. For binary, threshold at ≥1 non-standard honorific.
- **Source:** Wikidata (honorific labels); Wikipedia leader biography pages.
- **Expected coverage:** ~85%.
- **Notes:** Distinguish between common ceremonial titles shared across democracies (e.g., "Commander-in-Chief") and personalist honorifics unique to the individual.

#### B5. `national_holiday_birthday`
- **Definition:** The leader's birthday is designated as a national public holiday or day of celebration.
- **Coding:** 1 = official national holiday on leader's birthday; 0 = not.
- **Source:** Wikipedia public holidays by country; government gazettes.
- **Expected coverage:** ~90%.

#### B6. `mandatory_portraits`
- **Definition:** Portraits, photographs, or images of the leader are required or ubiquitously displayed in public buildings, classrooms, or workplaces by state mandate or strong social expectation.
- **Coding:** 1 = documented requirement or ubiquitous display; 0 = not documented.
- **Source:** US State Department Human Rights Reports; Freedom House narratives; Amnesty International reports. Text-mineable at scale.
- **Expected coverage:** ~60% (depends on report detail).

#### B7. `state_media_dominance`
- **Definition:** State-controlled media devotes a disproportionate share of coverage to the leader's activities, speeches, and image.
- **Coding:** Continuous (ratio of leader-mention paragraphs to total coverage) where data allows; binary (1 = documented state media cult, 0 = not) otherwise.
- **Source:** GDELT; BBC Monitoring; US State Dept reports.
- **Expected coverage:** ~50%.

#### B8. `monuments_and_statues`
- **Definition:** The state has commissioned large-scale monuments, statues, or public artworks depicting or honoring the living leader.
- **Coding:** Count of documented monuments; binary threshold at ≥1 large-scale monument.
- **Source:** Wikipedia; news archives; satellite imagery for large structures.
- **Expected coverage:** ~65%.

#### B9. `loyalty_oath_personal`
- **Definition:** Public officials, military personnel, or citizens are required to swear an oath of loyalty to the leader personally (as opposed to the constitution or state).
- **Coding:** 1 = oath to person documented; 0 = oath to state/constitution only.
- **Source:** Constitute Project (constitutional oath provisions); State Dept reports.
- **Expected coverage:** ~70%.

#### B10. `state_hagiography`
- **Definition:** The state publishes, commissions, or mandates official biographies, autobiographies, or ideological works attributed to or celebrating the leader.
- **Coding:** 1 = documented state-sponsored hagiographic publication; 0 = not.
- **Source:** WorldCat/library catalogs; Wikipedia bibliographies; publisher records.
- **Expected coverage:** ~60%.

---

## Coding Procedures

### Automated Coding
Indicators A1, A2, B1, B2, B3, B4, B5, and B9 can be substantially automated through API queries and web scraping. The scripts in `scripts/` implement this.

### Semi-Automated Coding (Text Mining)
Indicators A4, B6, B7, and B10 can be partially automated through text mining of US State Department reports and Freedom House narratives using keyword dictionaries.

### Manual Coding
Indicators A3 (partial), A5, A6, and B8 require manual coding from secondary sources, supplemented by Wikidata where available.

### Intercoder Reliability
All manually coded indicators should achieve Cohen's kappa ≥ 0.8 across two independent coders. Discrepancies resolved by discussion; persistent disagreements coded as "uncertain" and excluded from primary analysis.

---

## Measurement Model

The indicators feed into a **Bayesian two-parameter Item Response Theory (IRT)** model:

- Each indicator *j* has a discrimination parameter (α_j) and a difficulty parameter (β_j).
- Each country-leader-year *i* has a latent personalism score (θ_i).
- P(Y_ij = 1) = logit⁻¹(α_j(θ_i - β_j))

This approach:
- Does not require all indicators to be present (handles missingness naturally)
- Provides uncertainty estimates (credible intervals) for each score
- Allows indicators to contribute differentially (some are more informative than others)
- Can be extended to a multidimensional model separating power concentration from personality cult

Prior specification follows Treier and Jackman (2008) and V-Dem methodology.

---

## Source Priority Hierarchy

When multiple sources conflict on the coding of an indicator:
1. Country/area expert assessment (highest priority)
2. Constitutional text (for de jure indicators)
3. US State Department reports
4. Freedom House reports
5. Wikipedia/Wikidata
6. Automated scrapers (lowest priority, used as starting point)

---

## Known Limitations

1. **Survivorship bias in sources:** Prominent dictatorships (North Korea, Libya, Iraq) have abundant documentation; smaller regimes may be undercoded.
2. **Temporal precision:** Some indicators (e.g., currency portraits) change infrequently and may not capture the exact year of onset.
3. **Monarchies:** Several indicators (currency portraits, birthday holidays) are standard practice in monarchies regardless of personalism level. The measurement model should include a monarchy indicator as a covariate or code monarchies separately.
4. **Personality cult without personalism:** A few cases may exhibit cult indicators without genuine power concentration (e.g., regimes that maintain cult imagery from a predecessor). The two-dimensional model helps address this.

---

## Variable Summary

The following 16 indicators are currently coded and included in the compiled dataset (`data/compiled/personalism_full.csv`). The IRT model retains a subset of these based on positive discrimination parameters.

**Collected (16 indicators):**

- `term_limits_absent` — Constitute Project (script 11)
- `president_for_life` — Constitute + Wikipedia categories (scripts 10, 11)
- `family_in_govt` — Wikidata SPARQL (scripts 03, 06, 14)
- `political_killings` — V-Dem v2clkill (script 08)
- `military_executive` — V-Dem v2x_ex_military (script 08)
- `judicial_purges` — V-Dem v2jupurge (script 08)
- `const_disregard` — V-Dem v2exrescon (script 08)
- `no_leg_constraint` — V-Dem v2xlg_legcon (script 08)
- `places_named` — Wikidata P138 "named after" (scripts 03, 14)
- `grandiose_titles` — Wikidata honorific labels (scripts 03, 14)
- `monuments` — Wikidata P180/P547 (script 09)
- `birthday_holiday` — Wikidata P547 (script 09)
- `hagiography` — Wikidata P50 (script 09)
- `cult_of_personality` — Wikipedia categories (script 10)
- `currency_portrait` — Wikipedia currency articles (script 12)
- `oath_to_person` — Constitute Project (script 11)

**Planned but not yet coded:**

- `appointment_monopoly` — Requires qualitative text mining
- `stamps_portrait` — Requires Colnect scraper
- `mandatory_portraits` — Requires State Dept report text mining
- `state_media_dominance` — Requires GDELT/BBC Monitoring
