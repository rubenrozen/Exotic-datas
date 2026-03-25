# 🌍 Earth Pulse v2

> L'autre côté du miroir — 34 signaux, 9 univers visuels distincts

## Mise en ligne GitHub Pages

```bash
git init && git add . && git commit -m "init"
git remote add origin https://github.com/TON_USER/earthpulse.git
git push -u origin main
# Settings → Pages → Source: main / root
```

## Données live (aucune config requise)

| Source | Catégorie | CORS |
|--------|-----------|------|
| USGS Earthquakes | Catastrophes | ✅ natif |
| NOAA Kp + Solar Wind | From Above | ✅ natif |
| wheretheiss.at (ISS) | From Above | ✅ natif |
| NOAA CO₂ Mauna Loa | Météo | ✅ CSV |
| OpenAQ (PM2.5) | Météo | ✅ natif |
| USGS River Flow | Météo | ✅ natif |
| OpenSky Network | Trade Flows | ✅ natif |
| Wikimedia Pageviews | Trends | ✅ natif |
| arXiv API | Trends | ✅ Atom/XML |

## GitHub Secrets requis (optionnels mais recommandés)

| Secret | Source | Gratuit |
|--------|--------|---------|
| `EIA_API_KEY` | eia.gov | ✅ |
| `AGSI_KEY` | agsi.gie.eu | ✅ |
| `ACLED_KEY` + `ACLED_EMAIL` | acleddata.com | ✅ |

## Architecture

```
earthpulse/
├── index.html          # Dashboard complet — design distinct par onglet
├── fetch_data.py       # Collecteur Python (GitHub Actions)
├── data/               # JSON populés automatiquement
└── .github/workflows/update_data.yml  # Cron 6h UTC
```

## Score EPI — Calcul

`EPI = Σ(poids × score_catégorie) / Σpoids`

Chaque score catégorie = percentile historique normalisé 0–100 (100 = calme)

Pondérations : Conflits ×1.4 · Catastrophes ×1.3 · Trade ×1.1 · Météo/Énergie ×1.0 · Internet/Above ×0.9 · Trends/Démo ×0.8
