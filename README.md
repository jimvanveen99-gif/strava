# Singelloop Coach (10 km)

Dit project haalt je Strava-runs van de afgelopen week op en mailt elke zondagavond (NL tijd) een korte analyse + schema voor de komende week.

## Benodigd (GitHub Secrets)

Zet in je GitHub repo onder **Settings → Secrets and variables → Actions**:

- `STRAVA_CLIENT_ID`
- `STRAVA_CLIENT_SECRET`
- `STRAVA_REFRESH_TOKEN`
- `GMAIL_USER` (bijv. `jimvanveen99@gmail.com`)
- `GMAIL_APP_PASSWORD` (Google App-wachtwoord)
- `MAIL_TO` (bijv. `jimvanveen@me.com`)
- (optioneel) `OPENAI_API_KEY`
- (optioneel) `OPENAI_MODEL` (default: `gpt-4o-mini`)

## Draaien

De workflow draait automatisch op zondag (UTC) en verstuurt alleen wanneer het in **Europe/Amsterdam** zondag **22:00** is.

Je kunt ook handmatig starten via **Actions → Weekly Singelloop Coach → Run workflow**.

