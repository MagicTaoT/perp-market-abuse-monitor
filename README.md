# Perp Market Abuse Monitor

Read-only research tool for unusual structure in USDT perpetual markets. It scans public KuCoin, Binance, and Bybit market data, compares price and open interest within the same contract, and shows funding, supply, and known token unlocks. The dashboard separates early accumulation, elevated-price short watch, long holding risk, and short holding risk.

These are **research signals, not proof of market manipulation or trading advice**. A score ranks evidence for further review; it does not identify a trader or predict a return. Missing history, ambiguous token identities, and uncovered unlock schedules remain unknown rather than becoming zero-risk evidence. [Scoring rules](docs/SCORING.md) document the bands, source conventions, and candidate gates.

## Run locally

Use Python 3.11 or newer:

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -e ".[test]"
cp config.example.json config.json
perp-market-abuse-monitor --config config.json --once
perp-market-abuse-monitor --config config.json --serve
```

With `--serve` running, open `http://127.0.0.1:8770/`. The example stores observations under `./data`, relative to the current working directory. The dashboard listens only on local loopback. The broad scan runs every 15 minutes, the pinned IRYS detail feed every five minutes, and supply/unlock enrichment is scheduled daily with later retries after failures. The one-hour comparison needs about one hour of locally collected history; seven-day funding and 21-day price/OI ranks take longer to build.

Run the tests with `python -m pytest -q`.

## Public data and limits

Market observations are requested from public KuCoin, Binance, and Bybit endpoints. When the endpoint responds and identity checks pass, [CoinMarketCap's keyless public API](https://coinmarketcap.com/api/documentation/pro-api-reference/keyless-public-api) can enrich a symbol with circulating supply, market cap, and FDV. The monitor also attempts to match events from [PatternPlus's limited unlock calendar](https://pattern.plus/developers); an absent event does **not** mean no unlock. The dashboard links `Data: PatternPlus` wherever it displays its events. Provider coverage, rate limits, or temporary failures can leave any of these inputs unavailable.

No API key, wallet, exchange account, or order permission is required. The application contains no order route. Keep local `config.json`, `data/`, raw API responses, and any credentials out of Git; the included `.gitignore` excludes those paths.
