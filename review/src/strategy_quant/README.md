# A-share Strategy Quant

This directory contains an executable strategy engine built from the merged logic of several A-share short-line WeChat authors.

## Data sources

All paths are centralized in `config.py` (project root). The main data sources are:

- Daily bars: `ashares-k-lines` (`KLINES_DAILY_FILE`)
- Intraday ticks: `ashares-ticks` (`TICKS_DATA_DIR`)
- Theme knowledge: `stock-vector-knowledge` (`VECTOR_PROJECT`)

## Strategies implemented

- `trend_revaluation`
- `new_mainline_breakout`
- `main_rise_resonance`
- `mainline_low_absorption`
- `ice_repair`
- `catchup_rotation`
- `fast_rotation_scalp`
- `mispriced_recovery`

## Files

- `loaders.py`: read daily, ticks, and theme knowledge
- `features.py`: market state, stock strength, intraday, and theme context
- `strategies.py`: strategy cards and concrete signal engine
- `cli.py`: minimal CLI

## Usage

```bash
python -m strategy_quant.cli --date 2026-03-06 --top-n 10
python -m strategy_quant.cli --date 2026-03-06 --top-n 10 --output result.json
```

## Notes

- Theme support is derived from local merged concept knowledge, not from an online API.
- Intraday features are optional. If a date has no tick file, the engine still runs on daily bars.
- Signals are candidate generators, not an automated execution system.
