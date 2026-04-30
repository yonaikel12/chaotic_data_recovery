# Chaotic Data Recovery

Proyecto aislado para ingesta robusta y limpieza fail-safe de archivos caoticos generados por usuarios no tecnicos.

## Componentes
- `src/chaotic_data_recovery/data_recovery_engine.py`: motor reusable con pipeline modular y reporte de calidad.
- `notebooks/frankenstein_data_recovery_demo.ipynb`: notebook que fabrica un dataset apocaliptico y demuestra recuperacion configurable.
- `docs/research_notes.md`: investigacion aplicada de problemas reales y tooling especializado.

## Quick start
```bash
pip install -r requirements.txt
pip install -e .
```

Para desarrollo y validacion automatizada:

```bash
pip install -r requirements-dev.txt
pytest -q
```

```python
from pathlib import Path
from chaotic_data_recovery import DataRecoveryEngine, RecoveryConfig

config = RecoveryConfig(
    header_row=3,
    footers_to_skip=1,
    forced_encoding=None,
    forced_delimiter=None,
    decimal_separators=[",", "."],
    thousands_separators=[".", ",", " "],
    currency_symbols=["$", "EUR", "USD", "S/", "COP"],
)

engine = DataRecoveryEngine(config=config)
result = engine.run(Path("data/raw/archivo_caotico.xlsx"))
clean_df = result.dataframe
quality_report = result.report
```

Si el auto-detect falla o es ambiguo, se puede forzar la ingesta sin tocar el core:

```python
config = RecoveryConfig(
    header_row=2,
    footers_to_skip=1,
    forced_encoding="cp1252",
    forced_delimiter=";",
)
```

## Principios de diseno
- Fail-safe por defecto.
- Parametrizacion explicita.
- Logging silencioso pero auditable.
- Uso de librerias especializadas para cada tipo de caos.

## Validacion incluida
- El notebook `notebooks/frankenstein_data_recovery_demo.ipynb` genera un Excel con celdas combinadas y un CSV en cp1252, luego ejecuta el motor con configuraciones distintas.
- La validacion del notebook confirma reparacion de mojibake en Excel, parseo de fechas multiformato, normalizacion de montos y auditoria de filas basura descartadas.

## Suite automatizada
- `tests/test_data_recovery_engine.py` cubre Excel con celdas fusionadas, mojibake, overrides de encoding y delimitador, fechas parcialmente parseables y comportamiento fail-safe ante excepciones internas.
- Los fixtures caoticos se generan en tiempo de ejecucion para que la suite no dependa de archivos estaticos externos.
