# Investigacion aplicada a data caotica

## Problemas reales observados
- Mojibake y decodificacion rota: secuencias como `Ã¡`, `â‚¬`, HTML entities fuera de HTML y mezcla cp1252/utf-8.
- Dialectos CSV ambiguos: delimitadores `;`, `,`, `|`, tabulaciones, comillas inconsistentes y filas con longitudes irregulares.
- Reportes no tabulares: titulos, notas legales, subtotales, separadores visuales y multiples pseudo-tablas en un mismo archivo.
- Excel con celdas combinadas: encabezados verticales u horizontales fusionados que rompen la lectura tabular directa.
- Fechas multilingues y mixtas: `01/02/24`, `15 ene 2025`, `March 7, 2024`, `hace 2 dias`.
- Numericos sucios: separadores miles y decimales mezclados, monedas incrustadas, negativos con parentesis y porcentajes.

## Librerias especializadas seleccionadas
- `clevercsv`: deteccion de dialecto CSV basada en patrones de filas y tipos. Mas robusta que `csv.Sniffer` ante archivos no estandar.
- `charset-normalizer`: inspeccion de bytes para inferir encoding sin confiar en supuestos del sistema.
- `ftfy`: reparacion heuristica de mojibake y entidades HTML mal decodificadas, minimizando falsos positivos.
- `dateparser`: parseo de fechas humanas y multilingues con configuracion de orden de fecha y locale.
- `Unidecode`: transliteracion a ASCII para normalizar nombres tecnicos de columnas y llaves operativas.
- `openpyxl`: lectura de Excel y manejo de rangos fusionados para propagar valores al hacer unmerge logico.

## Enfoque industrial
- Cada etapa del pipeline es fail-safe: captura excepciones, registra el evento y sigue procesando.
- Los errores de limpieza se traducen a `NaN` o texto original preservado segun el tipo de transformacion.
- El reporte de calidad captura librerias disparadas, causas, filas basura descartadas y tasa de exito por columna.
- La configuracion se expone en un `RecoveryConfig` tipado para que cualquier analista pueda adaptar la limpieza sin tocar el core.
