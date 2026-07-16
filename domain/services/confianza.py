# domain/services/confianza.py
"""Cálculo determinista de la CONFIANZA de valoración de un albarán.

Reemplaza la confianza que pedíamos a la IA por una métrica basada en
"pasos completados adecuadamente": cómo de bien quedó resuelta la
cabecera (obra + proveedor) y cuántas líneas casaron con contrato, con
código de imputación y unitario correctos.

Escala 0..100, dos bloques (pesos acordados):

  - Cabecera (30): Obra (15) + Proveedor/CIF (15). Por campo:
      manual / ia        -> 100 % del peso  (dato fiable / verificado)
      deterministic      ->  60 % del peso  (match por texto, umbral 0.5)
      det_familia_obra   ->  50 % del peso  (CIF deducido por familia de
                             producto entre los proveedores de la obra)
      ausente (NULL/"")  ->   0 %

  - Líneas (70): media SIMPLE del score por línea base (from_albaran),
    cada uno 0..1:
      score = 0.50 * casado + 0.25 * partida + 0.25 * unitario

      casado:
        asignada a contrato (manual / IA conf > 50)      -> 1.00
        asignada por IA semántica/familia (conf <= 50)   -> 0.70
        "nueva" creada manualmente por el revisor        -> 0.70
        "nueva" automática (la IA no encontró nada)      -> 0.30
        sin salmón (en blanco)                           -> 0.00
      partida: código de imputación presente -> 1.00, ausente -> 0.00
      unitario:
        asignada y coincide con contrato   -> 1.00
        asignada pero difiere              -> 0.50
        nueva con precio                   -> 0.70
        sin precio                         -> 0.00

La función es pura: recibe los datos de cabecera y una lista de objetos
de conciliación (uno por línea base; ``None`` si la línea quedó en
blanco) y devuelve el porcentaje. Se llama al construir el detalle y
tras cada operación que cambia el estado, por lo que la confianza se
actualiza dinámicamente según se rellena y guarda la valoración.
"""

from __future__ import annotations

from typing import Any, Sequence

# Pesos de bloque.
PESO_CABECERA = 30.0
PESO_LINEAS = 70.0

# Factor para cabecera resuelta por texto determinista (vs ia/manual).
FACTOR_DETERMINISTIC = 0.6
# Factor cuando el CIF se dedujo por FAMILIA de producto entre los
# proveedores con contrato en la obra (jul 2026, origen
# 'det_familia_obra' del HeaderResolver de sv3): senal razonada pero el
# nombre leido NO supero el umbral -> menos fiable que 'deterministic'.
FACTOR_DET_FAMILIA_OBRA = 0.5

# Sub-pesos del score por línea.
W_CASADO = 0.50
W_PARTIDA = 0.25
W_UNITARIO = 0.25

# Umbral de confianza de la IA por debajo del cual un match asignado se
# considera "semántico/familia" (match débil) — coincide con la banda que
# usa el prompt de sv5 para el match por familia.
UMBRAL_MATCH_DEBIL = 50.0


def _is_blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _header_field_pts(value: Any, origen: Any, full_pts: float) -> float:
    """Puntos de un campo de cabecera (obra o proveedor)."""
    if _is_blank(value):
        return 0.0
    o = (origen or "").strip().lower() if isinstance(origen, str) else ""
    if o == "deterministic":
        return full_pts * FACTOR_DETERMINISTIC
    if o == "det_familia_obra":
        return full_pts * FACTOR_DET_FAMILIA_OBRA
    # 'ia', 'manual' o presente sin marca -> dato fiable.
    return full_pts


def _es_manual(c: Any) -> bool:
    mm = (getattr(c, "match_method", None) or "").strip().lower()
    src = (getattr(c, "precio_unitario_source", None) or "").strip().lower()
    return mm == "manual" or src.startswith("manual")


def _casado_factor(c: Any) -> float:
    if c is None:
        return 0.0
    kind = getattr(c, "kind", None)
    if kind == "assigned":
        if _es_manual(c):
            return 1.0
        conf = getattr(c, "match_confidence_pct", None)
        if conf is not None and conf <= UMBRAL_MATCH_DEBIL:
            return 0.70
        return 1.0
    if kind == "derived":
        origen = (getattr(c, "derived_origen", None) or "").strip().lower()
        if origen == "manual_override":
            return 0.70
        # 'nueva_no_match' (la IA no casó) y otras derivadas automáticas
        # (missing_partida / alm_acopio): resueltas pero sin contrato.
        return 0.30
    return 0.0


def _partida_factor(c: Any) -> float:
    if c is None:
        return 0.0
    return 0.0 if _is_blank(getattr(c, "codigo_partida", None)) else 1.0


def _unitario_factor(c: Any) -> float:
    if c is None:
        return 0.0
    tiene_precio = getattr(c, "precio_unitario_final", None) is not None
    if getattr(c, "kind", None) == "assigned":
        agree = getattr(c, "agree_unitario", None)
        price_agreement = getattr(c, "price_agreement", None)
        if agree is True or price_agreement == "agree":
            return 1.0
        if agree is False or price_agreement == "disagree":
            return 0.50
        # Asignada sin referencia comparable: si hay precio, vale.
        return 1.0 if tiene_precio else 0.0
    # Derivada / nueva: sin contrato con qué validar.
    return 0.70 if tiene_precio else 0.0


def _line_score(c: Any) -> float:
    return (
        W_CASADO * _casado_factor(c)
        + W_PARTIDA * _partida_factor(c)
        + W_UNITARIO * _unitario_factor(c)
    )


# Penalizacion cuando el CONTRATO fue elegido automaticamente entre
# VARIOS candidatos (selector deterministico de sv3, origen
# 'auto_multiple'). La eleccion es razonada (familia/palabras) pero no
# esta verificada por un humano -> el documento entero merece revision.
PENALIZACION_CONTRATO_AUTO_MULTIPLE = 15.0


def compute_confianza_pct(
    *,
    obra_codigo: Any,
    obra_codigo_origen: Any,
    proveedor_cif: Any,
    proveedor_cif_origen: Any,
    conciliaciones: Sequence[Any],
    selected_contrato_origen: Any = None,
) -> float | None:
    """Confianza 0..100. ``None`` si no hay líneas (sin valoración útil).

    ``conciliaciones``: una entrada por línea BASE del albarán, con la
    ``ConciliacionDisplay`` correspondiente o ``None`` si quedó en blanco.
    """
    if not conciliaciones:
        return None

    cabecera = (
        _header_field_pts(obra_codigo, obra_codigo_origen, 15.0)
        + _header_field_pts(proveedor_cif, proveedor_cif_origen, 15.0)
    )

    scores = [_line_score(c) for c in conciliaciones]
    lineas = (sum(scores) / len(scores)) * PESO_LINEAS

    total = cabecera + lineas
    if str(selected_contrato_origen or "").strip().lower() == (
        "auto_multiple"
    ):
        total -= PENALIZACION_CONTRATO_AUTO_MULTIPLE

    return round(max(total, 0.0), 1)
