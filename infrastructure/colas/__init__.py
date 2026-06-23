# infrastructure/colas/__init__.py
"""Adapters de SALIDA de sv4 hacia las colas del sistema.

Sustituyen a los antiguos clientes HTTP (``HttpOrchestratorClient`` →
sv7 y ``Sv3RefetchClient`` → sv3). Implementan los mismos puertos del
dominio (``OrchestratorClient`` y ``ContratoRefetchClient``), de modo
que el wiring y los endpoints del portal no cambian: solo se swap-ea el
adapter (arquitectura hexagonal).
"""
