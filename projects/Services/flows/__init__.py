"""
============================================================================
Flows module for Services
============================================================================
Note: Les imports sont intentionnellement retirés pour éviter les imports
circulaires. Importer directement depuis les modules spécifiques :

    from flows.currency_rates import load_currency_codes_flow
    from flows.time_dimension import build_time_dimension_flow
============================================================================
"""

# ❌ NE PAS faire ça (cause import circulaire) :
# from .currency_rates import load_currency_codes_flow, load_exchange_rates_flow
# from .time_dimension import build_time_dimension_flow

# ✅ À la place, laisser vide et importer directement dans le code utilisateur

__all__ = []  # Liste vide - imports explicites requis