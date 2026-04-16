"""Parsers package for Portco Alpha (Journey Hospitality).

Public API:
    from parsers.alpha_parser import parse_alpha_ma

Internal modules:
    common.py       -> shared helpers
    schema.py       -> canonical KPI catalog
    era1_parser.py  -> Nov 2024 - Oct 2025 legacy format
    era2_parser.py  -> Nov 2025 - Dec 2025 transition format
    era3_parser.py  -> Jan 2026+ canonical format (Feb 2026 reference)
    router.py       -> era detection + dispatch
"""
