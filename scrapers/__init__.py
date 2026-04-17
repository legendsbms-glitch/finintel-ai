"""
FinIntel AI Scrapers
================
Data collection modules for Phase 2
"""
from .base import BaseScraper
from .fred import FredScraper
from .openinsider import OpenInsiderScraper
from .barchart import BarchartScraper
from .dataroma import DataromaScraper
from .kitco import KitcoScraper, GoldSilverRatioScraper
from .googlenews import GoogleNewsScraper
from .ipoico import IPOscraper, ICOscraper

__all__ = [
    "BaseScraper",
    "FredScraper",
    "OpenInsiderScraper", 
    "BarchartScraper",
    "DataromaScraper",
    "KitcoScraper",
    "GoldSilverRatioScraper",
    "GoogleNewsScraper",
    "IPOscraper",
    "ICOscraper",
]