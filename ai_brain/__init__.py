"""
FinIntel AI Brain
================
AI processing modules
"""
from .client import GroqClient
from .cross_signal import CrossSignalEngine, WatchlistManager, NotificationManager

__all__ = [
    "GroqClient",
    "CrossSignalEngine", 
    "WatchlistManager",
    "NotificationManager",
]