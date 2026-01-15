class ScraperError(Exception):
    """Base exception for all scraper-related errors."""
    pass

class ProxyError(ScraperError):
    """Raised when a proxy/session is blocked, expired, or throttled."""
    pass

class RateLimitError(ProxyError):
    """429 Too Many Requests."""
    pass
