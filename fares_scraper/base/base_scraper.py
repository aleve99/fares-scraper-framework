import logging
import asyncio
import aiohttp
import re

from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any, Iterable, Tuple
from datetime import date, time

from .session_manager import SessionManager
from .config import settings, ScraperSettings
from .exceptions import ScraperError, ProxyError, RateLimitError
from .types import Airport, OneWayFare, RoundTripFare, ConcurrentResults
from ..utils.timer import Timer

logger = logging.getLogger("scraper.base")


class BaseScraper(ABC):
    """
    The core of the framework. Defines the interface and provides high-level 
    utilities for concurrent requests, retries, and error handling.
    """
    def __init__(
        self,
        config: ScraperSettings = settings,
        base_url: Optional[str] = None,
        warm_up_url: Optional[str] = None,
        default_headers: Optional[Dict[str, str]] = None
    ):
        self.config = config
        self._semaphore = asyncio.Semaphore(config.pool_size)
        self.active_airports: Tuple[Airport, ...] = tuple()
        
        # Merge provided headers with config default
        headers = {
            "User-Agent": config.user_agent,
            "Accept": "application/json",
            **(default_headers or {})
        }
        
        self.sm = SessionManager(
            timeout=config.timeout,
            base_url=base_url,
            warm_up_url=warm_up_url,
            default_headers=headers,
            proxies=config.proxies
        )

    @staticmethod
    def parse_flight_number(flight_num_raw: str, carrier_code: str = "") -> int:
        """
        Extracts the numeric part of a flight number, stripping the carrier code
        if it's present at the beginning of the string.
        
        Args:
            flight_num_raw: The raw flight number string (e.g., "FR1234", "W6 1234").
            carrier_code: Optional carrier code to strip (e.g., "FR", "W6").
            
        Returns:
            The flight number as an integer. Returns 0 if no digits are found.
        """
        if not flight_num_raw:
            return 0
            
        # 1. Clean spaces and make uppercase for robust comparison
        cleaned = flight_num_raw.replace(' ', '').upper()
        carrier_code = carrier_code.replace(' ', '').upper()
        
        # 2. Strip carrier code from the start if it matches
        if carrier_code and cleaned.startswith(carrier_code):
            cleaned = cleaned[len(carrier_code):]
            
        # 3. Extract all remaining digits
        digits = re.sub(r'\D', '', cleaned)
        return int(digits) if digits else 0

    # --- Frozens Spots ---
    def get_airport(self, iata_code: str) -> Optional[Airport]:
        # Use the getter which includes the initialization check
        airports = self.get_active_airports() 
        for airport in airports:
            if airport.iata_code == iata_code:
                return airport
        
        return None

    def get_active_airports(self) -> Tuple[Airport, ...]:
        """Returns the cached tuple of active airports.

        Raises:
            RuntimeError: If the client has not been initialized (e.g., used outside an `async with` block).
        """
        if self.active_airports is None:
            raise RuntimeError("Scraper not initialized. Use within an 'async with' block.")
        return self.active_airports
    
    # --- Abstract Methods (The "Hot Spots") ---

    @abstractmethod
    async def update_active_airports(self) -> None:
        """Fetch and cache active airports for this airline."""
        pass

    @abstractmethod
    async def get_destination_codes(self, origin: str) -> Tuple[str, ...]:
        """Fetch available destinations from a given origin."""
        pass

    @abstractmethod
    async def get_available_dates(self, origin: str, destination: str) -> Tuple[str, ...]:
        """Fetch available flight dates for a given route."""
        pass

    @abstractmethod
    async def search_one_way_fares(
        self,
        origin: str,
        from_date: date,
        to_date: Optional[date] = None,
        destinations: Iterable[str] = []
    ) -> List[OneWayFare]:
        """Search for one-way fares across routes and dates."""
        pass

    @abstractmethod
    async def search_round_trip_fares(
        self,
        origin: str,
        min_days: int,
        max_days: int,
        from_date: date,
        to_date: Optional[date] = None,
        destinations: Iterable[str] = []
    ) -> List[RoundTripFare]:
        """Search for round-trip fares across routes and dates."""
        pass

    # --- Core Infrastructure Methods ---

    @staticmethod
    def _sanitize_params(params: Optional[Dict[str, Any]]) -> Optional[Dict[str, str]]:
        """
        Converts all parameter values to strings suitable for aiohttp.
        """
        if not params:
            return None
        
        sanitized = {}
        for k, v in params.items():
            if v is None: continue # Skip None values
            if isinstance(v, date):
                sanitized[k] = v.isoformat()
            elif isinstance(v, bool):
                sanitized[k] = str(v).lower() # Convert bool to "true"/"false"
            elif isinstance(v, list):
                # Convert list items to string and join with comma
                sanitized[k] = ",".join(map(str, v))
            elif isinstance(v, time):
                sanitized[k] = v.strftime('%H:%M') # Format time as HH:MM
            else:
                # Assume other types can be directly converted to string
                sanitized[k] = str(v)
        return sanitized

    async def request(
        self, 
        method: str, 
        url: str, 
        params: Optional[Dict] = None, 
        stateless: bool = False,
        **kwargs
    ) -> aiohttp.ClientResponse:
        """
        Standardized request method with automatic retries, proxy rotation, 
        and error mapping.
        """
        last_exception = None
        
        # Sanitize parameters for aiohttp compatibility
        clean_params = self._sanitize_params(params)

        for attempt in range(1, self.config.max_retries + 1):
            session = await self.sm.get_session(stateless=stateless)
            proxy = self.sm.get_next_proxy()
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            
            # For stateless requests, we manually inject the "clean" cookies
            if stateless:
                initial_cookies = self.sm.get_warm_up_cookies()
                # Merge with any cookies provided in kwargs
                provided_cookies = kwargs.get("cookies") or {}
                kwargs["cookies"] = {**initial_cookies, **provided_cookies}

            try:
                response = await session.request(
                    method=method,
                    url=url,
                    params=clean_params,
                    proxy=proxy,
                    timeout=timeout,
                    **kwargs
                )
                
                if response.status == 429:
                    raise RateLimitError(f"Rate limited (429) on {url}")
                if response.status == 403:
                    raise ProxyError(f"Forbidden (403) for {url}")
                if response.status == 401:
                    raise ProxyError(f"Unauthorized (401) for {url}")
                
                if response.status >= 400:
                    body = await response.text()
                    logger.warning(f"Request to {url} failed with status {response.status}. Body: {body[:200]}")
                
                response.raise_for_status()
                return response
            
            except (asyncio.TimeoutError, aiohttp.ClientError, ScraperError) as e:
                logger.warning(f"Attempt {attempt}/{self.config.max_retries} failed for {url}: {e}")
                last_exception = e
                # Release connection if we have a response object
                if 'response' in locals() and response is not None:
                    await response.release()
                
                if attempt < self.config.max_retries:
                    # Exponential backoff: 1s, 2s, 4s...
                    await asyncio.sleep(2 ** (attempt - 1))

        if isinstance(last_exception, ScraperError):
            raise last_exception
        raise ScraperError(f"Failed {method} {url} after {self.config.max_retries} attempts. Last error: {last_exception}")

    async def get(self, url: str, stateless: bool = False, **kwargs) -> aiohttp.ClientResponse:
        return await self.request("GET", url, stateless=stateless, **kwargs)

    async def post(self, url: str, stateless: bool = False, **kwargs) -> aiohttp.ClientResponse:
        return await self.request("POST", url, stateless=stateless, **kwargs)

    async def run_concurrently(self, tasks: Iterable[Any]) -> ConcurrentResults:
        """
        Executes a collection of tasks (usually request calls) concurrently,
        respecting the pool_size semaphore. The order of the results is guaranteed by asyncio.gather().
        """
        async def wrap_task(task):
            async with self._semaphore:
                try:
                    if asyncio.iscoroutine(task):
                        return await task
                    return task
                except Exception as e:
                    logger.error(f"Concurrent task failed: {e}")
                    return e
        
        timer = Timer(start=True)
        results = await asyncio.gather(*(wrap_task(t) for t in tasks), return_exceptions=True)
        timer.stop()
        
        execution_time = timer.seconds_elapsed
        logger.debug(f"Concurrent tasks completed in {execution_time:.2f}s")
        return ConcurrentResults(results=results, execution_time=execution_time)

    async def __aenter__(self):
        await self.update_active_airports()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.sm.close()
