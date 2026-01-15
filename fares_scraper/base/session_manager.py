import aiohttp
import asyncio
import logging
from itertools import cycle
from typing import List, Dict, Optional

logger = logging.getLogger("scraper.session_manager")

class SessionManager:
    """
    Manages the lifecycle of aiohttp ClientSession and proxy rotation.
    Designed to be robust and reusable across different airline scrapers.
    """
    def __init__(
        self, 
        timeout: int, 
        base_url: Optional[str] = None, 
        warm_up_url: Optional[str] = None,
        default_headers: Optional[Dict[str, str]] = None,
        proxies: Optional[List[str]] = None
    ):
        self._session: Optional[aiohttp.ClientSession] = None
        self._stateless_session: Optional[aiohttp.ClientSession] = None
        self._warm_up_cookies: Dict[str, str] = {}
        self._timeout = timeout
        self._base_url = base_url
        self._warm_up_url = warm_up_url
        self._default_headers = default_headers
        self._header_size_limit = 16 * 1024
        
        self._proxies = proxies or [None]
        self._proxies_loop = cycle(self._proxies)
        self._lock = asyncio.Lock()

    async def get_session(self, stateless: bool = False) -> aiohttp.ClientSession:
        """Returns the needed session, creating it if necessary."""
        async with self._lock:
            # Check if we need to initialize or warm up
            if self._session is None or self._session.closed:
                logger.debug("Initializing new aiohttp ClientSession")
                self._session = aiohttp.ClientSession(
                    headers=self._default_headers,
                    max_line_size=self._header_size_limit,
                    max_field_size=self._header_size_limit,
                    base_url=self._base_url
                )
                
                # Warm up session if a URL is provided (useful for setting cookies)
                target_warm_up = self._warm_up_url or (self._base_url if self._base_url else None)
                if target_warm_up:
                    try:
                        timeout = aiohttp.ClientTimeout(total=self._timeout)
                        # If target_warm_up is relative, join with base_url
                        url = target_warm_up if "://" in target_warm_up else f"{self._base_url}{target_warm_up}"
                        async with self._session.get(url, timeout=timeout) as response:
                            response.raise_for_status()
                            # Capture clean cookies for stateless requests
                            cookies = self._session.cookie_jar.filter_cookies(url)
                            self._warm_up_cookies = {k: v.value for k, v in cookies.items()}
                            logger.debug(f"Session warmed up via {url}. Captured {len(self._warm_up_cookies)} cookies.")
                    except Exception as e:
                        logger.warning(f"Session warm-up failed: {e}")

            if stateless:
                if self._stateless_session is None or self._stateless_session.closed:
                    # Create a session that DOES NOT store cookies
                    self._stateless_session = aiohttp.ClientSession(
                        headers=self._default_headers,
                        max_line_size=self._header_size_limit,
                        max_field_size=self._header_size_limit,
                        base_url=self._base_url,
                        cookie_jar=aiohttp.DummyCookieJar()
                    )
                return self._stateless_session
            
            return self._session

    def get_warm_up_cookies(self) -> Dict[str, str]:
        """Returns the cookies captured during the last warm-up."""
        return self._warm_up_cookies

    def get_next_proxy(self) -> Optional[str]:
        """Returns the next proxy from the rotation loop."""
        return next(self._proxies_loop)

    async def close(self):
        """Gracefully closes all sessions."""
        async with self._lock:
            if self._session and not self._session.closed:
                await self._session.close()
                self._session = None
            if self._stateless_session and not self._stateless_session.closed:
                await self._stateless_session.close()
                self._stateless_session = None
            logger.debug("All aiohttp ClientSessions closed")

    @property
    def timeout(self) -> int:
        return self._timeout
