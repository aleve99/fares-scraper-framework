import logging
import asyncio
import aiohttp
from typing import Optional, Tuple, List, Iterable, Dict
from datetime import date, timedelta, datetime, time

from ...base.base_scraper import BaseScraper
from ...base.types import Airport, OneWayFare, RoundTripFare, Schedule, ConcurrentResults
from ...base.config import settings, ScraperSettings
from .payload import get_farfnd_one_way_payload, get_availabilty_payload
from .models import RyanairAirportResponse, RyanairScheduleResponse

logger = logging.getLogger("scraper.ryanair")

class RyanairScraper(BaseScraper):
    OPERATORS = {
        "Malta Air": "MW*",
        "BUZZ": "RR",
        "Ryanair UK": "RK",
        "Ryanair": "FR"
    }
    FLEX_DAYS = 6

    BASE_API_URL = "https://www.ryanair.com/api/"
    SERVICES_API_URL = "https://services-api.ryanair.com/"

    def __init__(self, config: ScraperSettings = settings, USD: bool = False):
        self._market = "en-us" if USD else "it-it"
        self._currency_str = "en-us/" if USD else ""
        
        super().__init__(
            config=config,
            base_url="https://www.ryanair.com",
            warm_up_url="https://www.ryanair.com/",
            default_headers={"client": "desktop", "client-version": "3.132.0"}
        )

    async def update_active_airports(self):
        logger.info("Updating Ryanair active airports...")
        url = self._active_airports_url()
        async with await self.get(url) as res:
            data = await res.json()
            airports = [RyanairAirportResponse(**a) for a in data]
            self.active_airports = tuple(
                Airport(
                    iata_code=a.code,
                    lat=a.coordinates.latitude,
                    lng=a.coordinates.longitude,
                    name=a.name
                ) for a in airports
            )
        logger.info(f"Successfully updated active airports. Found {len(self.active_airports)}.")

    async def get_destination_codes(self, origin: str) -> Tuple[str, ...]:
        url = self._destinations_url(origin)
        logger.info(f"Getting destinations for {origin}")
        async with await self.get(url) as res:
             data = await res.json()
             # Check if data is a list and contains dicts with 'arrivalAirport'
             if isinstance(data, list) and all(isinstance(item, dict) and 'arrivalAirport' in item for item in data):
                return tuple(
                    dest['arrivalAirport']['code'] for dest in data if 'code' in dest['arrivalAirport']
                )
             else:
                 logger.warning(f"Unexpected format for destination codes response for {origin}: {data}")
                 # Return empty tuple on unexpected format
                 return tuple()
                 
    async def search_one_way_fares(
        self,
        origin: str,
        from_date: date,
        to_date: Optional[date] = None,
        destinations: Iterable[str] = []
    ) -> List[OneWayFare]:
        """
        Searches for one-way fares using the Farfnd API.
        
        This method uses a time-slicing approach based on flight schedules to bypass 
        the API's limitation of only returning the cheapest fare in a given time range.
        """
        if not destinations:
            destinations = await self.get_destination_codes(origin)
        
        if not to_date:
            to_date = from_date + timedelta(days=1)

        # 1. Prepare time-sliced parameters based on monthly schedules
        request_params = await self._prepare_availability_request_params_v2(
            origin=origin,
            from_date=from_date,
            to_date=to_date,
            destinations=destinations,
            round_trip=False
        )

        # 2. Execute all granular requests concurrently via framework engine
        tasks = [self.get(p["url"], params=p["params"]) for p in request_params]
        logger.info(f"Executing {len(tasks)} Farfnd requests concurrently.")
        results: ConcurrentResults = await self.run_concurrently(tasks)

        fares = []
        for i, result in enumerate(results.results):
            if isinstance(result, Exception):
                logger.warning(f"Farfnd request failed (index {i}): {result}")
                continue
            
            try:
                async with result as response:
                    if response.status != 200:
                        logger.warning(f"Farfnd request failed with status {response.status}: {response.url}")
                        continue
                    
                    json_res = await response.json()
                    if json_res.get('nextPage') is not None:
                        # Pagination is rare but logged for future awareness
                        logger.warning(f"Farfnd response indicates pagination (nextPage is not None), but handling is not implemented. URL: {response.url}")

                    # 3. Parse individual flight entries
                    for flight in json_res.get('fares', []):
                        info = flight.get('outbound')
                        if not info: continue
                        
                        try:
                            # Extract flight number (e.g., "FR1234")
                            flight_num_raw = info.get('flightNumber', '')
                            carrier = flight_num_raw.replace(' ', '')[:2] if len(flight_num_raw.replace(' ', '')) >= 2 else ''
                            flight_num = self.parse_flight_number(flight_num_raw, carrier)
                            
                            fares.append(
                                OneWayFare(
                                    dep_time=datetime.fromisoformat(info['departureDate']),
                                    arr_time=datetime.fromisoformat(info['arrivalDate']),
                                    origin=origin,
                                    destination=info['arrivalAirport']['iataCode'],
                                    fare=info['price']['value'],
                                    left=-1, # Not provided by this endpoint
                                    currency=info['price']['currencyCode'],
                                    flight_number=flight_num,
                                    operating_carrier=carrier,
                                    marketing_carrier=carrier
                                )
                            )
                        except (KeyError, TypeError, ValueError) as parse_err:
                            logger.warning(f"Failed to parse fare entry: {parse_err}. Data: {info}")
                             
            except Exception as e:
                 logger.error(f"Failed to process farfnd response: {e}", exc_info=True)
        
        logger.info(f"Scraped {origin} one-way fares in {results.execution_time:.2f}s, found {len(fares)} fares.")
        return fares

    async def search_round_trip_fares(
        self,
        origin: str,
        min_days: int,
        max_days: int,
        from_date: date,
        to_date: Optional[date] = None,
        destinations: Iterable[str] = []
    ) -> List[RoundTripFare]:
        """
        Searches for round-trip fares using the Availability API (v1).
        
        This method supports finding flexible date combinations within a stay duration 
        range. It handles the aggregation of multiple flexible date responses.
        """
        if not destinations:
            destinations = await self.get_destination_codes(origin)

        # 1. Prepare parameters for all required requests (v1 availability)
        code_requests_params = await self._prepare_availability_request_params(
            origin=origin,
            from_date=from_date,
            to_date=to_date,
            destinations=destinations,
            round_trip=True
        )
        
        all_fares = []
        for code, param_list in code_requests_params.items():
            # 2. Execute requests concurrently for the specific destination
            tasks = [self.get(p["url"], params=p["params"]) for p in param_list]
            results: ConcurrentResults = await self.run_concurrently(tasks)
            
            successful_data = []
            for res in results.results:
                if isinstance(res, aiohttp.ClientResponse):
                    async with res as response:
                        successful_data.append(await response.json())
                elif isinstance(res, Exception):
                    logger.warning(f"Availability request failed for {origin}-{code}: {res}")

            if successful_data:
                # 3. Compute all valid round-trip combinations from the aggregated data
                computed_fares = self._compute_round_trip_fares(
                    response_data_list=successful_data,
                    origin=origin,
                    destination=code,
                    min_days=min_days,
                    max_days=max_days
                )
                all_fares.extend(computed_fares)
            
            logger.info(f"{origin}-{code} availability scraped in {results.execution_time:.2f}s")

        return all_fares

    async def _prepare_availability_request_params(
        self,
        origin: str,
        from_date: date,
        to_date: Optional[date],
        destinations: Iterable[str],
        round_trip: bool = True
    ) -> Dict[str, List[Dict]]:
        """Prepares parameters for availability API calls (v1)."""
        requests_params = {}
        dest_list = list(destinations)

        resolved_to_date = {}
        if to_date is None:
            tasks = [self.get_available_dates(origin, code) for code in dest_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for i, result in enumerate(results):
                code = dest_list[i]
                if isinstance(result, Exception) or not result:
                    resolved_to_date[code] = from_date
                else:
                    resolved_to_date[code] = date.fromisoformat(result[-1])
        else:
            for code in dest_list:
                resolved_to_date[code] = to_date

        for code in dest_list:
            param_list = []
            _to_date = resolved_to_date.get(code, from_date)
            if _to_date < from_date: _to_date = from_date

            _current_date = from_date
            while _current_date <= _to_date:
                days_remaining = (_to_date - _current_date).days
                flex_days = min(self.FLEX_DAYS, days_remaining)
                
                payload = get_availabilty_payload(
                    origin=origin,
                    destination=code,
                    date_out=_current_date,
                    date_in=_current_date,
                    flex_days=flex_days,
                    round_trip=round_trip
                )
                
                url = self._availabilty_url()
                param_list.append({"url": url, "params": payload.to_dict()})
                _current_date += timedelta(days=flex_days + 1)
            
            requests_params[code] = param_list
        
        return requests_params

    async def _prepare_availability_request_params_v2(
        self,
        origin: str,
        from_date: date,
        to_date: date,
        destinations: Iterable[str],
        round_trip: bool = True
    ) -> List[Dict]:
        """
        Prepares parameters for Farfnd (v2) API calls using a time-slicing algorithm.
        
        Ryanair's Farfnd API returns only the single cheapest flight for a given 
        destination within a requested time window. To retrieve ALL flights, we:
        1. Fetch the full schedule for the requested date range.
        2. Group flights into granular time windows (slices) such that no destination 
           repeats within a single slice.
        3. Generate an API request for each unique slice.
        """
        if round_trip:
            raise ValueError("round_trip must be False for Farfnd one-way fares")

        dest_list = list(destinations)
        # Fetch actual schedules to drive the slicing logic
        schedules_by_code = await self._fetch_and_compute_schedules(
            origin=origin,
            destinations=dest_list,
            from_date=from_date,
            to_date=to_date
        )

        request_params = []
        for days_offset in range(0, (to_date - from_date).days + 1):
            current_date = from_date + timedelta(days=days_offset)
            day_schedules: List[Schedule] = []
            for code in dest_list:
                for s in schedules_by_code.get(code, []):
                    if s.departure_time.date() == current_date:
                        day_schedules.append(s)

            if not day_schedules:
                continue

            day_schedules.sort(key=lambda s: s.departure_time)
            
            time_ranges = []
            time_from = time(0, 0)
            time_to = time_from
            active_dest_list = list(set(s.destination for s in day_schedules))
            counts = {code: 0 for code in active_dest_list}
            
            i = 0
            while i < len(day_schedules):
                s = day_schedules[i]

                if s.destination not in counts:
                    i += 1
                    continue

                if counts[s.destination] == 1:
                    # Destination repeats -> End current slice 1 minute before this departure
                    time_to = (datetime.combine(
                        current_date, 
                        s.departure_time.time()) - timedelta(minutes=1)
                    ).time()
                    time_ranges.append((time_from, time_to))
                    
                    # Start new slice from this departure time
                    time_from = s.departure_time.time()
                    time_to = time_from
                    
                    # Handle multiple flights departing at the same minute
                    same_time_schedules = [
                        ds for ds in day_schedules[i:]
                        if ds.departure_time.time() == s.departure_time.time()
                    ]

                    # Reset all counts for the new granular slice
                    counts = {code: 0 for code in active_dest_list}
                    for x in same_time_schedules:
                        if x.destination in counts:
                            counts[x.destination] += 1
                    
                    # Advance pointer past all same-minute flights
                    if same_time_schedules:
                        last_idx = -1
                        for idx in range(i, len(day_schedules)):
                            if day_schedules[idx] == same_time_schedules[-1]:
                                last_idx = idx
                                break
                        if last_idx != -1:
                            i = last_idx
                        else:
                            i += len(same_time_schedules) - 1 
                else:
                    # First time seeing this destination in the current slice
                    counts[s.destination] += 1
                    time_to = s.departure_time.time()
                
                i += 1
            
            # Close the final slice of the day
            if time_ranges or day_schedules:
                time_ranges.append((time_from, time(23, 59)))

            # Generate request payloads for each calculated slice
            for t_from, t_to in time_ranges:
                if not active_dest_list: continue

                payload = get_farfnd_one_way_payload(
                    origin=origin,
                    destinations=active_dest_list,
                    date_from=current_date,
                    date_to=current_date,
                    time_from=t_from,
                    time_to=t_to,
                    market=self._market
                )
                # Framework handles sanitization automatically via request()
                request_params.append({
                    "url": self._one_way_fares_url(), 
                    "params": payload.to_dict()
                })

        return request_params

    async def _fetch_and_compute_schedules(
            self,
            origin: str,
            destinations: Iterable[str],
            from_date: date,
            to_date: date
        ) -> Dict[str, List[Schedule]]:
        """(Async) Fetches schedules concurrently for multiple destinations using get_schedules."""
        schedules_by_code: Dict[str, List[Schedule]] = {dest: [] for dest in destinations}

        tasks = []
        # We store metadata to map results back to destinations
        task_metadata = []

        for destination in destinations:
            for year in range(from_date.year, to_date.year + 1):
                if year == from_date.year and from_date.year != to_date.year:
                    month_range = range(from_date.month, 13)
                elif year == to_date.year and from_date.year != to_date.year:
                    month_range = range(1, to_date.month + 1)
                elif year == from_date.year and from_date.year == to_date.year:
                    month_range = range(from_date.month, to_date.month + 1)
                else:
                    month_range = range(1, 13)

                for month in month_range:
                    tasks.append(self.get_schedules(origin, destination, year, month))
                    task_metadata.append(destination)

        # Execute get_schedules tasks concurrently
        results: ConcurrentResults = await self.run_concurrently(tasks)
        
        # results.results contains the lists of schedules returned by each get_schedules call
        for destination, res in zip(task_metadata, results.results):
            if isinstance(res, Exception):
                 logger.warning(f"Schedule task failed for {origin}-{destination}: {res}")
                 continue
            
            if isinstance(res, list):
                for sched in res:
                    # Filter by the specific date range
                    if from_date <= sched.departure_time.date() <= to_date:
                        schedules_by_code[destination].append(sched)
        
        return schedules_by_code

    def _compute_round_trip_fares(
        self,
        response_data_list: List[dict],
        origin: str,
        destination: str,
        min_days: int,
        max_days: int
    ) -> List[RoundTripFare]:
        """Computes round trip fares from availability JSON data."""
        aggregated_trips = None
        currency = None

        for json_res in response_data_list:
            if not isinstance(json_res, dict) or 'trips' not in json_res:
                continue
            if aggregated_trips is None:
                aggregated_trips = json_res['trips']
                currency = json_res.get('currency')
            else:
                if len(aggregated_trips) >= 2 and len(json_res['trips']) >= 2:
                    aggregated_trips[0]['dates'].extend(json_res['trips'][0]['dates'])
                    aggregated_trips[1]['dates'].extend(json_res['trips'][1]['dates'])

        if not aggregated_trips or len(aggregated_trips) < 2:
            return []

        fares = []
        outbound_dates = aggregated_trips[0].get('dates', [])
        return_dates = aggregated_trips[1].get('dates', [])
        return_dates_map = {date.fromisoformat(d['dateOut'][:10]): d['flights'] for d in return_dates if 'dateOut' in d}

        for trip_date_out in outbound_dates:
            date_out_str = trip_date_out.get('dateOut')
            if not date_out_str: continue
            date_out = date.fromisoformat(date_out_str[:10])
            
            for outbound_flight in trip_date_out.get('flights', []):
                if outbound_flight.get('faresLeft', 0) != 0 and outbound_flight.get('regularFare'):
                    min_ret = date_out + timedelta(days=min_days)
                    max_ret = date_out + timedelta(days=max_days)

                    curr_ret = min_ret
                    while curr_ret <= max_ret:
                        if curr_ret in return_dates_map:
                            for ret_flight in return_dates_map[curr_ret]:
                                if ret_flight.get('faresLeft', 0) != 0 and ret_flight.get('regularFare'):
                                    out_num_raw = outbound_flight['flightNumber']
                                    ret_num_raw = ret_flight['flightNumber']
                                    
                                    out_carrier = out_num_raw.replace(' ', '')[:2] if len(out_num_raw.replace(' ', '')) >= 2 else ''
                                    ret_carrier = ret_num_raw.replace(' ', '')[:2] if len(ret_num_raw.replace(' ', '')) >= 2 else ''
                                    
                                    out_num = self.parse_flight_number(out_num_raw, out_carrier)
                                    ret_num = self.parse_flight_number(ret_num_raw, ret_carrier)
                                    
                                    fares.append(RoundTripFare(
                                        outbound=OneWayFare(
                                            dep_time=datetime.fromisoformat(outbound_flight['time'][0]),
                                            arr_time=datetime.fromisoformat(outbound_flight['time'][1]),
                                            origin=origin,
                                            destination=destination,
                                            fare=outbound_flight['regularFare']['fares'][0]['amount'],
                                            currency=currency,
                                            flight_number=out_num,
                                            operating_carrier=out_carrier,
                                            marketing_carrier=out_carrier,
                                            left=outbound_flight['faresLeft']
                                        ),
                                        inbound=OneWayFare(
                                            dep_time=datetime.fromisoformat(ret_flight['time'][0]),
                                            arr_time=datetime.fromisoformat(ret_flight['time'][1]),
                                            origin=destination,
                                            destination=origin,
                                            fare=ret_flight['regularFare']['fares'][0]['amount'],
                                            currency=currency,
                                            flight_number=ret_num,
                                            operating_carrier=ret_carrier,
                                            marketing_carrier=ret_carrier,
                                            left=ret_flight['faresLeft']
                                        )
                                    ))
                        curr_ret += timedelta(days=1)
        return fares

    async def get_schedules(
        self,
        origin: str,
        destination: str,
        year: int,
        month: int
    ) -> List[Schedule]:
        """Fetches or parses flight schedules for a given route and month."""
        if year is None or month is None:
            raise ValueError("Ryanair requires year and month for schedules")
        
        url = self._schedules_url(origin, destination, year, month)
        async with await self.get(url) as res:
            data = await res.json()

        response_model = RyanairScheduleResponse(**data)
        
        schedules = []
        for day in response_model.days:
            day_date = date(year, month, day.day)
            for flight in day.flights:
                dep_time = time.fromisoformat(flight.departureTime)
                arr_time = time.fromisoformat(flight.arrivalTime)
                dep_dt = datetime.combine(day_date, dep_time)
                arr_dt = datetime.combine(day_date if arr_time >= dep_time else day_date + timedelta(days=1), arr_time)
                
                schedules.append(Schedule(
                    origin=origin,
                    destination=destination,
                    departure_time=dep_dt,
                    arrival_time=arr_dt,
                    flight_number=int(flight.number)
                ))
        
        return schedules

    async def get_available_dates(self, origin: str, destination: str) -> Tuple[str, ...]:
        url = self._available_dates_url(origin, destination)
        async with await self.get(url) as res:
            data = await res.json()
            return tuple(data)

    @classmethod
    def _airport_info_url(cls, iata_code: str) -> str:
        return cls.BASE_API_URL + f'views/locate/5/airports/en/{iata_code}'

    @classmethod
    def _available_dates_url(cls, origin: str, destination: str) -> str:
        return cls.BASE_API_URL + \
            f"farfnd/v4/oneWayFares/{origin}/{destination}/availabilities"
    
    @classmethod
    def _active_airports_url(cls) -> str:
        return cls.BASE_API_URL + "views/locate/5/airports/en/active"
    
    @classmethod
    def _destinations_url(cls, origin: str) -> str:
        return cls.BASE_API_URL + \
            f"views/locate/searchWidget/routes/en/airport/{origin}"
    
    @classmethod
    def _one_way_fares_url(cls) -> str:
        return cls.SERVICES_API_URL + "farfnd/v4/oneWayFares"

    @classmethod
    def _round_trip_fares_url(cls) -> str:
        return cls.SERVICES_API_URL + "farfnd/v4/roundTripFares"
    
    @classmethod
    def _schedules_url(cls, origin: str, destination: str, year: int, month: int) -> str:
        return cls.SERVICES_API_URL + \
            f"timtbl/3/schedules/{origin}/{destination}/years/{year}/months/{month}"
    
    def _availabilty_url(self) -> str:
        return self.BASE_API_URL + \
            f"booking/v4/{self._currency_str}availability"
