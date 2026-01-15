import logging
from typing import Optional, Tuple, List, Iterable, Dict
from datetime import date, timedelta, datetime

from ...base.base_scraper import BaseScraper
from ...base.types import Airport, OneWayFare, RoundTripFare, ConcurrentResults
from ...base.config import settings, ScraperSettings
from .payload import TimetableV2Payload, TimetableFlightItem, MapPayload, FlightDatesPayload, DetailedFlightsPayload
from .models import (
    WizzAirMapResponse,
    WizzAirTimetableResponse,
    WizzAirFlightDatesResponse,
    WizzAirDetailedFlightsResponse,
    WizzAirDetailedFlight,
    WizzAirTimetableFlight
)

logger = logging.getLogger("scraper.wizzair")

class WizzAirScraper(BaseScraper):
    def __init__(self, config: ScraperSettings = settings):
        super().__init__(
            config=config,
            base_url="https://be.wizzair.com",
            warm_up_url="https://www.wizzair.com/",
            default_headers={

            }
        )
        self.build_number = None
        self._connections: Dict[str, Tuple[str, ...]] = {}

    async def _update_build_number(self):
        """Fetches the current build number from wizzair.com."""
        url = self._build_number_url()
        async with await self.get(url) as res:
            text = await res.text()
            # The response is usually like "SSR https://be.wizzair.com/27.39.0"
            if "be.wizzair.com/" in text:
                self.build_number = text.split("be.wizzair.com/")[1].strip()
                logger.info(f"Updated WizzAir build number: {self.build_number}")
            else:
                logger.error(f"Failed to parse build number from: {text}")
                raise ValueError(f"Could not find build number in response: {text}")

    async def update_active_airports(self):
        if not self.build_number:
            await self._update_build_number()

        logger.info("Updating WizzAir active airports...")
        url = self._active_airports_url()
        payload = MapPayload()
        async with await self.get(url, params=payload.to_dict()) as res:
            data = await res.json()
            model = WizzAirMapResponse(**data)
            
            # Create a set of valid (non-MAC) airport codes
            valid_iatas = {
                city.iata for city in model.cities 
                if not city.isFakeStation and (city.mac is None or city.mac != city.iata)
            }

            # Map origin -> connections (filtering both origins and destinations)
            self._connections = {
                city.iata: tuple(conn.iata for conn in city.connections if conn.iata in valid_iatas)
                for city in model.cities
                if city.iata in valid_iatas
            }
            
            # Populate base scraper airports
            self.active_airports = tuple(
                Airport(
                    iata_code=city.iata,
                    lat=city.latitude,
                    lng=city.longitude,
                    name=city.shortName
                ) for city in model.cities
                if city.iata in valid_iatas
            )
        logger.info(f"Successfully updated active airports. Found {len(self.active_airports)}.")

    async def get_destination_codes(self, origin: str) -> Tuple[str, ...]:
        if not self._connections:
            await self.update_active_airports()
        
        logger.info(f"Getting destinations for {origin}")
        return self._connections.get(origin, tuple())

    async def _fetch_detailed_flights(self, origin: str, destination: str, date_obj: date) -> List[WizzAirDetailedFlight]:
        """Fetches detailed flight info (flight numbers, carriers, exact times) for a route and date."""
        url = self._travel_agency_booking_url()
        payload = DetailedFlightsPayload(
            date=date_obj,
            departureStationIata=origin,
            arrivalStationIata=destination
        )
        
        try:
            # Using stateless=True to avoid session state tainting
            async with await self.get(url, params=payload.to_dict(), stateless=True) as res:
                if res.status == 200:
                    data = await res.json()
                    model = WizzAirDetailedFlightsResponse(**data)
                    return model.flights
        except Exception as e:
            logger.warning(f"Failed to fetch detailed flights for {origin}-{destination} on {date_obj}: {e}")
        
        return []

    async def get_available_dates(self, origin: str, destination: str, max_months: int = 12) -> Tuple[str, ...]:
        """Fetches available flight dates for a given route up to max_months ahead."""
        if not self.build_number:
            await self._update_build_number()

        all_dates = []
        today = date.today()
        
        # WizzAir API allows ~2 months range (61 days). 
        # We calculate the number of 2-month chunks needed to cover max_months.
        num_chunks = (max_months + 1) // 2
        
        tasks = []
        for i in range(num_chunks):
            from_date = today + timedelta(days=i*61)
            to_date = from_date + timedelta(days=60)
            payload = FlightDatesPayload(
                departureStation=origin,
                arrivalStation=destination,
                from_date=from_date,
                to_date=to_date
            )
            tasks.append(self.get(self._flight_dates_url(), params=payload.to_dict(), stateless=True))

        results: ConcurrentResults = await self.run_concurrently(tasks)
        
        for res in results.results:
            if isinstance(res, Exception):
                logger.warning(f"Error fetching date chunk: {res}")
                continue
                
            async with res as response:
                if response.status == 200:
                    data = await response.json()
                    model = WizzAirFlightDatesResponse(**data)
                    if model.flightDates:
                        chunk_dates = [d.date().isoformat() for d in model.flightDates]
                        all_dates.extend(chunk_dates)
                else:
                    logger.warning(f"Flight dates API returned status {response.status}")

        return tuple(sorted(list(set(all_dates))))

    async def search_one_way_fares(
        self,
        origin: str,
        from_date: date,
        to_date: Optional[date] = None,
        destinations: Iterable[str] = []
    ) -> List[OneWayFare]:
        if not self.build_number:
            await self._update_build_number()

        if not destinations:
            destinations = await self.get_destination_codes(origin)

        logger.info(f"Getting one-way fares for {origin} to {len(destinations)} destinations")
        if not to_date:
            to_date = from_date + timedelta(days=1) # Default to just one day

        # 1. Fetch prices from timetableV2
        tasks = []
        for dest in destinations:
            # Slice the date range into 30-day chunks to avoid InvalidTimeDateRange
            current_from = from_date
            while current_from <= to_date:
                current_to = min(current_from + timedelta(days=30), to_date)
                payload = TimetableV2Payload(
                    flightList=[
                        TimetableFlightItem(
                            departureStation=origin,
                            arrivalStation=dest,
                            from_date=current_from,
                            to_date=current_to
                        )
                    ]
                )
                url = self._timetable_url()
                tasks.append(self.post(url, json=payload.to_dict(), stateless=True))
                current_from = current_to + timedelta(days=1)

        logger.info(f"Executing {len(tasks)} timetable requests concurrently.")
        results: ConcurrentResults = await self.run_concurrently(tasks)
        
        price_results: List[WizzAirTimetableFlight] = [] # List of flights with price
        detailed_tasks = []
        detailed_metadata = [] # List of (origin, destination, date)

        for res in results.results:
            if isinstance(res, Exception): continue
            try:
                async with res as response:
                    if response.status != 200: continue
                    data = await response.json()
                    model = WizzAirTimetableResponse(**data)
                    
                    for flight in filter(lambda f: f.priceType == "price", model.outboundFlights):
                        price_results.append(flight)
                        # Prepare tasks to fetch detailed flight info for these dates
                        dep_date = flight.departureDate.date()
                        meta = (flight.departureStation, flight.arrivalStation, dep_date)
                        if meta not in detailed_metadata:
                            detailed_metadata.append(meta)
                            detailed_tasks.append(self._fetch_detailed_flights(*meta))
            except Exception as e:
                logger.error(f"Error parsing WizzAir timetable response: {e}")

        # 2. Fetch detailed flight info concurrently
        if not detailed_tasks:
            return []

        logger.info(f"Enriching {len(price_results)} price entries with detailed flight info for {len(detailed_tasks)} unique {origin} dates.")
        detailed_results: ConcurrentResults = await self.run_concurrently(detailed_tasks)
        
        # Map detailed results by (origin, destination, departure_time)
        details_map: Dict[Tuple[str, str, datetime], WizzAirDetailedFlight] = {}
        for (orig, dest, _), res in zip(detailed_metadata, detailed_results.results):
            if isinstance(res, list):
                for f in res:
                    details_map[(orig, dest, f.departureDateTime)] = f

        # 3. Combine results
        fares = []
        for flight in price_results:
            details = details_map.get((flight.departureStation, flight.arrivalStation, flight.departureDate))
            
            if details:
                flight_num = self.parse_flight_number(details.flightNumber, details.carrierCode)

                fares.append(
                    OneWayFare(
                        dep_time=flight.departureDate,
                        arr_time=details.arrivalDateTime,
                        origin=flight.departureStation,
                        destination=flight.arrivalStation,
                        fare=flight.price.amount,
                        currency=flight.price.currencyCode,
                        flight_number=flight_num,
                        operating_carrier=details.carrierCode,
                        marketing_carrier=details.carrierCode
                    )
                )
            else:
                logger.warning(f"Detailed info missing for {flight.departureStation}-{flight.arrivalStation} on {flight.departureDate}. Skipping fare.")

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
        if not destinations:
            destinations = await self.get_destination_codes(origin)
        
        logger.info(f"Getting round-trip fares for {origin} to {len(destinations)} destinations")
        if not to_date:
            to_date = from_date + timedelta(days=30)

        # 1. Fetch prices from timetableV2 (both legs)
        tasks = []
        for dest in destinations:
            # Slice the date range into 30-day chunks to avoid InvalidTimeDateRange
            current_from = from_date
            while current_from <= to_date:
                current_to = min(current_from + timedelta(days=30), to_date)
                payload = TimetableV2Payload(
                    flightList=[
                        TimetableFlightItem(
                            departureStation=origin,
                            arrivalStation=dest,
                            from_date=current_from,
                            to_date=current_to
                        ),
                        TimetableFlightItem(
                            departureStation=dest,
                            arrivalStation=origin,
                            from_date=current_from,
                            to_date=current_to
                        )
                    ]
                )
                url = self._timetable_url()
                tasks.append(self.post(url, json=payload.to_dict(), stateless=True))
                current_from = current_to + timedelta(days=1)

        logger.info(f"Executing {len(tasks)} timetable requests concurrently.")
        results: ConcurrentResults = await self.run_concurrently(tasks)
        
        price_results = [] # List of (outbound_flights, return_flights)
        detailed_tasks = []
        detailed_metadata = []

        for res in results.results:
            if isinstance(res, Exception): continue
            async with res as response:
                if response.status != 200: continue
                data = await response.json()
                model = WizzAirTimetableResponse(**data)
                
                if not model.outboundFlights or not model.returnFlights:
                    continue
                
                price_results.append((model.outboundFlights, model.returnFlights))
                
                # Collect unique route-days for enrichment
                for flight in model.outboundFlights + model.returnFlights:
                    dep_date = flight.departureDate.date()
                    meta = (flight.departureStation, flight.arrivalStation, dep_date)
                    if meta not in detailed_metadata:
                        detailed_metadata.append(meta)
                        detailed_tasks.append(self._fetch_detailed_flights(*meta))

        # 2. Fetch detailed flight info concurrently
        if not detailed_tasks:
            return []

        logger.info(f"Enriching round-trip price entries with detailed flight info for {len(detailed_tasks)} unique route-days.")
        detailed_results: ConcurrentResults = await self.run_concurrently(detailed_tasks)
        
        details_map = {}
        for (orig, dest, _), res in zip(detailed_metadata, detailed_results.results):
            if isinstance(res, list):
                for f in res:
                    details_map[(orig, dest, f.departureDateTime)] = f

        # 3. Combine and compute round trip combinations
        all_round_trip_fares = []
        for outbound_flights, return_flights in price_results:
            for out in outbound_flights:
                out_details = details_map.get((out.departureStation, out.arrivalStation, out.departureDate))
                
                for ret in return_flights:
                    ret_details = details_map.get((ret.departureStation, ret.arrivalStation, ret.departureDate))
                    
                    stay_duration = (ret.departureDate - out.departureDate).days
                    if min_days <= stay_duration <= max_days:
                        if not out_details:
                            logger.warning(f"Detailed info missing for outbound {out.departureStation}-{out.arrivalStation} on {out.departureDate}. Skipping combination.")
                            continue
                        if not ret_details:
                            logger.warning(f"Detailed info missing for inbound {ret.departureStation}-{ret.arrivalStation} on {ret.departureDate}. Skipping combination.")
                            continue

                        # Construct outbound OneWayFare
                        out_flight_num = self.parse_flight_number(out_details.flightNumber, out_details.carrierCode)

                        out_fare = OneWayFare(
                            dep_time=out.departureDate,
                            arr_time=out_details.arrivalDateTime,
                            origin=out.departureStation,
                            destination=out.arrivalStation,
                            fare=out.price.amount,
                            currency=out.price.currencyCode,
                            flight_number=out_flight_num,
                            operating_carrier=out_details.carrierCode,
                            marketing_carrier=out_details.carrierCode
                        )
                        
                        # Construct inbound OneWayFare
                        ret_flight_num = self.parse_flight_number(ret_details.flightNumber, ret_details.carrierCode)

                        ret_fare = OneWayFare(
                            dep_time=ret.departureDate,
                            arr_time=ret_details.arrivalDateTime,
                            origin=ret.departureStation,
                            destination=ret.arrivalStation,
                            fare=ret.price.amount,
                            currency=ret.price.currencyCode,
                            flight_number=ret_flight_num,
                            operating_carrier=ret_details.carrierCode,
                            marketing_carrier=ret_details.carrierCode
                        )
                        
                        all_round_trip_fares.append(RoundTripFare(outbound=out_fare, inbound=ret_fare))
        
        logger.info(f"Scraped {origin} round-trip fares in {results.execution_time:.2f}s, found {len(all_round_trip_fares)} fares.")
        return all_round_trip_fares

    @classmethod
    def _build_number_url(cls) -> str:
        return "https://www.wizzair.com/buildnumber"

    def _active_airports_url(self) -> str:
        return f"{self.build_number}/Api/asset/map"

    def _flight_dates_url(self) -> str:
        return f"{self.build_number}/Api/search/flightDates"

    def _timetable_url(self) -> str:
        return f"{self.build_number}/Api/search/timetableV2"

    def _travel_agency_booking_url(self) -> str:
        return f"{self.build_number}/Api/booking/travelAgencyBookingFindFlight"
