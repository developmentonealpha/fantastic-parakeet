import datetime
import enum
import json
import logging
import random
import re
import string
import pandas as pd
from websocket import create_connection
import requests
import threading
from typing import List, Dict, Optional
from queue import Queue
import time

logger = logging.getLogger(__name__)

class Interval(enum.Enum):
    in_1_minute = "1"
    in_3_minute = "3"
    in_5_minute = "5"
    in_15_minute = "15"
    in_30_minute = "30"
    in_45_minute = "45"
    in_1_hour = "1H"
    in_2_hour = "2H"
    in_3_hour = "3H"
    in_4_hour = "4H"
    in_daily = "1D"
    in_weekly = "1W"
    in_monthly = "1M"

class TvDatafeed:
    __sign_in_url = 'https://www.tradingview.com/accounts/signin/'
    __search_url = 'https://symbol-search.tradingview.com/symbol_search/?text={}&hl=1&exchange={}&lang=en&type=&domain=production'
    __ws_headers = json.dumps({"Origin": "https://data.tradingview.com"})
    __signin_headers = {'Referer': 'https://www.tradingview.com'}
    __ws_timeout = 10  # Increased timeout
    __max_retries = 3
    __retry_delay = 3  # Increased delay

    def __init__(
        self,
        username: str = None,
        password: str = None,
        proxies: List[Dict[str, str]] = None,
    ) -> None:
        """Create TvDatafeed object

        Args:
            username (str, optional): TradingView username. Defaults to None.
            password (str, optional): TradingView password. Defaults to None.
            proxies (List[Dict[str, str]], optional): List of proxy dictionaries for HTTP requests. Defaults to None.
        """
        self.ws_debug = True  # Enable debug for troubleshooting
        self.proxies = proxies if proxies else []
        self.proxy_lock = threading.Lock()
        self.token = self.__auth(username, password)

        if self.token is None:
            self.token = "unauthorized_user_token"
            # logger.warning("Using nologin method, data you access may be limited")

    def __auth(self, username: str, password: str, proxy: Dict[str, str] = None) -> Optional[str]:
        if username is None or password is None:
            # logger.info("No credentials provided, using unauthorized access")
            return None

        data = {"username": username, "password": password, "remember": "on"}
        for attempt in range(self.__max_retries):
            try:
                response = requests.post(
                    url=self.__sign_in_url,
                    data=data,
                    headers=self.__signin_headers,
                    proxies=proxy,
                    timeout=10
                )
                if response.status_code == 200:
                    token = response.json()['user']['auth_token']
                    logger.info("Authentication successful")
                    return token
                else:
                    logger.error(f"Auth failed with status code: {response.status_code}")
            except Exception as e:
                logger.error(f'Auth attempt {attempt + 1}/{self.__max_retries} failed: {e}')
                if attempt < self.__max_retries - 1:
                    time.sleep(self.__retry_delay)
        logger.error('All auth attempts failed')
        return None

    def __create_connection(self):
        logger.debug("Creating websocket connection")
        ws_url = "wss://data.tradingview.com/socket.io/websocket"
        for attempt in range(self.__max_retries):
            try:
                self.ws = create_connection(
                    ws_url,
                    headers=self.__ws_headers,
                    timeout=self.__ws_timeout
                )
                logger.debug("WebSocket connection created successfully")
                return
            except Exception as e:
                logger.error(f"WebSocket connection attempt {attempt + 1}/{self.__max_retries} failed: {e}")
                if attempt < self.__max_retries - 1:
                    time.sleep(self.__retry_delay)
        raise Exception("Failed to create WebSocket connection after all retries")

    @staticmethod
    def __filter_raw_message(text: str) -> tuple:
        try:
            found = re.search('"m":"(.+?)",', text).group(1)
            found2 = re.search('"p":(.+?"}"])}', text).group(1)
            return found, found2
        except AttributeError:
            logger.error("Error in filter_raw_message")
            return None, None

    @staticmethod
    def __generate_session() -> str:
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters) for i in range(stringLength))
        return "qs_" + random_string

    @staticmethod
    def __generate_chart_session() -> str:
        stringLength = 12
        letters = string.ascii_lowercase
        random_string = "".join(random.choice(letters) for i in range(stringLength))
        return "cs_" + random_string

    @staticmethod
    def __prepend_header(st: str) -> str:
        return "~m~" + str(len(st)) + "~m~" + st

    @staticmethod
    def __construct_message(func: str, param_list: list) -> str:
        return json.dumps({"m": func, "p": param_list}, separators=(",", ":"))

    def __create_message(self, func: str, param_list: list) -> str:
        return self.__prepend_header(self.__construct_message(func, param_list))

    def __send_message(self, func: str, args: list):
        m = self.__create_message(func, args)
        if self.ws_debug:
            logger.debug(f"Sending: {m}")
        try:
            self.ws.send(m)
        except Exception as e:
            logger.error(f"Error sending WebSocket message: {e}")
            raise

    @staticmethod
    def __create_df(raw_data: str, symbol: str) -> pd.DataFrame:
        try:
            logger.debug(f"Processing raw data for {symbol}")
            out = re.search('"s":\[(.+?)\}\]', raw_data).group(1)
            x = out.split(',{"')
            data = []
            volume_data = True

            for xi in x:
                xi = re.split("\[|:|,|\]", xi)
                ts = datetime.datetime.fromtimestamp(float(xi[4]))
                row = [ts]
                for i in range(5, 10):
                    if not volume_data and i == 9:
                        row.append(0.0)
                        continue
                    try:
                        row.append(float(xi[i]))
                    except (ValueError, IndexError):
                        if i == 9:  # Volume column
                            volume_data = False
                            row.append(0.0)
                            logger.debug('No volume data')
                        else:
                            row.append(0.0)
                data.append(row)

            data = pd.DataFrame(
                data, columns=["datetime", "open", "high", "low", "close", "volume"]
            ).set_index("datetime")
            data.insert(0, "symbol", value=symbol)
            # logger.info(f"Successfully created DataFrame for {symbol} with {len(data)} rows")
            return data
        except (AttributeError, IndexError) as e:
            logger.error(f"No data found for {symbol}, error: {e}")
            logger.debug(f"Raw data sample: {raw_data[:500]}...")
            return pd.DataFrame()

    @staticmethod
    def __format_symbol(symbol: str, exchange: str = None, contract: int = None) -> str:
        # Only use the symbol as-is, do not prepend exchange
        # If user provides EXCHANGE:SYMBOL, keep it, else just use symbol
        if ":" in symbol:
            return symbol
        elif contract is None:
            return symbol
        elif isinstance(contract, int):
            return f"{symbol}{contract}!"
        else:
            raise ValueError("Not a valid contract")

    def get_hist(
        self,
        symbol: str,
        exchange: str,  # Only used for folder naming, not for data fetch
        interval: Interval = Interval.in_daily,
        n_bars: int = 10,
        fut_contract: int = None,
        extended_session: bool = False,
        proxy: Dict[str, str] = None
    ) -> pd.DataFrame:
        """Get historical data for a single symbol

        Args:
            symbol (str): Symbol name
            exchange (str): Exchange name
            interval (Interval, optional): Chart interval. Defaults to Interval.in_daily.
            n_bars (int, optional): Number of bars to download, max 5000. Defaults to 10.
            fut_contract (int, optional): None for cash, 1 for current contract, 2 for next contract. Defaults to None.
            extended_session (bool, optional): Regular session if False, extended if True. Defaults to False.
            proxy (Dict[str, str], optional): Proxy for HTTP requests. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame with OHLCV columns
        """
        print(f"Fetching historical data for {symbol} (exchange: {exchange}) with interval {interval.value} and n_bars {n_bars}")
        # Only use the symbol for data fetch, not the exchange
        formatted_symbol = self.__format_symbol(symbol, None, fut_contract)
        # logger.info(f"Fetching data for symbol: {formatted_symbol}")
        interval_value = interval.value
        session = self.__generate_session()
        chart_session = self.__generate_chart_session()

        for attempt in range(self.__max_retries):
            try:
                logger.info(f"Attempt {attempt + 1}/{self.__max_retries} for {formatted_symbol}")
                self.__create_connection()
                
                # Send initial messages
                self.__send_message("set_auth_token", [self.token])
                self.__send_message("chart_create_session", [chart_session, ""])
                self.__send_message("quote_create_session", [session])
                self.__send_message(
                    "quote_set_fields",
                    [
                        session,
                        "ch", "chp", "current_session", "description", "local_description",
                        "language", "exchange", "fractional", "is_tradable", "lp", "lp_time",
                        "minmov", "minmove2", "original_name", "pricescale", "pro_name",
                        "short_name", "type", "update_mode", "volume", "currency_code", "rchp", "rtc"
                    ]
                )
                self.__send_message("quote_add_symbols", [session, formatted_symbol, {"flags": ["force_permission"]}])
                self.__send_message("quote_fast_symbols", [session, formatted_symbol])
                self.__send_message(
                    "resolve_symbol",
                    [
                        chart_session,
                        "symbol_1",
                        f'={{"symbol":"{formatted_symbol}","adjustment":"splits","session":"{"regular" if not extended_session else "extended"}"}}'
                    ]
                )
                self.__send_message("create_series", [chart_session, "s1", "s1", "symbol_1", interval_value, n_bars])
                self.__send_message("switch_timezone", [chart_session, "exchange"])

                raw_data = ""
                message_count = 0
                max_messages = 100  # Prevent infinite loop
                
                logger.debug(f"Waiting for data...")
                while message_count < max_messages:
                    try:
                        result = self.ws.recv()
                        raw_data += result + "\n"
                        message_count += 1
                        
                        if self.ws_debug and message_count % 5 == 0:
                            logger.debug(f"Received {message_count} messages")
                        
                        if "series_completed" in result:
                            logger.debug("Series completed message received")
                            df = self.__create_df(raw_data, formatted_symbol)
                            if not df.empty:
                                return df
                            else:
                                logger.warning(f"Empty DataFrame created for {formatted_symbol}")
                            break
                        elif "series_error" in result:
                            logger.error(f"Series error received for {formatted_symbol}")
                            break
                    except Exception as e:
                        logger.error(f"Error receiving data for {formatted_symbol}: {e}")
                        break
                
                if message_count >= max_messages:
                    logger.warning(f"Reached maximum message limit for {formatted_symbol}")
                    
            except Exception as e:
                logger.error(f"Error in get_hist for {formatted_symbol} (attempt {attempt + 1}/{self.__max_retries}): {e}")
            finally:
                if hasattr(self, 'ws') and self.ws:
                    try:
                        self.ws.close()
                    except:
                        pass
            
            if attempt < self.__max_retries - 1:
                logger.info(f"Retrying in {self.__retry_delay} seconds...")
                time.sleep(self.__retry_delay)
                
        logger.error(f"All {self.__max_retries} attempts failed for {formatted_symbol}")
        return pd.DataFrame()

    def get_multiple_hist(
        self,
        symbols: List[str],
        exchange: str,  # Only used for folder naming, not for data fetch
        interval: Interval = Interval.in_daily,
        n_bars: int = 10,
        fut_contract: int = None,
        extended_session: bool = False
    ) -> Dict[str, pd.DataFrame]:
        """Get historical data for multiple symbols sequentially (avoiding threading issues)"""
        results = {}
        for symbol in symbols:
            logger.info(f"Processing symbol: {symbol}")
            df = self.get_hist(
                symbol=symbol,
                exchange=exchange,  # Only used for folder naming, not for data fetch
                interval=interval,
                n_bars=n_bars,
                fut_contract=fut_contract,
                extended_session=extended_session
            )
            results[symbol] = df
            # Add delay between requests to avoid rate limiting
            if len(symbols) > 1:
                time.sleep(2)
        return results

    def search_symbol(self, text: str, exchange: str = '', proxy: Dict[str, str] = None) -> List[Dict]:
        url = self.__search_url.format(text, exchange)
        logger.info(f"Searching for symbol: {text} on exchange: {exchange}")
        
        for attempt in range(self.__max_retries):
            try:
                resp = requests.get(url, proxies=proxy, timeout=10)
                if resp.status_code == 200:
                    symbols_list = json.loads(resp.text.replace('</em>', '').replace('<em>', ''))
                    logger.info(f"Found {len(symbols_list)} symbols")
                    return symbols_list
                else:
                    logger.error(f"Search failed with status code: {resp.status_code}")
            except Exception as e:
                logger.error(f"Search symbol attempt {attempt + 1}/{self.__max_retries} failed: {e}")
                if attempt < self.__max_retries - 1:
                    time.sleep(self.__retry_delay)
        logger.error(f"All {self.__max_retries} search attempts failed for {text}")
        return []

if __name__ == "__main__":
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # Create TvDatafeed instance (no proxies needed for testing)
    tv = TvDatafeed()

    # First, let's search for A to see available exchanges
    print("Searching for A...")
    search_results = tv.search_symbol("A")
    
    if search_results:
        print("\nAvailable A symbols:")
        for result in search_results[:5]:  # Show first 5 results
            print(f"Symbol: {result.get('symbol', 'N/A')}, "
                  f"Exchange: {result.get('exchange', 'N/A')}, "
                  f"Description: {result.get('description', 'N/A')}")
    
    # Try different exchanges for A
    exchanges_to_try = ["AMEX", "NYSE", "NASDAQ", "BATS", ""]
    
    for exchange in exchanges_to_try:
        print(f"\n{'='*50}")
        print(f"Trying A on exchange: {exchange if exchange else 'Auto-detect'}")
        print(f"{'='*50}")
        
        try:
            df = tv.get_hist(
                symbol="A",
                exchange=exchange if exchange else "AMEX",  # Default to AMEX for A
                interval=Interval.in_daily,
                n_bars=10,
                extended_session=False
            )
            
            if not df.empty:
                print(f"SUCCESS! Retrieved data for A on {exchange if exchange else 'AMEX'}:")
                print(df.head())
                print(f"Total rows: {len(df)}")
                break
            else:
                print(f"No data returned for A on {exchange}")
                
        except Exception as e:
            print(f"Error fetching A from {exchange}: {e}")
    else:
        print("\nFailed to fetch A data from all exchanges")