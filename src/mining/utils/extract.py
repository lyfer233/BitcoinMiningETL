import logging
import aiohttp
import asyncio


async def fetch_data_from_api(url: str, timeout: int = 5):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=timeout) as response:
                response.raise_for_status()
                data = await response.json()
                return data
    except aiohttp.ClientConnectionError:
        logging.error("Connection error, Check current network!")
    except asyncio.TimeoutError:
        logging.error("Request timeout~")
    except aiohttp.ClientResponseError as e:
        logging.error(f"HTTP request failed, status code is {e.status}")
    except Exception as e:
        logging.error(f"Request has an exception: {e}")
