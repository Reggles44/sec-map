import httpx
from aiolimiter import AsyncLimiter


HTTPX_CLIENT = httpx.AsyncClient(timeout=5)
SEC_RATE_LIMITER = AsyncLimiter(5, 1)


async def get(url):
    async with SEC_RATE_LIMITER:
        try:
            response = await HTTPX_CLIENT.get(url, headers={'User-Agent': 'Company Name myname@company.com'})
            response.raise_for_status()
            return response
        except httpx.TimeoutException:
            pass