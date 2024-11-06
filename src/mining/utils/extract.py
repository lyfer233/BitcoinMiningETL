from airflow.models import Variable


async def fetch(session, url, api_alarm_threshold):
    async with session.get(url) as response:
        if response.status == 200:
            return await response.json()
        else:
            api_error_count = int(Variable.get('api_error_count', 0))
            if api_error_count >= api_alarm_threshold:
                Variable.set('api_error_count', 0)
                raise RuntimeError(f"Failed to fetch data from {url}, "
                                   f"exceeding threshold, "
                                   f"HTTP status code is {response.status}")
            else:
                Variable.set('api_error_count', api_error_count + 1)


def check_task_interval(now, next_time, interval, task_name):
    if now - next_time > interval:
        raise RuntimeError(f"{task_name} interval is "
                           f"too short to handle task.")
