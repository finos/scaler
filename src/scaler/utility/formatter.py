STORAGE_SIZE_MODULUS = 1024.0
TIME_MODULUS = 1000


def format_bytes(number) -> str:
    for unit in ["B", "K", "M", "G", "T"]:
        if number >= STORAGE_SIZE_MODULUS:
            number /= STORAGE_SIZE_MODULUS
            continue

        if unit in {"B", "K"}:
            return f"{int(number)}{unit}"

        return f"{number:.1f}{unit}"

    raise ValueError("This should not happen")


def format_integer(number):
    return f"{number:,}"


def format_percentage(number: int):
    return f"{(number/1000):.1%}"


def format_microseconds(number: int) -> str:
    for unit in ["us", "ms", "s"]:
        # Seconds is the last unit, so it has to render whatever is left rather
        # than divide again and fall out of the loop returning None.
        if unit != "s" and number >= TIME_MODULUS:
            number = int(number / TIME_MODULUS)
            continue

        if unit == "us":
            return f"{number/TIME_MODULUS:.1f}ms"

        return f"{int(number)}{unit}"

    raise ValueError("This should not happen")


def format_seconds(number: int) -> str:
    if number < 60:
        return f"{number}s"

    minutes, seconds = divmod(number, 60)
    if minutes < 60:
        return f"{minutes}m{seconds}s" if seconds else f"{minutes}m"

    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes}m" if minutes else f"{hours}h"
