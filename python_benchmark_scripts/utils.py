"""Generate random string."""

import secrets
import string


def generate_random_string(number: int) -> str:
    """Generate a random string of length `number`."""
    # Choose characters from uppercase letters, lowercase letters and
    # digits
    characters = string.ascii_letters + string.digits
    # Generate the random string
    iterable = (secrets.choice(characters) for _ in range(number))
    return "".join(iterable)
