
""" funzioni di utlità che possono usare tutti i componenti del progetto """

from datetime import datetime, UTC



def utc_now() -> datetime:
    """
        datetime corrente in UTC
    """
    return datetime.now(UTC)