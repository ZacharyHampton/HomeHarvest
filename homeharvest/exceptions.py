class InvalidListingType(Exception):
    """Raised when a provided listing type is does not exist."""


class InvalidDate(Exception):
    """Raised when only one of date_from or date_to is provided or not in the correct format. ex: 2023-10-23"""


class AuthenticationError(Exception):
    """Raised when there is an issue with the authentication process (e.g. a 403 Forbidden)."""
    def __init__(self, *args, response):
        super().__init__(*args)

        self.response = response


class RateLimitError(Exception):
    """Raised when the data source keeps answering 429 Too Many Requests after all retries."""
    def __init__(self, *args, response=None):
        super().__init__(*args)

        self.response = response


class HomeHarvestWarning(UserWarning):
    """Warning emitted when results are incomplete (a page or extra details could not be fetched)."""
