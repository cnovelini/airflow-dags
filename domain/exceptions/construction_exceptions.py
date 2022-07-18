from airflow import AirflowException

from domain.enumerations.profile import Profile


class ProfileError(AirflowException):
    def __init__(self, profile: Profile) -> None:
        message = f"Nonexistent credential manager for profile {profile}. Please verify."
        super().__init__(message)
