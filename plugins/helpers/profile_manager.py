from domain.enumerations.environment import Environment
from domain.enumerations.profile import Profile
from domain.exceptions.construction_exceptions import ProfileError
from domain.interfaces.credential_management import ICredentialManager
from infrastructure.credentials.airflow_credential_manager import AirflowCredentialManager


class ProfileManager:
    """Profiles management class."""

    profile_managers = {Profile.PROFILE_001: AirflowCredentialManager}

    @classmethod
    def get_profile(cls, profile: Profile, environment: Environment) -> ICredentialManager:
        """Returns the requested profile, configured for the requested environment.

        Parameters:
            profile: (Profile)
                The profile identifier

            environment: (Environment)
                The environment identifier

        Returns:
            requested_profile: (ICredentialManager)
                The requested profile

        Raises:
            ProfileError: Raised when requested profile doesn't have an associated configuration manager
        """
        try:
            return cls.profile_managers[profile](environment)
        except KeyError:
            raise ProfileError(profile)
