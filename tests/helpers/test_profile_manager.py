import pytest
from pytest import mark

from domain.enumerations.environment import Environment
from domain.enumerations.profile import Profile
from domain.exceptions.construction_exceptions import ProfileError
from domain.interfaces.credential_management import ICredentialManager
from helpers.profile_manager import ProfileManager
from infrastructure.credentials.airflow_credential_manager import AirflowCredentialManager


@mark.profile_manager
class ProfileManagerTests:
    def test_profile_manager_instance(self, profile_manager: ProfileManager):

        assert isinstance(profile_manager, ProfileManager)

        assert getattr(profile_manager, "profile_managers")
        assert getattr(profile_manager, "get_profile")

        assert isinstance(profile_manager.profile_managers, dict)

        assert all([isinstance(profile, Profile) for profile in profile_manager.profile_managers.keys()])
        print(profile_manager.profile_managers.values())
        assert all([issubclass(manager, ICredentialManager) for manager in profile_manager.profile_managers.values()])

    @mark.parametrize("profile_info", [dict(profile=Profile.PROFILE_001, manager=AirflowCredentialManager)])
    def test_profile_manager_known_profiles(
        self, dev_environment: Environment, profile_manager: ProfileManager, profile_info: dict
    ):

        profile = profile_manager.get_profile(profile_info["profile"], dev_environment)

        assert isinstance(profile, profile_info["manager"])

    def test_profile_manager_unknown_profile_error(self, dev_environment: Environment, profile_manager: ProfileManager):

        unknown_profile = 9999999999999999

        with pytest.raises(ProfileError):
            profile_manager.get_profile(unknown_profile, dev_environment)
