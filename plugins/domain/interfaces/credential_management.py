from abc import ABC, abstractmethod


class ICredentialManager(ABC):
    """Credential management interface."""

    @abstractmethod
    def get(self, name: str) -> str:
        """Returns the requested credential value.

        Parameters:
            name: (str)
                The credential name on target vault

        Returns:
            credential_value: (str)
                The credential value as raw string
        """

    @abstractmethod
    def get_user(self) -> str:
        """Returns the username credential.

        Returns:
            username: (str)
                The username value as raw string
        """

    @abstractmethod
    def get_pass(self) -> str:
        """Returns the password credential.

        Returns:
            password: (str)
                The password value as raw string
        """

    @abstractmethod
    def get_host(self) -> str:
        """Returns the host credential.

        Returns:
            host: (str)
                The host value as raw string
        """

    @abstractmethod
    def generate_sqlalchemy_connection_string(self) -> str:
        """Generates a fully functional SQLAlchemy connection string.

        Returns:
            connection_string: (str)
                The fully functional connection string
        """
