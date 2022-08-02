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
    def generate_sqlalchemy_connection_string(
        self,
        user_key: str = None,
        password_key: str = None,
        host_key: str = None,
        schema_key: str = None,
        db_type_key: str = None,
        db_lib_key: str = None,
        db_port_key: str = None,
    ) -> str:
        """Generates a fully functional SQLAlchemy connection string.

        Parameters:
            user_key: (str)
                The username key on credentials vault
            password_key: (str)
                The password key on credentials vault
            host_key: (str)
                The hostname key on credentials vault
            schema_key: (str)
                The schema key on credentials vault
            db_type_key: (str)
                The database type key on credentials vault
            db_lib_key: (str)
                The python driver key on credentials vault
            db_port_key: (str)
                The connection port key on credentials vault

        Returns:
            connection_string: (str)
                The fully functional connection string
        """
