import os
from airflow.models import Variable

def generate_profiles_by_environment() -> dict:
    AS400_PROFILE_001 = {
            "host":Variable.get("coh_hostname"),
            "username": Variable.get("coh_username"),
            "password": Variable.get("coh_password"),
        }