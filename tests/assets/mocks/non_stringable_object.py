class NonStringableObject:
    def __str__(self) -> str:
        raise ValueError("Unable to transform this object on a string")
