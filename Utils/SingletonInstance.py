class SingleTonInstance:
    __instance = None

    @classmethod
    def __getinstance(cls):
        return cls.__instance

    @classmethod
    def instance(cls, *args, **kargs):
        cls.__instance = cls(*args, **kargs)
        cls.instance = cls.__getinstance
        return cls.__instance