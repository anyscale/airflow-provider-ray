

def wrap_all_methods_with_counter(decorator):
    """Wraps all methods of the class it decorates with specified `decorator`.
    """
    def decorate(cls):
        for attr in cls.__dict__:  # there's propably a better way to do this
            if callable(getattr(cls, attr)):
                setattr(cls, attr, decorator(getattr(cls, attr)))
        return cls
    return decorate


def call_counter(f):
    """Decorator to count number of method calls. Used for testing.
    """
    from functools import wraps

    @wraps(f)
    def wrapper(self, *args, **kwargs):

        if not hasattr(self, 'n_called'):
            self.n_called = {}

        if f.__name__ in self.n_called:
            self.n_called[f.__name__] += 1
        else:
            self.n_called[f.__name__] = 1

        return f(self, *args, **kwargs)

    wrapper.calls = 0
    wrapper.__name__ = f.__name__
    return wrapper
