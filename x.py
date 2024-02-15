class MyClass:

    def __init__(self):
        self.name = "Example"

    def non_dynamic_method(self):
        print("This is a non-dynamic method")

    def __getattr__(self, name):
        if name.startswith('dynamic_'):

            def method(*args, **kwargs):
                print(
                    f"Dynamic method called: {name}, with args: {args} and kwargs: {kwargs}"
                )

            return method
        else:
            raise AttributeError(
                f"'{self.__class__.__name__}' object has no attribute '{name}'")


# Usage
obj = MyClass()
obj.non_dynamic_method()  # Calls the explicitly defined method
obj.dynamic_method_1(
    1, 2, a=3)  # Dynamically created method, adjust prefix as needed
