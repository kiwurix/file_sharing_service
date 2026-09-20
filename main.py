from client import App
from single_instance import SingleInstanceGuard, focus_existing_window


if __name__ == "__main__":
    with SingleInstanceGuard() as instance:
        if not instance.is_primary:
            focus_existing_window()
        else:
            App().run()
