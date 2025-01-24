import kopf

# In a simple scenario, you can directly call kopf.run(...)
# but since we have a multi-file structure, we import the handlers from tuskr_controller.


def main():
    kopf.configure(verbose=True)
    kopf.run()


if __name__ == "__main__":
    main()
