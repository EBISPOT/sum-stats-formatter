from format.utils import *


def main():
    print("\n")
    print("Showing known valid column headers...")
    print("\n")
    for header in VALID_INPUT_HEADERS:
        print(header)
    print("\n")
    print("Showing desired headers...")
    print("\n")
    for header in DESIRED_HEADERS:
        print(header)
    print("\n")


if __name__ == "__main__":
    main()