import json
import sys


def update_one_test(test):
    if (
        not test["result"]["valid"]
        or "explain" not in test["result"]
        or "query" in test["result"]["explain"]
    ):
        return

    test["result"]["explain"] = {"query": test["result"]["explain"]}


def main():
    for file in sys.argv[1:]:
        print(f"Updating {file}")
        with open(file, "r") as f:
            data = json.load(f)

        for test in data["tests"]:
            update_one_test(test)

        with open(file, "w") as f:
            json.dump(data, f, indent=2)


if __name__ == "__main__":
    main()
