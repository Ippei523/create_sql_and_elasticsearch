from elastic_search import Index


def main():
    index = Index()
    print("es: ", index.es)

    exec_method = input("select method: ")
    index.enter_key(exec_method)


if __name__ == "__main__":
    main()
