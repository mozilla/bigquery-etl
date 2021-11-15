import click


@click.command()
@click.option("--arg", default="test")
def main(arg):
    print(arg)


if __name__ == "__main__":
    main()
