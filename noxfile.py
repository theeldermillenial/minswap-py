from pathlib import Path

import nox_poetry

local = Path(__file__).parent.absolute()


@nox_poetry.session(python=["3.10"])
def build_blockfrost_classes(session):
    """Autogenerate the blockfrost models.

    This uses `datamodel-code-generator` to generate model classes from the Blockfrost
    OpenAPI specification.

    Args:
        session (_type_): _description_
    """
    # Install dependencies
    session.install("datamodel-code-generator", ".")
    session.install("requests", ".")

    # Download the latest OpenAPI schema from blockfrost/openapi/master
    import requests

    url = "https://raw.githubusercontent.com/blockfrost/openapi/master/openapi.yaml"
    source = local.joinpath("src/models/data/blockfrost/openapi.yaml")
    source.parent.mkdir(exist_ok=True, parents=True)
    response = requests.get(url=url)
    with open(source, "w") as fw:
        fw.write(response.text)

    destination = local.joinpath("src/minswap/models/data/blockfrost_models.py")
    session.run(
        "datamodel-codegen",
        "--input",
        str(source),
        "--input-file-type",
        "openapi",
        "--output",
        str(destination),
        "--use-schema-description",
        "--use-field-description",
        "--collapse-root-models",
        "--field-constraints",
        "--target-python-version",
        "3.10",
    )
