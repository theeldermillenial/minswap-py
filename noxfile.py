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
    session.install("datamodel-code-generator", ".")
    source = local.joinpath("data/openapi.json")
    destination = local.joinpath("src/minswap/models/blockfrost.py")
    destination.parent.mkdir(exist_ok=True, parents=True)
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
