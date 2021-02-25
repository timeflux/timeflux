import logging
import json
import pathlib
import os
from jsonschema import Draft7Validator, validators, RefResolver
from jsonschema.exceptions import ValidationError

LOGGER = logging.getLogger(__name__)
RESOLVER = None


def extend_with_defaults(validator_class):

    """Extends the validator class to set defaults automatically."""

    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for property, subschema in properties.items():
            if "default" in subschema:
                instance.setdefault(property, subschema["default"])
        for error in validate_properties(
            validator,
            properties,
            instance,
            schema,
        ):
            yield error

    return validators.extend(
        validator_class,
        {"properties": set_defaults},
    )


Validator = extend_with_defaults(Draft7Validator)


def resolver():
    """Load the schema and returns a resolver."""
    if RESOLVER:
        return RESOLVER
    path = str(pathlib.Path(__file__).parents[1].joinpath("schema", "app.json"))
    with open(path) as stream:
        schema = json.load(stream)
    globals()["RESOLVER"] = RefResolver(
        "https://schema.timeflux.io/app.json", None
    ).from_schema(schema)
    return RESOLVER


def validate(instance, definition="app"):
    """Validate a Timeflux application or a graph.

    Args:
        instance (dict): The application to validate.
        definition (string): The subschema to validate against.

    """
    schema = {"$ref": "#/definitions/" + definition}
    validator = Validator(schema, resolver=resolver())
    errors = sorted(validator.iter_errors(instance), key=lambda e: e.path)
    if errors:
        for error in errors:
            path = "/".join(str(e) for e in error.path)
            LOGGER.error("%s (%s)" % (error.message, path))
        raise ValueError("Validation failed")
