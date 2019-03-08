import logging
from jsonschema import Draft7Validator, validators
from jsonschema.exceptions import ValidationError

def extend_with_defaults(validator_class):

    validate_properties = validator_class.VALIDATORS['properties']

    def set_defaults(validator, properties, instance, schema):
        for property, subschema in properties.items():
            if 'default' in subschema:
                instance.setdefault(property, subschema['default'])
        for error in validate_properties(
            validator, properties, instance, schema,
        ):
            yield error

    return validators.extend(
        validator_class, {'properties': set_defaults},
    )

Validator = extend_with_defaults(Draft7Validator)

def ValidateWithDefaults(schema, instance):
    try:
        Validator(schema).validate(instance)
    except ValidationError as error:
        logging.error(error)
        return False
    return True
