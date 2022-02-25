"""Limit imported Stripe data to a set of allowed fields."""

import re
from hashlib import sha256
from pathlib import Path
from typing import Any, List, Tuple, Type

import click
import stripe
import ujson
import yaml
from google.cloud import bigquery
from stripe.api_resources.abstract import ListableAPIResource


def _snake_case(resource: Type[ListableAPIResource]) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", resource.__name__).lower()


# event data types with separate events and a defined schema
ALLOWED_FIELDS = yaml.safe_load(
    (Path(__file__).parent / "allowed_fields.yaml").read_text()
)


def _valid_float(value: str):
    try:
        float(value)
    except ValueError:
        return False
    else:
        return True


def get_rooted_schema(type_) -> bigquery.SchemaField:
    """Load schema for given stripe type from json."""
    path = Path(__file__).parent / f"{type_}.schema.json"
    return bigquery.SchemaField.from_api_repr(
        {"name": "root", "type": "RECORD", "fields": ujson.loads(path.read_text())}
    )


class FilteredSchema:
    """Apply ALLOWED_FIELDS to resources and their schema."""

    type: str
    allowed: dict
    root: bigquery.SchemaField
    filtered: Tuple[bigquery.SchemaField, ...]

    def __init__(self, resource: Type[ListableAPIResource]):
        """Get filtered schema and allowed fields for a Stripe resource type."""
        self.type = _snake_case(resource)
        self.allowed = ALLOWED_FIELDS[self.type]
        self.root = get_rooted_schema(self.type)
        self.filtered = self._filter_schema(self.root.fields, self.allowed)

    def _filter_schema(
        self, fields: Tuple[bigquery.SchemaField], allowed: dict
    ) -> Tuple[bigquery.SchemaField, ...]:
        return tuple(
            bigquery.SchemaField(
                **{
                    ("field_type" if key == "type" else key): self._filter_schema(
                        field.fields, allowed[field.name]
                    )
                    if key == "fields"
                    else value
                    for key, value in field.to_api_repr().items()
                }
            )
            if field.field_type == "RECORD"
            else field
            for field in fields
            if field.name in allowed
        )

    @staticmethod
    def expand(obj: Any) -> Any:
        """Recursively expand paged lists provided by stripe."""
        if isinstance(obj, stripe.ListObject):
            if obj.data and _snake_case(type(obj.data[0])) in ALLOWED_FIELDS:
                # don't expand lists of resources that get updated in separate events
                return []
            # expand paged list
            return list(map(FilteredSchema.expand, obj.auto_paging_iter()))
        if isinstance(obj, list):
            return list(map(FilteredSchema.expand, obj))
        if isinstance(obj, dict):
            return {key: FilteredSchema.expand(value) for key, value in obj.items()}
        return obj

    def format_row(self, row: Any, strict: bool) -> Any:
        """Format stripe object for BigQuery, and validate against original schema."""
        return self._format_helper(row, self.allowed, self.root, (self.type,), strict)

    def _hash_kv_userid(self, arr: List[dict], key_name: str) -> List[dict]:
        """Hash a userid key if it exists in a key-value list."""
        res = []
        for field in arr:
            key, value = field[key_name], field["value"]
            if key == "userid":
                # hash fxa uid before it reaches BigQuery
                key = "fxa_uid"
                value = sha256(value.encode("UTF-8")).hexdigest()
            res.append({key_name: key, "value": value})
        return res

    def _format_helper(
        self,
        obj: Any,
        allowed: dict,
        field: bigquery.SchemaField,
        path: Tuple[str, ...],
        strict: bool,
        is_list_item: bool = False,
    ) -> Any:
        if path[-1] == "metadata":
            # format metadata as a key-value list
            obj = self._hash_kv_userid(
                [{"key": key, "value": value} for key, value in obj.items()], "key"
            )
        elif path[-1] == "custom_fields":
            # NOTE: We treat the custom field like metadata, but does the
            # invoice custom field contain the userid?
            # https://stripe.com/docs/api/invoices/create#create_invoice-custom_fields
            obj = self._hash_kv_userid(obj, "name")
        if isinstance(obj, list):
            # enforce schema
            if field.mode != "REPEATED":
                raise click.ClickException(
                    f"expected {field.field_type} at {'.'.join(path)} but got ARRAY"
                )
            return [
                self._format_helper(
                    obj=e,
                    allowed=allowed,
                    field=field,
                    path=(*path[:-1], f"{path[-1]}[{i}]"),
                    strict=strict,
                    is_list_item=True,
                )
                for i, e in enumerate(obj)
            ]
        if isinstance(obj, dict):
            # enforce schema
            if field.mode == "REPEATED" and not is_list_item:
                raise click.ClickException(
                    f"expected ARRAY at {'.'.join(path)} but got RECORD"
                )
            if field.field_type != "RECORD":
                raise click.ClickException(
                    f"expected {field.field_type} at {'.'.join(path)} but got RECORD"
                )
            # recursively format and keep allowed non-empty values
            fields_by_name = {f.name: f for f in field.fields}
            result = {}
            for key, value in obj.items():
                if value in (None, [], {}):
                    continue  # drop empty values without checking schema
                if key == "use_stripe_sdk":
                    # drop use_stripe_sdk because the contents are only for use in Stripe.js
                    # https://stripe.com/docs/api/payment_intents/object#payment_intent_object-next_action-use_stripe_sdk
                    continue
                if key not in fields_by_name:
                    if strict:
                        # enforce schema
                        raise click.ClickException(
                            f"{'.'.join(path)} contained unexpected field: {key}"
                        )
                    # skip unexpected field
                    continue
                formatted = self._format_helper(
                    obj=value,
                    allowed=allowed.get(key) or {},
                    field=fields_by_name[key],
                    path=(*path, key),
                    strict=strict,
                )
                # apply allow list after formatting to enforce schema
                if formatted not in (None, [], {}) and key in allowed:
                    result[key] = formatted
            return result
        # enforce schema
        if field.mode == "REPEATED" and not is_list_item:
            raise click.ClickException(
                f"expected ARRAY at {'.'.join(path)} but got {obj!r}"
            )
        if field.field_type == "RECORD":
            raise click.ClickException(
                f"expected RECORD at {'.'.join(path)} but got {obj!r}"
            )
        if not (
            # STRING can be any primitive type
            field.field_type == "STRING"
            # TIMESTAMP can be int or string
            or (field.field_type == "TIMESTAMP" and isinstance(obj, (int, str)))
            # INT64 can be int or digit string
            or (
                field.field_type in ("INT64", "INTEGER")
                and (isinstance(obj, int) or (isinstance(obj, str) and obj.isdecimal()))
            )
            # FLOAT64 can be int, float or decimal string
            or (
                field.field_type in ("FLOAT64", "FLOAT", "NUMERIC")
                and (
                    isinstance(obj, (int, float))
                    or (isinstance(obj, str) and _valid_float(obj))
                )
            )
            # BOOL must be bool
            or (field.field_type in ("BOOL", "BOOLEAN") and isinstance(obj, bool))
        ):
            raise click.ClickException(
                f"expected {field.field_type} at {'.'.join(path)} but got {obj!r}"
            )
        return obj
