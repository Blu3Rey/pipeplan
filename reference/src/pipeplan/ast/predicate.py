"""Recursive predicate AST powering the ``filter`` transform.

A predicate is a JSON tree. Internal nodes are logical combinators and leaves
are column comparisons::

    {"AND": [
        {"status": {"op": "==", "value": "Active"}},
        {"OR": [
            {"score": {"op": ">=", "value": 90}},
            {"finish_time": {"op": "<=", "value": "10.00"}}
        ]}
    ]}

The tree is validated by Pydantic using a *callable discriminator* (Pydantic
v2.5+): the node type is inferred from the dict's keys, so no explicit ``type``
tag is needed in the blueprint. Every node exposes :meth:`evaluate`, which
returns a boolean ``pd.Series`` aligned to the input frame's index -- fully
vectorised, no per-row Python.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Any, Union

import pandas as pd
from pydantic import BaseModel, ConfigDict, Discriminator, Tag, model_validator

from ..core.exceptions import ExpressionError


class Operator(str, Enum):
    EQ = "=="
    NE = "!="
    GT = ">"
    GE = ">="
    LT = "<"
    LE = "<="
    IN = "in"
    NOT_IN = "not_in"
    CONTAINS = "contains"
    STARTSWITH = "startswith"
    ENDSWITH = "endswith"
    ISNULL = "isnull"
    NOTNULL = "notnull"
    BETWEEN = "between"


_LOGICAL_KEYS = {"AND", "OR", "NOT"}


def _coerce_scalar(series: pd.Series, value: Any) -> Any:
    """Best-effort coercion of a literal to the dtype of ``series``.

    Blueprints frequently express numeric thresholds as strings
    (``"value": "10.00"``). Comparing a float column against a raw string raises
    in pandas, so we align the literal to the column dtype when it is safe.
    """
    if value is None:
        return value
    dtype = series.dtype
    try:
        if pd.api.types.is_numeric_dtype(dtype) and not isinstance(value, (int, float)):
            return pd.to_numeric(value)
        if pd.api.types.is_datetime64_any_dtype(dtype):
            return pd.to_datetime(value)
    except (ValueError, TypeError):
        return value
    return value


class _Node(BaseModel):
    model_config = ConfigDict(extra="forbid")

    def evaluate(self, df: pd.DataFrame) -> pd.Series:  # pragma: no cover - abstract
        raise NotImplementedError


class CompareNode(_Node):
    column: str
    op: Operator
    value: Any = None

    @model_validator(mode="before")
    @classmethod
    def _unpack(cls, data: Any) -> Any:
        # Accept the natural ``{column: {op, value}}`` shape.
        if isinstance(data, dict) and "column" not in data:
            if len(data) != 1:
                raise ValueError(
                    f"a comparison must be a single {{column: {{op, value}}}} mapping, got {data!r}"
                )
            (column, spec), = data.items()
            if not isinstance(spec, dict):
                raise ValueError(f"comparison for column '{column}' must be a mapping, got {spec!r}")
            return {"column": column, **spec}
        return data

    def evaluate(self, df: pd.DataFrame) -> pd.Series:
        if self.column not in df.columns:
            raise ExpressionError(
                f"filter references unknown column '{self.column}'; "
                f"available columns: {list(df.columns)}"
            )
        series = df[self.column]
        op = self.op

        if op is Operator.ISNULL:
            return series.isna()
        if op is Operator.NOTNULL:
            return series.notna()
        if op is Operator.IN:
            return series.isin(self._as_list())
        if op is Operator.NOT_IN:
            return ~series.isin(self._as_list())
        if op is Operator.BETWEEN:
            lo, hi = self._as_pair()
            return series.between(_coerce_scalar(series, lo), _coerce_scalar(series, hi))
        if op in (Operator.CONTAINS, Operator.STARTSWITH, Operator.ENDSWITH):
            text = series.astype("string")
            if op is Operator.CONTAINS:
                return text.str.contains(str(self.value), na=False, regex=False)
            if op is Operator.STARTSWITH:
                return text.str.startswith(str(self.value), na=False)
            return text.str.endswith(str(self.value), na=False)

        value = _coerce_scalar(series, self.value)
        comparators = {
            Operator.EQ: series.eq,
            Operator.NE: series.ne,
            Operator.GT: series.gt,
            Operator.GE: series.ge,
            Operator.LT: series.lt,
            Operator.LE: series.le,
        }
        return comparators[op](value)

    def _as_list(self) -> list[Any]:
        if not isinstance(self.value, (list, tuple)):
            raise ExpressionError(f"operator '{self.op.value}' requires a list value")
        return list(self.value)

    def _as_pair(self) -> tuple[Any, Any]:
        values = self._as_list()
        if len(values) != 2:
            raise ExpressionError("operator 'between' requires exactly two values [low, high]")
        return values[0], values[1]


class AndNode(_Node):
    operands: list["PredicateNode"]

    @model_validator(mode="before")
    @classmethod
    def _unpack(cls, data: Any) -> Any:
        if isinstance(data, dict) and "operands" not in data and "AND" in data:
            return {"operands": data["AND"]}
        return data

    def evaluate(self, df: pd.DataFrame) -> pd.Series:
        mask = pd.Series(True, index=df.index)
        for operand in self.operands:
            mask &= operand.evaluate(df)
        return mask


class OrNode(_Node):
    operands: list["PredicateNode"]

    @model_validator(mode="before")
    @classmethod
    def _unpack(cls, data: Any) -> Any:
        if isinstance(data, dict) and "operands" not in data and "OR" in data:
            return {"operands": data["OR"]}
        return data

    def evaluate(self, df: pd.DataFrame) -> pd.Series:
        mask = pd.Series(False, index=df.index)
        for operand in self.operands:
            mask |= operand.evaluate(df)
        return mask


class NotNode(_Node):
    operand: "PredicateNode"

    @model_validator(mode="before")
    @classmethod
    def _unpack(cls, data: Any) -> Any:
        if isinstance(data, dict) and "operand" not in data and "NOT" in data:
            return {"operand": data["NOT"]}
        return data

    def evaluate(self, df: pd.DataFrame) -> pd.Series:
        return ~self.operand.evaluate(df)


def _discriminate(value: Any) -> str:
    """Infer the node tag from raw JSON keys (or a constructed instance)."""
    if isinstance(value, _Node):
        return {
            AndNode: "and",
            OrNode: "or",
            NotNode: "not",
            CompareNode: "compare",
        }[type(value)]
    if isinstance(value, dict):
        if "AND" in value:
            return "and"
        if "OR" in value:
            return "or"
        if "NOT" in value:
            return "not"
        return "compare"
    raise ExpressionError(f"cannot interpret predicate node: {value!r}")


PredicateNode = Annotated[
    Union[
        Annotated[AndNode, Tag("and")],
        Annotated[OrNode, Tag("or")],
        Annotated[NotNode, Tag("not")],
        Annotated[CompareNode, Tag("compare")],
    ],
    Discriminator(_discriminate),
]

# Resolve the forward references now that the union alias exists.
for _model in (AndNode, OrNode, NotNode):
    _model.model_rebuild()


def parse_predicate(raw: Any) -> "PredicateNode":
    """Validate a raw mapping into a typed, evaluatable predicate tree."""
    from pydantic import TypeAdapter

    try:
        return TypeAdapter(PredicateNode).validate_python(raw)
    except Exception as exc:
        raise ExpressionError(f"invalid filter predicate: {exc}") from exc
