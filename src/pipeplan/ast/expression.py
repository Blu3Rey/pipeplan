"""Recursive expression AST powering the ``derive`` (compute) transform.

Expressions are JSON trees that evaluate to a column (``pd.Series``) or scalar::

    {"+": [{"col": "subtotal"}, {"col": "tax"}]}
    {"fn": "round", "args": [{"col": "ratio"}, {"lit": 2}]}

Leaves are ``{"col": name}`` (a column reference) or ``{"lit": value}`` (a
constant). Arithmetic nodes key on the operator symbol. Function nodes dispatch
to the expression registry, which is extensible via the ``pipeplan.expressions``
entry-point group. Evaluation is vectorised throughout.
"""

from __future__ import annotations

from typing import Annotated, Any, Union

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Discriminator, Tag, model_validator

from ..core.exceptions import ExpressionError
from ..core.registry import EXPRESSIONS, register_expression

_ARITHMETIC = {
    "+": lambda a, b: a + b,
    "-": lambda a, b: a - b,
    "*": lambda a, b: a * b,
    "/": lambda a, b: a / b,
    "%": lambda a, b: a % b,
}


class _Expr(BaseModel):
    model_config = ConfigDict(extra="forbid")

    def evaluate(self, df: pd.DataFrame) -> Any:  # pragma: no cover - abstract
        raise NotImplementedError


class ColExpr(_Expr):
    col: str

    def evaluate(self, df: pd.DataFrame) -> pd.Series:
        if self.col not in df.columns:
            raise ExpressionError(
                f"expression references unknown column '{self.col}'; "
                f"available columns: {list(df.columns)}"
            )
        return df[self.col]


class LitExpr(_Expr):
    lit: Any

    def evaluate(self, df: pd.DataFrame) -> Any:
        return self.lit


class BinOpExpr(_Expr):
    op: str
    operands: list["ExpressionNode"]

    @model_validator(mode="before")
    @classmethod
    def _unpack(cls, data: Any) -> Any:
        if isinstance(data, dict) and "op" not in data:
            present = [key for key in data if key in _ARITHMETIC]
            if len(present) == 1:
                op = present[0]
                return {"op": op, "operands": data[op]}
        return data

    def evaluate(self, df: pd.DataFrame) -> Any:
        if len(self.operands) < 2:
            raise ExpressionError(f"operator '{self.op}' needs at least two operands")
        func = _ARITHMETIC[self.op]
        result = self.operands[0].evaluate(df)
        for operand in self.operands[1:]:
            result = func(result, operand.evaluate(df))
        return result


class FnExpr(_Expr):
    fn: str
    args: list["ExpressionNode"] = []

    def evaluate(self, df: pd.DataFrame) -> Any:
        func = EXPRESSIONS.get(self.fn)
        evaluated = [arg.evaluate(df) for arg in self.args]
        return func(*evaluated)


def _discriminate(value: Any) -> str:
    if isinstance(value, _Expr):
        return {ColExpr: "col", LitExpr: "lit", FnExpr: "fn", BinOpExpr: "binop"}[type(value)]
    if isinstance(value, dict):
        if "col" in value:
            return "col"
        if "lit" in value:
            return "lit"
        if "fn" in value:
            return "fn"
        if any(key in _ARITHMETIC for key in value):
            return "binop"
    raise ExpressionError(f"cannot interpret expression node: {value!r}")


ExpressionNode = Annotated[
    Union[
        Annotated[ColExpr, Tag("col")],
        Annotated[LitExpr, Tag("lit")],
        Annotated[FnExpr, Tag("fn")],
        Annotated[BinOpExpr, Tag("binop")],
    ],
    Discriminator(_discriminate),
]

for _model in (BinOpExpr, FnExpr):
    _model.model_rebuild()


def parse_expression(raw: Any) -> "ExpressionNode":
    from pydantic import TypeAdapter

    try:
        return TypeAdapter(ExpressionNode).validate_python(raw)
    except Exception as exc:
        raise ExpressionError(f"invalid compute expression: {exc}") from exc


# --------------------------------------------------------------------------- #
# Built-in, vectorised expression functions
# --------------------------------------------------------------------------- #


@register_expression("round")
def _round(value: Any, ndigits: Any = 0) -> Any:
    return np.round(value, int(ndigits))


@register_expression("abs")
def _abs(value: Any) -> Any:
    return np.abs(value)


@register_expression("coalesce")
def _coalesce(*values: Any) -> Any:
    if not values:
        raise ExpressionError("coalesce requires at least one argument")
    result = values[0]
    if isinstance(result, pd.Series):
        result = result.copy()
        for other in values[1:]:
            result = result.where(result.notna(), other)
        return result
    return result if result is not None else _coalesce(*values[1:]) if len(values) > 1 else result


@register_expression("concat")
def _concat(*values: Any) -> Any:
    parts = [v.astype("string") if isinstance(v, pd.Series) else str(v) for v in values]
    result = parts[0]
    for part in parts[1:]:
        result = result + part
    return result


@register_expression("lower")
def _lower(value: Any) -> Any:
    return value.str.lower() if isinstance(value, pd.Series) else str(value).lower()


@register_expression("upper")
def _upper(value: Any) -> Any:
    return value.str.upper() if isinstance(value, pd.Series) else str(value).upper()


@register_expression("length")
def _length(value: Any) -> Any:
    return value.astype("string").str.len() if isinstance(value, pd.Series) else len(str(value))
