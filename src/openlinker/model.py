from __future__ import annotations

from dataclasses import MISSING, dataclass, field, fields, is_dataclass
from datetime import datetime
from typing import Any, ClassVar, TypeVar, Union, get_args, get_origin, get_type_hints


T = TypeVar("T", bound="Model")


def jfield(alias: str | None = None, default: Any = None, default_factory: Any = MISSING):
    metadata = {}
    if alias:
        metadata["json"] = alias
    if default_factory is not MISSING:
        return field(default_factory=default_factory, metadata=metadata)
    return field(default=default, metadata=metadata)


def _json_key(f) -> str:
    return f.metadata.get("json", f.name)


def _strip_optional(tp: Any) -> Any:
    origin = get_origin(tp)
    if origin is Union:
        args = [arg for arg in get_args(tp) if arg is not type(None)]
        if len(args) == 1:
            return args[0]
    return tp


def _from_value(tp: Any, value: Any) -> Any:
    if value is None:
        return None
    tp = _strip_optional(tp)
    origin = get_origin(tp)
    if origin in (list, tuple):
        args = get_args(tp)
        item_type = args[0] if args else Any
        if not isinstance(value, list):
            return value
        return [_from_value(item_type, item) for item in value]
    if origin is dict:
        return value
    if isinstance(tp, type) and issubclass(tp, Model) and isinstance(value, dict):
        return tp.from_dict(value)
    return value


def to_json_value(value: Any) -> Any:
    if isinstance(value, Model):
        return value.to_dict()
    if is_dataclass(value):
        return {
            _json_key(f): to_json_value(getattr(value, f.name))
            for f in fields(value)
            if getattr(value, f.name) is not None
        }
    if isinstance(value, list):
        return [to_json_value(item) for item in value]
    if isinstance(value, tuple):
        return [to_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: to_json_value(item) for key, item in value.items() if item is not None}
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    return value


@dataclass
class Model:
    extra: dict[str, Any] = jfield(default_factory=dict)

    _drop_empty_lists: ClassVar[bool] = True

    @classmethod
    def from_dict(cls: type[T], data: dict[str, Any] | None) -> T:
        if data is None:
            return cls()  # type: ignore[call-arg]
        kwargs: dict[str, Any] = {}
        consumed: set[str] = set()
        hints = get_type_hints(cls)
        for f in fields(cls):
            if f.name == "extra":
                continue
            key = _json_key(f)
            consumed.add(key)
            if key in data:
                kwargs[f.name] = _from_value(hints.get(f.name, f.type), data[key])
        obj = cls(**kwargs)  # type: ignore[arg-type]
        obj.extra.update({key: value for key, value in data.items() if key not in consumed})
        return obj

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for f in fields(self):
            if f.name == "extra":
                continue
            value = getattr(self, f.name)
            if value is None:
                continue
            if self._drop_empty_lists and value == []:
                continue
            out[_json_key(f)] = to_json_value(value)
        out.update(self.extra)
        return out


def maybe_model(value: Any, cls: type[T]) -> T:
    if isinstance(value, cls):
        return value
    if isinstance(value, dict):
        return cls.from_dict(value)
    raise TypeError(f"expected {cls.__name__} or dict")
