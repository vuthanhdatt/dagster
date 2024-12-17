import pkgutil
from importlib import import_module
from types import ModuleType
from typing import Any, Iterable, Iterator, Mapping, Tuple, Type, Union

from dagster._core.definitions.asset_key import AssetCheckKey, AssetKey
from dagster._core.definitions.asset_spec import AssetSpec
from dagster._core.definitions.assets import AssetsDefinition
from dagster._core.definitions.cacheable_assets import CacheableAssetsDefinition
from dagster._core.definitions.executor_definition import ExecutorDefinition
from dagster._core.definitions.job_definition import JobDefinition
from dagster._core.definitions.logger_definition import LoggerDefinition
from dagster._core.definitions.partitioned_schedule import (
    UnresolvedPartitionedAssetScheduleDefinition,
)
from dagster._core.definitions.schedule_definition import ScheduleDefinition
from dagster._core.definitions.sensor_definition import SensorDefinition
from dagster._core.definitions.source_asset import SourceAsset
from dagster._core.definitions.unresolved_asset_job_definition import UnresolvedAssetJobDefinition
from dagster._core.executor.base import Executor

LoadableAssetObject = Union[AssetsDefinition, AssetSpec, SourceAsset, CacheableAssetsDefinition]
ScheduleDefinitionObject = Union[ScheduleDefinition, UnresolvedPartitionedAssetScheduleDefinition]
JobDefinitionObject = Union[JobDefinition, UnresolvedAssetJobDefinition]
ExecutorObject = Union[ExecutorDefinition, Executor]
LoggerDefinitionKeyMapping = Mapping[str, LoggerDefinition]
ResourceDefinitionMapping = Mapping[str, Any]
LoadableDagsterObject = Union[
    LoadableAssetObject,
    SensorDefinition,
    ScheduleDefinitionObject,
    JobDefinitionObject,
    ExecutorObject,
]
RuntimeKeyScopedAssetObjectTypes = (AssetsDefinition, AssetSpec, SourceAsset)
RuntimeAssetObjectTypes = (AssetsDefinition, AssetSpec, SourceAsset, CacheableAssetsDefinition)
RuntimeScheduleObjectTypes = (ScheduleDefinition, UnresolvedPartitionedAssetScheduleDefinition)
RuntimeJobObjectTypes = (JobDefinition, UnresolvedAssetJobDefinition)
RuntimeExecutorObjectTypes = (ExecutorDefinition, Executor)
RuntimeDagsterObjectTypes = (
    *RuntimeAssetObjectTypes,
    SensorDefinition,
    *RuntimeScheduleObjectTypes,
    *RuntimeJobObjectTypes,
    *RuntimeExecutorObjectTypes,
)


def find_objects_in_module_of_types(
    module: ModuleType,
    types: Union[Type, Tuple[Type, ...]],
) -> Iterator:
    """Yields instances or subclasses of the given type(s)."""
    for attr in dir(module):
        value = getattr(module, attr)
        if isinstance(value, types):
            yield value
        elif isinstance(value, list) and all(isinstance(el, types) for el in value):
            yield from value


def find_subclasses_in_module(
    module: ModuleType,
    types: Union[Type, Tuple[Type, ...]],
) -> Iterator:
    """Yields instances or subclasses of the given type(s)."""
    for attr in dir(module):
        value = getattr(module, attr)
        if isinstance(value, type) and issubclass(value, types):
            yield value


def find_modules_in_package(package_module: ModuleType) -> Iterable[ModuleType]:
    yield package_module
    if package_module.__file__:
        for _, modname, is_pkg in pkgutil.walk_packages(
            package_module.__path__, prefix=package_module.__name__ + "."
        ):
            submodule = import_module(modname)
            if is_pkg:
                yield from find_modules_in_package(submodule)
            else:
                yield submodule
    else:
        raise ValueError(
            f"Tried to find modules in package {package_module}, but its __file__ is None"
        )


def replace_keys_in_asset(
    asset: Union[AssetsDefinition, AssetSpec, SourceAsset],
    key_replacements: Mapping[AssetKey, AssetKey],
    check_key_replacements: Mapping[AssetCheckKey, AssetCheckKey],
) -> Union[AssetsDefinition, AssetSpec, SourceAsset]:
    if isinstance(asset, SourceAsset):
        return asset.with_attributes(key=key_replacements.get(asset.key, asset.key))
    if isinstance(asset, AssetSpec):
        return asset.replace_attributes(
            key=key_replacements.get(asset.key, asset.key),
        )
    else:
        updated_object = asset.with_attributes(
            output_asset_key_replacements={
                key: key_replacements.get(key, key) for key in asset.keys
            },
            output_check_key_replacements={
                key: check_key_replacements.get(key, key) for key in asset.check_keys
            },
            input_asset_key_replacements={
                key: key_replacements.get(key, key) for key in asset.keys_by_input_name.values()
            },
        )
        return updated_object
