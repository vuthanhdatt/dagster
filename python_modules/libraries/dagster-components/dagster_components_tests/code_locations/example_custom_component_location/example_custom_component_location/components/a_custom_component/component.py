from dagster import Definitions
from dagster._core.definitions.asset_spec import AssetSpec
from dagster_components import ComponentLoadContext, component
from dagster_components.core.component_decl_builder import ComponentDeclNode, YamlComponentDecl
from dagster_components.lib.custom_component import CustomComponent
from pydantic import BaseModel, TypeAdapter


class ACustomComponentParams(BaseModel): ...


@component(name="a_custom_component")
class ACustomComponent(CustomComponent):
    """Write a description of your component here."""

    params_schema = ACustomComponentParams

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # return Definitions()
        # the line above was initially generated
        return Definitions(assets=[AssetSpec("asset_key")])

    @classmethod
    def from_decl_node(
        cls, context: "ComponentLoadContext", decl_node: "ComponentDeclNode"
    ) -> "ACustomComponent":
        assert isinstance(decl_node, YamlComponentDecl)
        loaded_params = TypeAdapter(cls.params_schema).validate_python(
            decl_node.component_file_model.params
        )
        assert loaded_params  # silence linter complaints
        return ACustomComponent()
