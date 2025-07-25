# USM Stage Importer Framework

A modular, enterprise-grade framework for building staging-to-production import scripts in USM.

This framework provides a structured and reusable way to process data from staging tables into target business objects. It is designed to handle complex mapping logic, relationships, and reconciliation with a clear separation of concerns.

## Core Concepts

The framework is built around a few key components:

*   **`ImportOrchestrator`**: The main entry point for running an import. It coordinates one or more `factories` and optional `reconcilers`.
*   **`MappingProcessor`**: Defines the field-level mapping from a staging record to a target business object. You create subclasses of `MappingProcessor` to implement your specific import logic.
*   **`RelationProcessor`**: A specialized processor for creating relationships between existing business objects.
*   **`StagingRepository`**: A simple component that fetches records from a specified staging table.
*   **`Reconciler`**: An optional component that can deactivate target records that are no longer present in the source data (mark-and-sweep reconciliation).

## Getting Started

To use the framework, you typically need to:

1.  **Define a `MappingProcessor`**: Create a class that inherits from `MappingProcessor` and define the fields to be mapped using `PlainField`, `StaticField`, or `RelationField`.
2.  **Configure a `MappingProcessorFactory`**: Instantiate a factory, telling it which staging table to read from, which processor to use, and how to identify target objects.
3.  **Run the `ImportOrchestrator`**: Create an orchestrator, pass it your factory, and call the `run()` method.

## Examples

### Simple Import

This is a "hello world" example demonstrating a basic record import with reconciliation.

```python
# 1. Define the Processor
class SystemProcessor(MappingProcessor):
    """A simple processor for mapping staging data to the System BOType."""
    __processing_order__ = ('systemname', 'name', 'description', 'status')
    
    systemname = PlainField(source_field="CI_NR")
    name = PlainField(source_field="NAME")
    description = PlainField(source_field="DESCRIPTION")
    status = StaticField(value="ACTIVE")

# 2. Configure and Run the Orchestrator
def main()
    factory = MappingProcessorFactory(
        repository=StagingRepository("StagingSystem"),
        processor_class=SystemProcessor,
        target_bo_name="System",
        key_field="PK",
        target_key_field="xSourcePk"
    )

    baseline = ReconciliationBaseline(
        botype_name="System",
        key_field="xSourcePk",
        condition="status == 'ACTIVE'"
    )

    reconciler = Reconciler(baseline=baseline)

    orchestrator = ImportOrchestrator(
        factories=[factory],
        reconcilers=[reconciler]
    )
    
    orchestrator.run(commit_batch_size=100)
main()
```

### Relationship Import

This example shows how to link two existing business objects together.

```python
# 1. Define the Relationship Processor
class SystemToComponentProcessor(RelationshipProcessor):
    """Links a Component to a System."""
    rel_attr_name = "compsystems"

# 2. Define the Relationship Factory
class SystemToComponentFactory(RelationProcessorFactoryBase):
    """Defines the mapping between the staging table and the BOs."""
    # Source BO
    source_bot_name = "System"
    source_bo_key_attribute = "systemname"
    stage_bo_source_attribute = "src_system_name"

    # Target BO
    target_bot_name = "Component"
    target_bo_key_attribute = "ident"
    stage_bo_target_attribute = "tgt_component_ident"

# 3. Configure and Run
def main():
    repo = StagingRepository(
        staging_bo_name="StagingSystemComponentLink",
        condition="status == 'NEW'"
    )

    factory = SystemToComponentFactory(
        source_repository=repo,
        default_processor_class=SystemToComponentProcessor
    )

    orchestrator = ImportOrchestrator(factories=[factory])
    orchestrator.run()
main()
```

### Advanced Processor Features

The framework also supports more advanced scenarios, such as:

*   **`pre_process` and `post_process` hooks**: For custom logic before and after mapping.
*   **`processor_func`**: For transforming values in-flight.
    Bonus: If `processor_func` returns **stage_importer_framework.undefined**, mapping is skipped.
*   **Find-or-Create Logic**: `RelationField` can be configured to create related objects if they don't exist.

```python
def get_location_name(context):
    tr = context.get_transaction()
    source_bo = context.get_source()
    target_bo = context.get_target()
    # calculate the location name
    # ...
    return "my calculated location name"

class AdvancedSystemProcessor(MappingProcessor):
    """A processor demonstrating advanced features."""
    systemname = PlainField(source_field="NAME")
    serialnumber = PlainField(source_field="SERIAL", processor_func=format_serial_number)
    location = RelationField(
        source_field="LOCATION_ID",
        target_bo_name="Location",
        target_lookup_field="locationId",
        on_not_found_create={
            'name': FromAnywhere(get_location_name),
            'siteCode': FromSource("SITE_CODE"),
            'isDefault': Static(True)
        }
    )
    creation_date = StaticField(now(), lambda c, v: v if c.is_create or undefined)

    def pre_process(self):
        """Set a default description."""
        self.target.getBOField("description").setValue("No description provided.")

    def post_process(self):
        """Append a note to the description."""
        current_desc = self.target.getBOField("description").getValue()
        self.target.getBOField("description").setValue(current_desc + " [Processed by framework]")
```
