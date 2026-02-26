"""
Example demonstrating the use of processor_func with match_key fields.

This example shows how a processor_func can transform the match key value
before it's used to look up the target business object. The framework
uses the field's get_processed_value() method internally to ensure
consistent processing during both lookup and mapping phases.
"""

from stage_importer_framework import (
    MappingProcessor,
    PlainField,
    StaticField,
    StagingRepository,
    MappingProcessorFactory,
    ImportOrchestrator,
    ReconciliationBaseline,
    Reconciler,
    set_log_domain
)

# Set the log domain for this import
set_log_domain("MatchKeyProcessorExample")


def transform_identifier(context, value):
    """
    Example processor function that transforms the identifier.
    For instance, it might add a prefix or convert the format.
    """
    # Example: Convert to uppercase and add a prefix
    if value:
        return "PROC_" + str(value).upper()
    return value


class ExampleMatchKeyProcessor(MappingProcessor):
    """
    Example processor that uses a processor_func on the match_key field.
    The identifier field is both the match key and has a transformation function.
    """
    
    # Define the processing order
    __processing_order__ = ["identifier", "name", "description", "status"]
    
    # The identifier field is the match key and has a processor_func
    identifier = PlainField(
        source_field="stage_id",
        processor_func=transform_identifier,
        match_key=True  # This marks it as the matching key
    )
    
    name = PlainField(source_field="stage_name")
    description = PlainField(source_field="stage_description")
    status = StaticField(value="ACTIVE")
    
    class Meta:
        target_bo_name = "ExampleTarget"


def main():
    """
    Main function demonstrating the match_key with processor_func usage.
    """
    
    # Create the staging repository
    repo = StagingRepository(
        staging_bo_name="ExampleStaging",
        condition=""  # Process all records
    )
    
    # Create the factory
    # Note: We don't need to specify source_key and target_key
    # because they're defined via match_key in the processor
    factory = MappingProcessorFactory(
        repository=repo,
        default_processor_class=ExampleMatchKeyProcessor
    )
    
    # Optional: Set up reconciliation
    recon_baseline = ReconciliationBaseline(
        target_bo_name="ExampleTarget",
        active_condition="status == 'ACTIVE'"
    )
    reconciler = Reconciler(recon_baseline)
    
    # Create and run the orchestrator
    orchestrator = ImportOrchestrator(
        factories=[factory],
        reconcilers=[reconciler]
    )
    
    # Run the import
    orchestrator.run(commit_batch_size=100)
    
    print("Import completed successfully!")
    print(factory.get_summary())


if __name__ == "__main__":
    main()