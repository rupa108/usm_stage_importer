# -*- coding: utf-8 -*-
"""Demonstrates advanced processor features.

This script showcases several advanced capabilities of the MappingProcessor,
including:
    1.  Using `pre_process` and `post_process` hooks for custom logic that
        runs before and after the declarative mapping.
    2.  Using a `processor_func` to transform data in a `PlainField`.
    3.  Using `on_not_found_create` with all `ValueSource` types in a
        `RelationField` to implement find-or-create logic for related objects.
"""

import traceback

# Framework components
from stage_importer_framework import (
    ImportOrchestrator,
    MappingProcessorFactory,
    StagingRepository,
    MappingProcessor,
    PlainField,
    RelationField,
    Static,
    FromSource,
    FromAnywhere,
    ProcessingContext,
    log_
)

# ==============================================================================
# 1. CUSTOM FUNCTIONS FOR ADVANCED MAPPING
# ==============================================================================

def format_serial_number(context, value):
    """A custom processor function to format a serial number.

    Args:
        context (ProcessingContext): The context object, providing access to
            the source and target business objects.
        value (str): The original value from the source field.

    Returns:
        str: The transformed value, or None.
    """
    log_("Executing processor_func 'format_serial_number'", VM.LOG_DEBUG, context.get_source())
    if value:
        return "SN-%s" % value.upper()
    return None

def get_location_name(context):
    """A custom function for FromAnywhere to derive a location name.

    Args:
        context (ProcessingContext): The context object.

    Returns:
        str: A derived location name based on the site code.
    """
    source_bo = context.get_source()
    site_code = source_bo.getBOField("SITE_CODE").getValue()
    if site_code == "BER":
        return "Berlin"
    elif site_code == "NYC":
        return "New York City"
    return "Default Location"

# ==============================================================================
# 2. CONCRETE PROCESSOR WITH ADVANCED FEATURES
# ==============================================================================

class AdvancedSystemProcessor(MappingProcessor):
    """A processor demonstrating advanced mapping and lifecycle hooks.

    Attributes:
        systemname (PlainField): A direct mapping for the system name.
        serialnumber (PlainField): A mapping that uses a `processor_func` to
            transform the value.
        location (RelationField): A mapping for a related object that uses
            `on_not_found_create` to create the related object if it does not
            already exist.
    """
    __processing_order__ = ('systemname', 'serialnumber', 'location')

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

    def pre_process(self):
        """Sets a default description before other mappings are applied."""
        log_("Executing pre_process for: %s" % self.source.getBOField("NAME").getValue(), VM.LOG_INFO, self.source)
        if not self.source.getBOField("DESCRIPTION").getValue():
            self.target.getBOField("description").setValue("No description provided.")

    def post_process(self):
        """Appends a note to the description after all mappings are applied."""
        log_("Executing post_process for: %s" % self.target.getBOField("name").getValue(), VM.LOG_INFO, self.target)
        current_desc = self.target.getBOField("description").getValue()
        self.target.getBOField("description").setValue(current_desc + " [Processed by framework]")

# ==============================================================================
# 3. MAIN EXECUTION BLOCK
# ==============================================================================

if __name__ == "__main__":
    try:
        factory = MappingProcessorFactory(
            repository=StagingRepository("StagingAdvancedSystems"),
            processor_class=AdvancedSystemProcessor,
            target_bo_name="System",
            key_field="PK",
            target_key_field="xSourcePk"
        )

        orchestrator = ImportOrchestrator(factories=[factory])
        orchestrator.run()

    except Exception as e:
        log_("A critical error occurred during the advanced processor example: %s" % e, VM.LOG_ERROR)
        traceback.print_exc()