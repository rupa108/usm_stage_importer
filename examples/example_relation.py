# -*- coding: utf-8 -*-
"""Demonstrates the relationship import framework.

This script shows how to configure and run a relationship import using the
new factory-based approach.
"""

import traceback

# Framework components
from stage_importer_framework import (
    ImportOrchestrator,
    StagingRepository,
    RelationProcessorFactoryBase,
    RelationshipProcessor,
    log_
)

# ==============================================================================
# 1. CONCRETE RELATIONSHIP PROCESSOR
# ==============================================================================

class SystemToComponentProcessor(RelationshipProcessor):
    """
    A simple processor that links a Component to a System.
    The `rel_attr_name` must match the name of the relation field on the
    source BO (System).
    """
    rel_attr_name = "compsystems"

# ==============================================================================
# 2. CONCRETE RELATIONSHIP FACTORY
# ==============================================================================

class SystemToComponentFactory(RelationProcessorFactoryBase):
    """
    A factory for creating System-to-Component relationships.

    This class defines the mapping between the staging table and the
    source/target business objects.
    """
    # --- Source BO Configuration ---
    source_bot_name = "System"
    source_bo_key_attribute = "systemname"
    stage_bo_source_attribute = "src_system_name"

    # --- Target BO Configuration ---
    target_bot_name = "Component"
    target_bo_key_attribute = "ident"
    stage_bo_target_attribute = "tgt_component_ident"

# ==============================================================================
# 3. MAIN EXECUTION BLOCK
# ==============================================================================

if __name__ == "__main__":
    # This block orchestrates the relationship import.
    try:
        # 1. Define the repository for the staging data.
        #    This points to the staging table containing the relationship info.
        repo = StagingRepository(
            staging_bo_name="StagingSystemComponentLink",
            condition="status == 'NEW'"
        )

        # 2. Define the factory for the relationship import.
        #    It is configured with the repository and the specific processor to use.
        factory = SystemToComponentFactory(
            source_repository=repo,
            default_processor_class=SystemToComponentProcessor
        )

        # 3. The orchestrator runs the entire process.
        #    Note: Reconciliation of relationships is a more complex topic
        #    and would require a custom reconciler not shown in this example.
        orchestrator = ImportOrchestrator(
            factories=[factory]
        )
        
        orchestrator.run()

    except Exception as e:
        log_("A critical error occurred during the relationship import example: %s" % e, VM.LOG_ERROR)
        traceback.print_exc()
