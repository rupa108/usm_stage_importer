# -*- coding: utf-8 -*-
"""Demonstrates relationship import using Meta class configuration on RelationshipProcessor.

This example shows how to configure the required BO mapping attributes via a Meta class
on the RelationshipProcessor, instead of on the factory. This is the preferred, declarative style.
"""

import traceback

from stage_importer_framework import (
    ImportOrchestrator,
    StagingRepository,
    RelationProcessorFactory,
    RelationshipProcessor,
    log_
)

# ===============================================================================
# 1. RELATIONSHIP PROCESSOR WITH META CONFIGURATION
# ===============================================================================

class SystemToComponentProcessor(RelationshipProcessor):
    """
    A processor that links a Component to a System, using Meta for config.
    """
    rel_attr_name = "compsystems"

    class Meta:
        # --- Source BO Configuration ---
        source_bo_name = "System"
        source_bo_key_attribute = "systemname"
        stage_bo_source_attribute = "src_system_name"

        # --- Target BO Configuration ---
        target_bo_name = "Component"
        target_bo_key_attribute = "ident"
        stage_bo_target_attribute = "tgt_component_ident"

# ===============================================================================
# 2. MAIN EXECUTION BLOCK
# ===============================================================================

if __name__ == "__main__":
    try:
        repo = StagingRepository(
            staging_bo_name="StagingSystemComponentLink",
            condition="status == 'NEW'"
        )
        factory = RelationProcessorFactory(
            source_repository=repo,
            default_processor_class=SystemToComponentProcessor
        )
        orchestrator = ImportOrchestrator(factories=[factory])
        orchestrator.run()
    except Exception as e:
        log_("A critical error occurred during the Meta relationship import example: %s" % e, VM.LOG_ERROR)
        traceback.print_exc()
