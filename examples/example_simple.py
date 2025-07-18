# -*- coding: utf-8 -*-
"""A minimalist, working example of the import framework.

This script demonstrates the basic setup and execution of a record import,
including a simple reconciliation step. It is intended to be the "hello world"
of this framework.
"""

import traceback

# Framework components
from stage_importer_framework import (
    ImportOrchestrator,
    MappingProcessorFactory,
    StagingRepository,
    ReconciliationBaseline,
    Reconciler,
    MappingProcessor,
    PlainField,
    StaticField,
    log_
)

# ==============================================================================
# 1. CONCRETE PROCESSOR
# ==============================================================================

class SystemProcessor(MappingProcessor):
    """A simple processor for mapping staging data to the System BOType."""
    __processing_order__ = ('systemname', 'name', 'description', 'status')
    
    systemname = PlainField(source_field="CI_NR")
    name = PlainField(source_field="NAME")
    description = PlainField(source_field="DESCRIPTION")
    status = StaticField(value="ACTIVE")

# ==============================================================================
# 2. MAIN EXECUTION BLOCK
# ==============================================================================

if __name__ == "__main__":
    # This block orchestrates the import process. It wires together the
    # necessary components: a factory to process the data, a baseline to
    # define the scope of reconciliation, and a reconciler to handle obsolete
    # records.
    try:
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

    except Exception as e:
        log_("A critical error occurred during the simple import.", VM.LOG_ERROR)
        log_(traceback.format_exc(), VM.LOG_EXCEPTION)
