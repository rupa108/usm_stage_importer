# -*- coding: utf-8 -*-
"""Demonstrates advanced reconciliation features.

This script showcases several ways to configure and use the reconciliation
feature of the import framework:
    1.  Standard reconciliation (deactivating by setting status).
    2.  Custom reconciliation (e.g., setting a 'validto' date).
    3.  Running an import without any reconciliation.
    4.  Reconciling multiple BO types in a single run.
"""

import traceback
from datetime import datetime

# Framework components
from stage_importer_framework import (
    ImportOrchestrator,
    MappingProcessorFactory,
    StagingRepository,
    ReconciliationBaseline,
    Reconciler,
    MappingProcessor,
    PlainField,
    log_
)

# ==============================================================================
# 1. CONCRETE PROCESSORS
# ==============================================================================

class SystemProcessor(MappingProcessor):
    """A simple processor for Systems."""
    systemname = PlainField(source_field="CI_NR")
    name = PlainField(source_field="NAME")

class ComponentProcessor(MappingProcessor):
    """A simple processor for Components."""
    ident = PlainField(source_field="CI_NR")
    name = PlainField(source_field="NAME")

# ==============================================================================
# 2. CONCRETE RECONCILERS
# ==============================================================================

class ComponentReconciler(Reconciler):
    """Deactivates records by setting their status to 'INACTIVE'."""
    def deactivate_record(self, transaction, record_to_deactivate):
        record_to_deactivate.getBOField("status").setValue("INACTIVE")
        log_("Standard reconciliation: Deactivated '%s'" % record_to_deactivate.getBOField("name").getValue(), VM.LOG_INFO, record_to_deactivate)

class SystemReconciler(Reconciler):
    """Deactivates records by setting the 'validto' field to yesterday."""
    def deactivate_record(self, transaction, record_to_deactivate):
        yesterday_date = datetime.now() # In a real scenario, get yesterday's date
        record_to_deactivate.getBOField("validto").setValue(yesterday_date)
        log_("Custom reconciliation: Set 'validto' for '%s'" % record_to_deactivate.getBOField("name").getValue(), VM.LOG_INFO, record_to_deactivate)

# ==============================================================================
# 3. MAIN EXECUTION BLOCK
# ==============================================================================

if __name__ == "__main__":
    # This block demonstrates four different scenarios for using the
    # reconciliation functionality.
    try:
        # --- SCENARIO 1: Standard Reconciliation ---
        log_("===== SCENARIO 1: STANDARD RECONCILIATION =====", VM.LOG_INFO)
        system_factory = MappingProcessorFactory(
            repository=StagingRepository("StagingSystem"),
            default_processor_class=SystemProcessor,
            target_bo_name="System",
            source_key="PK",
            target_key="xSourcePk"
        )
        system_baseline = ReconciliationBaseline("System", "xSourcePk", "status == 'ACTIVE'")
        system_reconciler = SystemReconciler(system_baseline)
        
        orchestrator1 = ImportOrchestrator(
            factories=[system_factory],
            reconcilers=[system_reconciler]
        )
        orchestrator1.run()

        # --- SCENARIO 2: Custom Reconciliation ---
        log_("===== SCENARIO 2: CUSTOM RECONCILIATION =====", VM.LOG_INFO)
        component_factory = MappingProcessorFactory(
            repository=StagingRepository("StagingComponent"),
            default_processor_class=ComponentProcessor,
            target_bo_name="Component",
            source_key="PK",
            target_key="xSourcePk"
        )
        component_baseline = ReconciliationBaseline("Component", "xSourcePk", "validto IS NULL")
        component_reconciler = ComponentReconciler(component_baseline)

        orchestrator2 = ImportOrchestrator(
            factories=[component_factory],
            reconcilers=[component_reconciler]
        )
        orchestrator2.run()

        # --- SCENARIO 3: No Reconciliation ---
        log_("===== SCENARIO 3: NO RECONCILIATION =====", VM.LOG_INFO)
        orchestrator3 = ImportOrchestrator(
            factories=[system_factory]
            # No reconcilers are passed to the orchestrator
        )
        orchestrator3.run()

        # --- SCENARIO 4: Multi-Type Reconciliation in One Run ---
        log_("===== SCENARIO 4: MULTI-TYPE RECONCILIATION =====", VM.LOG_INFO)
        orchestrator4 = ImportOrchestrator(
            factories=[system_factory, component_factory],
            reconcilers=[system_reconciler, component_reconciler]
        )
        orchestrator4.run()

    except Exception as e:
        log_("A critical error occurred during the reconciliation examples: %s" % e, VM.LOG_ERROR)
        traceback.print_exc()