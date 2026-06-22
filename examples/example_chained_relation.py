# -*- coding: utf-8 -*-
"""Demonstrates the ``ChainedRelationField``.

A plain :class:`RelationField` resolves a related Business Object with a simple
find-or-create against a single lookup field. That is not enough when the
related object itself needs *rich* mapping (several attributes, transformations,
nested relations, dynamic processor selection, ...).

``ChainedRelationField`` solves this by delegating the resolution of the related
BO to a full :class:`MappingProcessor`, executed through a *chained* (i.e.
repository-less) factory. The chained factory is fed the **parent's source
record** instead of iterating its own staging table.

This example imports ``Contract`` records from a staging table. Each contract
references a supplier. Instead of merely linking an existing supplier by name,
we want to fully map/create the related ``Supplier`` BO (name, tax id, country)
from fields that live on the very same staging row.

Three usage variants are shown:

1. ``supplier`` - the minimal form: pass a ``MappingProcessor`` subclass and let
   the field build a chained factory automatically.
2. ``manager``  - pass a pre-built chained factory (useful to share config or to
   use ``rules`` for dynamic processor selection).
3. ``site``     - use a ``processor_func`` to short-circuit the chain (e.g. skip
   resolution for sentinel values) and only fall through to the chained factory
   when the function returns ``undefined``.
"""

import traceback

from stage_importer_framework import (
    MappingProcessor,
    MappingProcessorFactory,
    PlainField,
    StaticField,
    ChainedRelationField,
    StagingRepository,
    ImportOrchestrator,
    ReconciliationBaseline,
    Reconciler,
    set_log_domain,
    log_,
    undefined,
    CLEAR_LINK,
)

set_log_domain("ChainedRelationExample")


# ==============================================================================
# 1. PROCESSORS FOR THE RELATED ("CHAINED") OBJECTS
# ==============================================================================

class SupplierProcessor(MappingProcessor):
    """Maps/creates a ``Supplier`` from the contract staging row.

    The ``match_key`` field (``code``) is what makes this processor usable as a
    chained processor: it tells the chained factory how to find an existing
    Supplier (``supplier_code`` on the source row == ``code`` on the target)
    before deciding to create a new one.
    """

    __processing_order__ = ["code", "name", "country"]

    code = PlainField(source_field="supplier_code", match_key=True)
    name = PlainField(source_field="supplier_name")
    country = PlainField(source_field="supplier_country")

    class Meta:
        target_bo_name = "Supplier"


class ManagerProcessor(MappingProcessor):
    """Maps/creates a ``Person`` acting as the contract manager."""

    __processing_order__ = ["login", "display_name"]

    login = PlainField(source_field="manager_login", match_key=True)
    display_name = PlainField(source_field="manager_name")

    class Meta:
        target_bo_name = "Person"
        # The target match field differs from the source field name; declare it
        # explicitly via the match_key field's target (here login == login,
        # shown for completeness).


class SiteProcessor(MappingProcessor):
    """Maps/creates a ``Location`` for the contract's delivery site."""

    __processing_order__ = ["site_code", "site_label"]

    site_code = PlainField(source_field="site_code", match_key=True)
    site_label = PlainField(source_field="site_name")

    class Meta:
        target_bo_name = "Location"


def resolve_site(context, lookup_value):
    """Short-circuit resolution for the ``site`` chained relation.

    A ``processor_func`` lets the caller decide *per record* whether to resolve
    the related BO at all. Its return value controls what happens next:

    * any BO        -> used directly as the related object (no chaining);
    * ``CLEAR_LINK`` -> the existing link is cleared;
    * ``undefined``  -> fall through and run the chained ``SiteProcessor``.

    Here, the sentinel code ``"VIRTUAL"`` means "no physical delivery site", so
    we clear the link; every other code is mapped/created via the chain.
    """
    if lookup_value == "VIRTUAL":
        return CLEAR_LINK
    return undefined


# ==============================================================================
# 2. THE PARENT PROCESSOR USING ChainedRelationField
# ==============================================================================

# A pre-built chained factory (variant 2). Built via as_chained() so it has no
# repository of its own; it is driven by the parent's source record.
manager_factory = MappingProcessorFactory.as_chained(
    default_processor_class=ManagerProcessor
)


class ContractProcessor(MappingProcessor):
    """Maps the top-level ``Contract`` BO and its rich related objects."""

    __processing_order__ = ["number", "title", "status", "supplier", "manager", "site"]

    number = PlainField(source_field="contract_no", match_key=True)
    title = PlainField(source_field="contract_title")
    status = StaticField(value="ACTIVE")

    # Variant 1: minimal form - just hand over the processor class. The field
    # builds the chained factory internally and derives the target BO type from
    # SupplierProcessor.Meta.target_bo_name.
    supplier = ChainedRelationField(
        source_field="supplier_code",
        processor_or_factory=SupplierProcessor,
    )

    # Variant 2: pass a pre-built chained factory.
    manager = ChainedRelationField(
        source_field="manager_login",
        processor_or_factory=manager_factory,
    )

    # Variant 3: provide a processor_func to short-circuit when appropriate.
    site = ChainedRelationField(
        source_field="site_code",
        processor_or_factory=SiteProcessor,
        processor_func=resolve_site,
    )

    class Meta:
        target_bo_name = "Contract"


# ==============================================================================
# 3. MAIN EXECUTION BLOCK
# ==============================================================================

def main():
    repo = StagingRepository(
        staging_bo_name="StagingContract",
        condition="status == 'NEW'",
    )

    # Only the *parent* factory needs a repository. The chained factories used by
    # the ChainedRelationFields are repository-less and are fed each contract row.
    factory = MappingProcessorFactory(
        repository=repo,
        default_processor_class=ContractProcessor,
    )

    # Reconciliation still works for chained-created objects: process_chained()
    # propagates the related BOs' monikers into the parent factory's active keys.
    recon_baseline = ReconciliationBaseline(
        target_bo_name="Contract",
        active_condition="status == 'ACTIVE'",
    )
    reconciler = Reconciler(recon_baseline)

    orchestrator = ImportOrchestrator(
        factories=[factory],
        reconcilers=[reconciler],
    )

    orchestrator.run(commit_batch_size=100)

    print("Chained relation import completed.")
    print(factory.get_summary())


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_("A critical error occurred during the chained relation example: %s" % e, VM.LOG_ERROR)
        traceback.print_exc()
