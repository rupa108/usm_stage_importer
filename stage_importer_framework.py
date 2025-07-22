"""
A library for building staging-to-production import scripts.

This module provides a highly modular, enterprise-grade framework for creating
scripts that process data from a staging table into a target business object
structure. It is built on several core principles:

- **Separation of Concerns**: Each component has a single, well-defined responsibility.
- **Declarative and Ordered Mapping**: Business logic for field-to-field mapping is
  declared declaratively. The processing order can be explicitly guaranteed.
- **Lifecycle Hooks**: Provides `pre_process` and `post_process` hooks for custom logic.
- **Dynamic Processor Selection**: A rule-based system dynamically selects the correct
  processing logic for each record.
- **Batch Commits**: Large datasets are handled efficiently using the API's
  built-in `committedAfter()` functionality.
- **Mark and Sweep Reconciliation**: An optional step can deactivate target
  records that are no longer present in the source data.
"""
from abc import ABCMeta, abstractmethod
from de.usu.s3.api import ApiBObject
import traceback


# ==============================================================================
# 0. CUSTOM EXCEPTIONS
# ==============================================================================

class AmbiguousProcessorError(Exception):
    """
    Raised by a ProcessorProvider when more than one processor rule matches a
    given staging record, creating ambiguity.
    """
    def __init__(self, message, matching_processors=None):
        super(AmbiguousProcessorError, self).__init__(message)
        self.matching_processors = matching_processors or []

class MultipleRecordsFoundError(Exception):
    """
    Raised when a query that is expected to return a single record returns
    multiple records, indicating a data integrity issue.
    """
    def __init__(self, message):
        super(MultipleRecordsFoundError, self).__init__(message)
        self.message = message

# ==============================================================================
# 1. HELPER FUNCTIONS
# ==============================================================================

def yesterday():
    # TBD
    pass

def log_(message, level, bo=None):
    # type: (message: str, level: int, bo: ApiBObject) -> None
    """
    A centralized logging function that prints to the console for high-level
    messages and writes to the persistent log for all levels.
    """
    level_map = {VM.LOG_INFO: "INFO", VM.LOG_WARN: "WARNING", VM.LOG_ERROR: "ERROR", VM.LOG_DEBUG: "DEBUG"}

    if level in [VM.LOG_INFO, VM.LOG_WARN, VM.LOG_ERROR]:
        print "%s: %s" % (level_map.get(level, "LOG"), message)

    VM.persistentLogMessage("Importer", message, None, None, bo, level, True)


def get_bo(tr, bo_type, condition, trl_type=VM.TRL_CURRENT, strict=False):
    # type: (tr: ApiTransaction, bo_type: ApiBOType, condition: str, trl_type: int, strict: bool) -> ApiBObject
    """
    Finds a business object by type and condition, returning the first match.
    If strict is True, raises an exception if more than one object matches.
    """
    result = None

    bos = bo_type.createFilterForNewObjects(tr, condition)

    if bos.isEmpty():
        find_result = bo_type.find(tr, condition, trl_type)
        if strict and find_result.isMore():
            bot_name = bo_type.getName()
            raise MultipleRecordsFoundError("Found multiple %s objects for condition '%s'." % (bot_name, condition))
        else:
            result = find_result.getBObject()
    else:
        result = bos.get(0)

    return result

# ==============================================================================
# 2. CORE LIBRARY ABSTRACT CLASSES
# ==============================================================================

class AbstractField(object):
    """Abstract base class for a declarative mapping field."""
    __metaclass__ = ABCMeta

    def __init__(self, source_field=None, processor_func=None):
        self.source_field = source_field
        self.target_field = None
        self.processor_func = processor_func

    def set_target_field(self, name):
        self.target_field = name

    @abstractmethod
    def map_value(self, context):
        pass


class ProcessorMetaclass(ABCMeta):
    """
    A metaclass that captures the declaration order of FieldDescriptors
    on RecordProcessor subclasses if `__processing_order__` is defined.
    """
    def __new__(cls, name, bases, attrs):
        ordered_descriptors = []

        if '__processing_order__' in attrs:
            # If order is specified, enforce it
            processing_order = attrs['__processing_order__']
            for field_name in processing_order:
                descriptor = attrs.get(field_name)
                if not isinstance(descriptor, AbstractField):
                    raise TypeError(
                        "Field '%s' listed in `__processing_order__` is not a "
                        "valid FieldDescriptor instance in class %s." % (field_name, name)
                    )
                descriptor.target_field = field_name
                ordered_descriptors.append(descriptor)
        else:
            # If no order is specified, discover fields but do not guarantee order
            discovered_descriptors = []
            for key, value in attrs.items():
                if isinstance(value, AbstractField):
                    value.target_field = key
                    discovered_descriptors.append(value)
            ordered_descriptors = discovered_descriptors

        # remove descriptor fields from their original location in order to clean up the scope
        for each in ordered_descriptors:
            del attrs[each.target_field]

        attrs['_ordered_descriptors'] = ordered_descriptors
        return super(ProcessorMetaclass, cls).__new__(cls, name, bases, attrs)


class AbstractProcessor(object):
    """Base class for processing a single record and tracking touched objects."""
    __metaclass__ = ProcessorMetaclass

    def __init__(self, tr, source_bo, target_bo):
        self.transaction = tr
        self.source = source_bo
        self.target = target_bo
        self._touched_objects = set()
        self.message = ""

    @abstractmethod
    def process(self):
        """
        Applys the processing logic and returns the processing target.
        """
        raise NotImplementedError("Subclasses must implement a method of the processing logic")

    @abstractmethod
    def pre_process(self):
        """Hook for pre processing logic"""
        raise NotImplementedError("Subclasses must implement pre_process()")

    @abstractmethod
    def post_process(self):
        """Hook for post processing logic"""
        raise NotImplementedError("Subclasses must implement post_process()")

    @abstractmethod
    def add_touched_object(self, bo):
        """
        Adds a touched business object to the processor's internal set.
        This is used to track which objects were modified or created during processing."""
        if bo:
            self._touched_objects.add(bo)

    @abstractmethod
    def get_active_keys(self):
        keys = set()
        for bo in self._touched_objects:
            keys.add(bo.getMoniker())
        log_("Processor %s collected %d active keys." % (self.__class__.__name__, len(keys)), VM.LOG_DEBUG, self.source)
        return keys

class AbstractRepository(object):
    """Abstract base class for a repository that provides records to be processed."""
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_unprocessed_records(self, tr):
        """
        Returns an iterator over all unprocessed records in the repository.
        The iterator should yield ApiBObject instances.
        """
        # type: (tr: ApiTransaction) -> Iterator[ApiBObject]
        raise NotImplementedError("Subclasses must implement get_unprocessed_records()")

class AbstractFactory(object):
    """
    Abstract base class for a factory that processes records from a repository.
    The factory handles one or more processor classes that do the actual processing.

    Arguments:
        source_repository (AbstractRepository): An object that provides the records to be processed.
        default_processor_class (AbstractProcessor): The default processor class to use for processing source records.
    """
    __metaclass__ = ABCMeta

    def __init__(self, source_repository, default_processor_class):
        # type: (source_repository: AbstractRepository, default_processsor_class: AbstractProcessor) -> None
        self.repository = source_repository
        self.default_processor_class = default_processor_class
        self.processed_count = 0
        self.failed_count = 0

    @abstractmethod
    def process_all(self, tr, commit_batch_size=None):
        """
        Contains the main loop and logic for processing all records from the
        repository.
        """
        raise NotImplementedError("Subclasses must implement process_all()")

    def get_summary(self):
        return "Successfully processed: %d\nFailed: %d" % (self.processed_count, self.failed_count)

class AbstractReconciler(object):
    """Abstract base class for deactivating obsolete target records."""
    __metaclass__ = ABCMeta

    def __init__(self, reconciliation_baseline):
        assert isinstance(reconciliation_baseline, AbstractReconciliationBaseline)
        self.reconciliation_baseline = reconciliation_baseline
        self.deactivated_count = 0

    @abstractmethod
    def run(self, tr, active_keys):
        # type: (tr: ApiTransaction, active_keys: set) -> None
        """
        Runs the reconciliation process, deactivating target records that are
        no longer present in the source data.
        Args:
            tr: The transaction context for the reconciliation.
            active_keys: A set of keys that are still active in the source data.
                The keys are monikers gathered during the import phase.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @abstractmethod
    def deactivate_record(self, tr, bo):
        # type: (tr: ApiTransaction, bo: ApiBObject) -> None
        """
        Deactivate or delete the given record.
        """
        raise NotImplementedError("Subclasses must implement this method")

class AbstractReconciliationBaseline(object):
    """
    Abstract base class for fetching all active target records for reconciliation.
    This class is used to define the baseline for reconciliation, such as
    which target records are considered active and should be checked against
    the source data.

    Arguments:
        target_bo_name (str): The name of the target business object type.
        active_condition (str): A condition to filter active records, e.g. "status == 'ACTIVE'".
    """
    __metaclass__ = ABCMeta

    def __init__(self, target_bo_name, active_condition="status == 'ACTIVE'"):
        self.target_bo_name = target_bo_name
        self.active_condition = active_condition

    @abstractmethod
    def get_all_active_records(self, tr):
        raise NotImplementedError("Subclasses must implement get_all_active_records()")

# ==============================================================================
# 3. ATTRIBUTE MAPPING FRAMEWORK
# ==============================================================================

class ProcessingContext(object):
    """Provides a controlled context to a custom processor function."""
    def __init__(self, processor, source_field_name, target_field_name):
        self._processor = processor
        self._source_field_name = source_field_name
        self._target_field_name = target_field_name

    def get_source(self):
        return self._processor.source

    def get_target(self):
        return self._processor.target

    def add_touched_object(self, bo):
        self._processor.add_touched_object(bo)

    def get_transaction(self):
        return self._processor.transaction

class MappingProcessor(AbstractProcessor):
    """Default implementation of a Processor"""

    def __init__(self, tr, source_bo, target_bo):
        super(MappingProcessor, self).__init__(tr, source_bo, target_bo)
        self.add_touched_object(target_bo)


    def process(self):
        log_("Applying declarative mappings using %s..." % self.__class__.__name__, VM.LOG_DEBUG, self.source)
        if not hasattr(self, '__processing_order__'):
            log_("`__processing_order__` not defined for %s. Field processing order is not guaranteed." % self.__class__.__name__, VM.LOG_DEBUG, self.source)

        for descriptor in self._ordered_descriptors:
            try:
                context = ProcessingContext(self, descriptor.source_field, descriptor.target_field)
                descriptor.map_value(context)
            except Exception as e:
                log_("Could not map field '%s' to target '%s': %s" % (descriptor.source_field, descriptor.target_field, e), VM.LOG_WARN, self.source)
                raise

    def pre_process(self): pass

    def post_process(self): pass

    def add_touched_object(self, bo):
        super(MappingProcessor, self).add_touched_object(bo)

    def get_active_keys(self):
        return super(MappingProcessor, self).get_active_keys()

# ==============================================================================
# 3.1 Descriptor Fields
# ==============================================================================

class PlainField(AbstractField):
    """A descriptor for mapping simple, direct field-to-field values."""
    def map_value(self, context):
        source_bo = context.get_source()
        source_value = source_bo.getBOField(self.source_field).getValue()
        if self.processor_func:
            final_value = self.processor_func(context, source_value)
        else:
            final_value = source_value
        target_bo = context.get_target()
        target_bo.getBOField(self.target_field).setValue(final_value)


class StaticField(AbstractField):
    """A descriptor for setting a static, predefined value on a target field."""
    def __init__(self, value, processor_func=None):
        super(StaticField, self).__init__(source_field=None, processor_func=processor_func)
        self.value = value

    def map_value(self, context):
        if self.processor_func:
            final_value = self.processor_func(context, self.value)
        else:
            final_value = self.value

        target_bo = context.get_target()
        target_bo.getBOField(self.target_field).setValue(final_value)

class RelationField(AbstractField):
    """
    A descriptor for mapping a value to a related Business Object.
    It supports find-or-create logic for the related object.
    """
    def __init__(self, source_field, target_bo_name, target_lookup_field="name", on_not_found_create=None, processor_func=None):
        super(RelationField, self).__init__(source_field, processor_func)
        self.target_bo_name = target_bo_name
        self.target_lookup_field = target_lookup_field
        self.on_not_found_create = on_not_found_create

    def _create_related_bo(self, context, lookup_value):
        """Creates a new related BO based on the on_not_found_create config."""
        source_bo = context.get_source()
        log_("Creating new related object for '%s' in BO '%s'" % (lookup_value, self.target_bo_name), VM.LOG_INFO, source_bo)
        related_bo_type = VM.getBOType(self.target_bo_name)
        tr = context.get_transaction()
        new_bo = related_bo_type.createBO(tr, 1)

        for field, value_source in self.on_not_found_create.items():
            if not isinstance(value_source, ValueSource):
                raise TypeError("Value for 'on_not_found_create' must be an instance of a ValueSource class (Static, FromSource, etc.)")

            value = value_source.get_value(context)
            if value is not None:
                new_bo.getBOField(field).setValue(value)

        return new_bo

    def map_value(self, context):
        """Finds or creates a related BO and sets the relation on the target BO."""
        tr = context.get_transaction()
        source_bo = context.get_source()
        related_bo = None
        lookup_value = source_bo.getBOField(self.source_field).getValue()

        if self.processor_func:
            related_bo = self.processor_func(context, lookup_value)
        elif lookup_value:
            related_bo_type = VM.getBOType(self.target_bo_name)
            condition = "%s == '%s'" % (self.target_lookup_field, str(lookup_value).replace("'", "''"))
            find_result = related_bo_type.find(tr, condition)

            if find_result.isOne():
                related_bo = find_result.getBObject()
            elif find_result.isNone():
                for bo in tr.getAllNewlyCreatedObjects():
                    if bo.getBOType().getName() == self.target_bo_name and bo.matchCondition(condition):
                        related_bo = bo
                        break
                if not related_bo and self.on_not_found_create:
                    related_bo = self._create_related_bo(context, lookup_value)
            elif find_result.isMore():
                raise Exception("Found multiple related objects for '%s' = '%s'. The lookup field must be unique." % (self.target_lookup_field, lookup_value))

        target_bo = context.get_target()

        if related_bo:
            target_bo.getBOField(self.target_field).linkObject(related_bo)
            context.add_touched_object(related_bo)
        else:
            if lookup_value:
                log_("Could not find or create a related object for '%s' in BO '%s'" % (lookup_value, self.target_bo_name), VM.LOG_WARN, source_bo)

            target_bo.getBOField(self.target_field).setObject(None)

# ==============================================================================
# 3.2 Helper Classes for RelationField processing
# ==============================================================================

class ValueSource(object):
    """Abstract base class for instructions on how to get a value."""
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_value(self, context):
        pass

class Static(ValueSource):
    """An instruction to use a static, predefined value."""
    def __init__(self, value):
        self.value = value

    def get_value(self, context):
        return self.value

class FromSource(ValueSource):
    """An instruction to get a value from a field on the source record."""
    def __init__(self, source_field):
        self.source_field = source_field

    def get_value(self, context):
        source_bo = context.get_source()
        return context.getSource.source.getBOField(self.source_field).getValue()

class FromAnywhere(ValueSource):
    """An instruction to get a value by executing a custom function."""
    def __init__(self, callable_func):
        self.callable_func = callable_func

    def get_value(self, context):
        return self.callable_func(context)



# ==============================================================================
# 4. RELATIONSHIP PROCESSING FRAMEWORK
# ==============================================================================

class RelationshipProcessor(AbstractProcessor):
    """
    Base class for a processor that creates a single relationship
    between two business objects.

        It must be subclassed!

    """
    # Subclasses must define this attribute
    rel_attr_name = None  # The name of the relationship attribute on the source BO.

    def process(self):
        rel_attr_name = type(self).rel_attr_name
        if not rel_attr_name:
            raise NotImplementedError("Subclasses must define 'rel_attr_name' to specify the relationship attribute.")
        source_bo = self.source
        target_bo = self.target
        rel_field = source_bo.getBOField(rel_attr_name)
        if rel_field.isCollection():
            if target_bo and target_bo not in rel_field.getCollection():
                rel_bo = rel_field.linkObject(target_bo)
                if rel_bo:
                    self.add_touched_object(rel_bo)
        else:
            rel_field.setObject(target_bo)

    def pre_process(self): pass

    def post_process(self): pass

    def add_touched_object(self, bo):
        super(RelationshipProcessor, self).add_touched_object(bo)

    def get_active_keys(self):
        super(RelationshipProcessor, self).get_active_keys()

class RelationProcessorFactoryBase(AbstractFactory):
    """
    This is a base class for a relationship processor factory that MUST be subclassed.
    It handles relationships between business objects.
    This factory processes records from a repository and creates relationships
    between source and target business objects based on the defined rules.

    Arguments:
        source_repository (AbstractRepository): An object that provides the relationship data rows.
        default_processor_class (AbstractProcessor): A processor that creates the relation betwee given
        source and target bos.
    """
    # Subclasses must define these attributes
    source_bot_name = None
    source_bo_key_attribute = None
    stage_bo_source_attribute = None

    target_bot_name = None
    target_bo_key_attribute = None
    stage_bo_target_attribute = None



    def __init__(self, source_repository, default_processor_class):
        # type: (source_repository: AbstractRepository, default_processsor_class: AbstractProcessor) -> None
        cls = type(self)
        mandatory_attrs = [
            "source_bot_name",
            "source_bo_key_attribute",
            "stage_bo_source_attribute",
            "target_bot_name",
            "target_bo_key_attribute",
            "stage_bo_target_attribute",
        ]
        for attr in mandatory_attrs:
            if getattr(cls, attr) is None:
                raise NotImplementedError("Class %s must define the attribute '%s'." % (cls.__name__, attr))
        self.source_bo_type = VM.getBOType(cls.source_bot_name)
        self.target_bo_type = VM.getBOType(cls.target_bot_name)

        super(RelationProcessorFactory, self).__init__(source_repository, default_processor_class)



    def process_all(self, tr, commit_batch_size=None):
        """
        Executes the relationship import process.
        """
        log_("--- Starting Relationship Import Phase ---", VM.LOG_INFO)

        for row_bo in self.repository.get_unprocessed_records(tr):
            try:
                self._process_row(tr, row_bo)
            except:
                log_("Failed to process %s" % row_bo.getMoniker(), VM.LOG_ERROR, row_bo)
                log_(traceback.format_exc(), VM.LOG_EXCEPTION, row_bo)
                self.failed_count += 1
            else:
                self.processed_count += 1                

    def _process_row(self, tr, row_bo):
        """
        Processes a single row from the data source.
        """
        cls = type(self)
        source_condition = "%s == '%s'" % (
            cls.source_bo_key_attribute,
            row_bo.getBOField(cls.stage_bo_source_attribute).getValue()
        )

        source_bo = get_bo(tr, self.source_bo_type, source_condition, strict=True)
        target_condition = "%s == '%s'" % (
            cls.target_bo_key_attribute,
            row_bo.getBOField(cls.stage_bo_target_attribute).getValue()
        )
        target_bo = get_bo(tr, self.target_bo_type, target_condition, strict=True)

        if not source_bo or not target_bo:
            log_("Source or target BO not found for row: %s" % row_bo.getMoniker(), VM.LOG_WARN, row_bo)
            return
        ProcessorClass = self.default_processor_class # type: RelationshipProcessor
        processor = ProcessorClass(tr, source_bo, target_bo)
        processor.pre_process()
        processor.process()
        processor.post_process()
        self.add_touched_object(source_bo)
        self.add_touched_object(target_bo)

    def get_summary(self):
        return "Processed relationship links: %d\nFailed rows: %d" % (self.processed_count, self.failed_count)


# ==============================================================================
# 5. CONCRETE FACTORY AND REPOSITORY IMPLEMENTATIONS
# ==============================================================================


class MappingProcessorFactory(AbstractFactory):
    def __init__(self, repository, default_processor_class, target_bo_name, source_key, target_key, rules=None):
        super(MappingProcessorFactory, self).__init__(repository, default_processor_class)
        self.rules = rules if rules else []
        self.target_bo_name = target_bo_name
        self.key_field = source_key
        self.target_key_field = target_key
        self.active_target_keys = set()

    def get_target_key_field(self):
        return self.target_key_field

    def _get_or_create_target(self, tr, staging_record):
        target_type = VM.getBOType(self.target_bo_name)
        key_value = staging_record.getBOField(self.key_field).getValue()
        if not key_value:
            raise ValueError("Key field '%s' is empty." % self.key_field)

        condition = "%s == '%s'" % (self.target_key_field, str(key_value).replace("'", "''"))
        find_result = target_type.find(tr, condition)

        target_bo = None
        created = False
        if find_result.isOne():
            target_bo = find_result.getBObject()
        elif find_result.isMore():
            raise Exception("Found multiple target objects for key '%s' = '%s'. Key must be unique." % (self.target_key_field, key_value))
        elif find_result.isNone():
            for bo in tr.getAllNewlyCreatedObjects():
                if bo.getBOType().getName() == self.target_bo_name and bo.matchCondition(condition):
                    target_bo = bo
                    break

        if not target_bo:
            target_bo = target_type.createBO(tr,1)
            created = True

        return target_bo, created

    def _get_processor(self, tr, staging_record):
        matching_classes = [p_class for matcher, p_class in self.rules if matcher(staging_record)]

        if len(matching_classes) > 1:
            raise AmbiguousProcessorError(
                "More than one processor rule matched for record.",
                matching_processors=[cls.__name__ for cls in matching_classes]
            )

        processor_class = matching_classes[0] if matching_classes else self.default_processor_class

        if matching_classes:
            log_("Selected processor '%s' for record." % processor_class.__name__, VM.LOG_DEBUG, staging_record)
        else:
            log_("No specific processor matched. Using default: '%s'." % processor_class.__name__, VM.LOG_DEBUG, staging_record)

        target_bo, created = self._get_or_create_target(tr, staging_record)
        if created:
            log_("Created new target object '%s' for record." % target_bo.getMoniker(), VM.LOG_DEBUG, staging_record)

        return processor_class(tr, staging_record, target_bo)

    def process_all(self, tr, commit_batch_size=None):
        iterator = self.repository.get_unprocessed_records(tr)
        if commit_batch_size:
            iterator.committedAfter(commit_batch_size)

        for record in iterator:
            identifier = record.getBOField(self.key_field).getValue()
            try:
                processor_instance = self._get_processor(tr, record)

                processor_instance.pre_process()
                processor_instance.process()
                processor_instance.post_process()

                self.active_target_keys.update(processor_instance.get_active_keys())

                self._mark_as_processed(record, "PROCESSED", processor_instance.message)
                self.processed_count += 1
                log_("Success: %s" % processor_instance.message, VM.LOG_INFO, record)
            except Exception as e:
                self.failed_count += 1
                error_message = str(e)
                if isinstance(e, AmbiguousProcessorError):
                    error_message += " Conflicting processors: %s" % e.matching_processors
                self._mark_as_processed(record, "FAILED", error_message)
                log_("ERROR on record %s: %s" % (identifier, error_message), VM.LOG_ERROR, record)
                raise

    def _mark_as_processed(self, rec, status, msg=""):
        pass

class StagingRepository(AbstractRepository):
    """
    A repository that reads staging data from a remote system's BOType.
    """
    def __init__(self, staging_bo_name, condition=""):
        self.staging_bo_name = staging_bo_name
        self.condition = condition

    def get_unprocessed_records(self, tr):
        """
        Yields each BO from the staging BOType that matches the condition.
        """
        log_("Fetching staging data from '%s' with condition: '%s'" %(self.staging_bo_name, self.condition), level = VM.LOG_INFO)
        bo_type = VM.getBOType(self.staging_bo_name)
        return bo_type.createIterator(tr, self.condition)

# ==============================================================================
# 6. RECONCILIATION
# ==============================================================================

class ReconciliationBaseline(AbstractReconciliationBaseline):
    def __init__(self, target_bot_name, active_condition="status == 'ACTIVE'"):
        self.target_bot_name = target_bot_name
        self.active_condition = active_condition

    def get_all_active_records(self, tr):
        return VM.getBOType(self.target_bot_name).createIterator(tr, self.active_condition)

class Reconciler(AbstractReconciler):
    """
    A concrete implementation of a reconciler that deactivates target records
    that are no longer present in the source data.
    """
    def __init__(self, reconciliation_baseline):
        super(Reconciler, self).__init__(reconciliation_baseline)

    def run(self, tr, active_keys):
        log_("--- Starting Reconciliation Process ---", VM.LOG_INFO)
        all_active_targets = self.reconciliation_baseline.get_all_active_records(tr)

        baseline_keys = [bo.getMoniker() for bo in all_active_targets]
        log_("Found %d active target records to check for reconciliation." % len(baseline_keys), VM.LOG_DEBUG)

        for baseline_key in baseline_keys:
            if baseline_key not in active_keys:
                try:
                    self.deactivate_record(tr, VM.findBObjectMoniker(baseline_key, tr))
                    self.deactivated_count += 1
                    log_("Deactivated obsolete record: %s" % baseline_key, VM.LOG_INFO)
                except Exception as e:
                    log_("Failed to deactivate record %s: %s" % (baseline_key, e), VM.LOG_ERROR)
                    raise
        log_("--- Reconciliation Finished ---", VM.LOG_INFO)

    def deactivate_record(self, tr, bo):
        """Deactivates the given record by setting its status to 'INACTIVE'."""
        bo.getBOField("validto").setValue(yesterday())

# ==============================================================================
# 7. MAIN IMPORT ORCHESTRATOR
# ==============================================================================

class ImportOrchestrator(object):
    def __init__(self, factories, reconcilers=None, tr=None):
        self.factories = factories
        self.reconcilers = reconcilers or []
        for f in self.factories:
            assert isinstance(f, AbstractFactory)
        for r in self.reconcilers:
            assert isinstance(r, AbstractReconciler)
        self.transaction = tr if tr else transaction

    def run(self, commit_batch_size=None):
        """
        Runs the import process using the provided factories and reconciles
        the target records if any reconcilers are defined.
        """
        log_("--- Starting Import Orchestrator ---", VM.LOG_INFO)
        active_target_keys = set()
        tr = self.transaction

        try:
            for factory in self.factories:
                factory.process_all(tr, commit_batch_size)
                active_target_keys.update(factory.active_target_keys)
            tr.doCommitResume()
        except:
            log_("A critical error occurred during the main import phase.", VM.LOG_ERROR)
            log_(traceback.format_exc(), VM.LOG_EXCEPTION)
            raise

        try:
            for reconciler in self.reconcilers:
                reconciler.run(tr, active_target_keys)
            tr.doCommitResume()
        except:
            log_("A critical error occurred during the reconciliation phase.", VM.LOG_ERROR)
            log_(traceback.format_exc(), VM.LOG_EXCEPTION)
            raise
        log_("--- Import Orchestrator Finished ---", VM.LOG_INFO)

####################################################################################################
# TEST
####################################################################################################

    
class TestStageSystemProcessor(MappingProcessor):
    __processiong_order__ = ("name", "systype", "status","compsystems")
    name = PlainField(source_field="stageName")
    status = PlainField(source_field="stageStatus")
    systype = RelationField(source_field="stageType",target_bo_name="Systype", target_lookup_field="systype")
    compsystems = RelationField(source_field="component",target_bo_name="Component", target_lookup_field="ident")


def main():
    repo = StagingRepository("XStageTest")
    factory = MappingProcessorFactory(
        repository=repo,
        default_processor_class=TestStageSystemProcessor,
        target_bo_name = "System",
        source_key = "stageName",
        target_key = "name",
    )
    recon_baseline = ReconciliationBaseline("System", "datcre > 2025-07-17")
    reconciler = Reconciler(recon_baseline)
    orchestator = ImportOrchestrator(
        factories=[factory],
        reconcilers=[reconciler],
    )
    orchestator.run()

if __name__ == "__main__":
    main()
