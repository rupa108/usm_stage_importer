# -*- coding: utf-8 -*-
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
from abc import ABCMeta, abstractmethod, abstractproperty
import copy
from de.usu.s3.api import ApiBObject, ApiTransaction, ApiBOType
from typing import Any, List, Tuple, Callable
import traceback


__LOG_DOMAIN__ = "Importer"
undefined = object()
CLEAR_LINK = object()
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

class ValidationError(Exception):
    def __init__(self, message):
        super(ValidationError, self).__init__(message)
        self.message = message

# ==============================================================================
# 1. HELPER FUNCTIONS
# ==============================================================================

def yesterday():
    # TBD
    pass

def set_log_domain(log_domain):
    global __LOG_DOMAIN__
    __LOG_DOMAIN__ = log_domain

def log_(message, level, bo=None):
    # type: (str, int, ApiBObject) -> None
    """
    A centralized logging function that prints to the console for high-level
    messages and writes to the persistent log for all levels.
    """
    level_map = {VM.LOG_INFO: "INFO", VM.LOG_WARN: "WARNING", VM.LOG_ERROR: "ERROR", VM.LOG_DEBUG: "DEBUG", VM.LOG_EXCEPTION: "EXCEPTION", VM.LOG_FINER: "DEBUG_DETAIL", VM.LOG_FINEST: "TRACE"}

    if level in [VM.LOG_INFO, VM.LOG_WARN, VM.LOG_ERROR, VM.LOG_EXCEPTION]:
        print "%s: %s" % (level_map.get(level, "LOG"), message)

    VM.persistentLogMessage(__LOG_DOMAIN__, message, None, None, bo, level, False)


def get_bo(tr, bo_type, condition, trl_type=VM.TRL_CURRENT, strict=False):
    # type: (ApiTransaction, ApiBOType, str, int, bool) -> ApiBObject
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

def link_nm(source, target, rel_name, **kwargs):
    # type: (ApiBObject, ApiBObject, str, **Any) -> ApiBObject
    """We had strage issues with the ApiGOField.linkObject() method in some cases.
    This is a helper method that reliably creates links between two business objects.
    Creates a link between two business objects in a collection relationship.
    If the target is not already linked, it adds it to the collection.
    Returns the link object.
    """
    assert source is not None
    assert target is not None
    f_coll = source.getBOField(rel_name)
    coll = f_coll.getCollection()
    i = coll.indexOf(target)
    if i < 0:
        coll.add(target)
    link = coll.linkItem(coll.indexOf(target))
    if kwargs:
        for key, value in kwargs.items():
            link.getBOField(key).setValue(value)

    return link

# ==============================================================================
# 2. CORE LIBRARY ABSTRACT CLASSES
# ==============================================================================

class AbstractField(object):
    """
    Abstract base class for a declarative mapping field.
   """
    __metaclass__ = ABCMeta
    _creation_counter = 0

    def __init__(self, source_field=None, processor_func=None, match_key=False):
        # type: (str, callable, bool) -> None
        """
        Initializes the field descriptor.
        Arguments:
            source_field (str): The name of the field on the source business object.
            processor_func (callable): An optional function that processes the value
                before setting it on the target business object. The signature should be:
                `processor_func(context: ProcessingContext, source_value: Any) -> Any`
            match_key (bool): Whether this field is a match key for identifying existing target records.
        """
        self.source_field = source_field
        self.target_field = None
        self.processor_func = processor_func
        self.match_key = match_key
        self._creation_counter = AbstractField._creation_counter
        AbstractField._creation_counter += 1

    def set_target_field(self, name):
        self.target_field = name

    def set_target_value(self, context):
        """
        Sets the target field value directly.
        This is used for static values or when the processor function is not needed.
        """
        value = context.get_value(self)
        if not value is undefined:
            target_bo = context.get_target()
            target_bo.getBOField(self.target_field).setValue(value)
        else:
            log_("Value for field '%s' on source BO '%s' is undefined. Skipping mapping."
                  % (self.source_field, context.source.getMoniker()), VM.LOG_FINER)

    @abstractmethod
    def get_processed_value(self, context):
        # type: (ProcessingContext) -> Any
        """
        Gets the processed value without setting it on the target.
        This is useful for lookups where we need the transformed value
        but don't have a target object yet.

        Arguments:
            context (ProcessingContext): The context containing the source business object.

        Returns:
            The processed value that would be set on the target field.
        """
        pass

    @abstractmethod
    def map_value(self, context):
        # type: (ProcessingContext) -> None
        """
        Maps the value from the source to the target field.

        Arguments:
            context (ProcessingContext): The context containing the source and target business objects.
        """
        pass


class ProcessorMetaclass(ABCMeta):
    """
    A metaclass that captures the declaration order of FieldDescriptors
    on RecordProcessor subclasses if `__processing_order__` is defined.
    """
    def __new__(cls, name, bases, attrs):
        class_meta = attrs.pop("Meta", None)
        if class_meta is None:
            meta = None
            for base in bases:
                if hasattr(base, "meta"):
                    meta = copy.deepcopy(getattr(base, "meta"))
                    break
            if meta is None:
                meta = type("Meta", (), {})()
        else:
            meta = class_meta()

        descr_dict = {}
        
        for base in bases:
            if hasattr(base, "meta"):
                base_meta = getattr(base, "meta")
                if hasattr(base_meta, "fields"):
                    for field in base_meta.fields:
                        descr_dict[field.target_field] = field
        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, AbstractField):
                descr_dict[attr_name] = attr_value

        ordered_descriptors = []

        if '__processing_order__' in attrs:
            # If order is specified, enforce it
            processing_order = attrs['__processing_order__']
            for field_name in processing_order:
                try:
                    descriptor = descr_dict[field_name]
                except KeyError:
                    raise TypeError(
                        "Field '%s' listed in `__processing_order__` is not defined in class %s." % (field_name, name)
                    )
                if not isinstance(descriptor, AbstractField):
                    raise TypeError(
                        "Field '%s' listed in `__processing_order__` is not a "
                        "valid FieldDescriptor instance in class %s. %s" % (field_name, name, descriptor)
                    )
                descriptor.target_field = field_name
                ordered_descriptors.append(descriptor)
        else:
            # If no order is specified, discover fields and order as they were declared
            discovered_descriptors = []
            for field_name, descriptor in descr_dict.items():
                descriptor.set_target_field(field_name)
                discovered_descriptors.append(descriptor)
            ordered_descriptors = sorted(discovered_descriptors, key=lambda x: x._creation_counter)

        # remove descriptor fields from their original location in order to clean up the scope
        for attr_name, attr_value in attrs.items():
            if isinstance(attr_value, AbstractField):
                del attrs[attr_name]

        meta.fields = ordered_descriptors
        target_bo_name = getattr(meta, "target_bo_name", None)
        if target_bo_name:
            meta.target_type = VM.getBOType(target_bo_name)
        attrs["meta"] = meta

        return super(ProcessorMetaclass, cls).__new__(cls, name, bases, attrs)


class AbstractProcessor(object):
    """Base class for processing a single record and tracking touched objects."""

    def __init__(self, tr, source_bo, target_bo, **kwargs):
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
        """Returns a set of active keys for the touched objects."""
        keys = set()
        for bo in self._touched_objects:
            keys.add(bo.getMoniker())
        log_("Processor %s collected %d active keys." % (self.__class__.__name__, len(keys)), VM.LOG_DEBUG, self.source)
        return keys

    @classmethod
    def _assert_cached_bo(cls, tr, attr_name, bot, create_attrs, condition):
        # type: (ApiTransaction, str, ApiBOType, dict, str) -> ApiBObject
        """
        Helper method for creating properties of cached business objects.
        Asserts that a cached business object exists, or creates it if not under the given class attribute name.

        Attributes:
            tr (ApiTransaction): The transaction context.
            attr_name (str): The name of the class attribute to store the cached BO.
            bot (ApiBOType): The business object type to create or retrieve.
            create_attrs (dict): Attributes to use when creating a new BO if it does not exist.
            condition (str): Condition to find the BO in the transaction.
        """
        bo = getattr(cls, attr_name)
        if not bo:
            bo = get_bo(tr, bot, condition)
            if not bo:
                generate_key = True if bot.getBusinessKeyAttrName() else False
                bo = bot.createBO(tr, generate_key)
                for att_name, value in create_attrs.items():
                    bo.getBOField(att_name).setValue(value)

            setattr(cls, attr_name, bo)

        if not tr.containsBO(bo):
            bo = tr.get(bo)

        return bo

    @classmethod
    def get_match_key(cls):
        result = None
        for field in getattr(cls.meta, "fields", []):
            if field.match_key:
                result = field
        return result


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

    def __init__(self, source_repository, default_processor_class=None, rules=None):
        # type: (AbstractRepository, AbstractProcessor, List[Callable]) -> None
        self._check_constructor_args(source_repository=source_repository, default_processor_class=default_processor_class, rules=rules)
        self.repository = source_repository
        self.is_chained = False
        self.default_processor_class = default_processor_class
        self.rules = rules if rules else []
        self.processed_count = 0
        self.failed_count = 0
        self.active_target_keys = set()

    @classmethod
    def as_chained(cls, default_processor_class=None, rules=None):
        # type: (AbstractProcessor, List[Callable]) -> AbstractFactory
        """
        Alternative constructor!
        This is used when you need a full blown MappingProcessor for ChainedRelationField().
        The main difference here is that the factory is not fed by its own StageRepository. In
        fact this nested factory has no own repository but gets fed the records from the superior
        repository.

        Subclasses that need additional state initialized for chained processing should
        override :meth:`_init_chained` (which is called by this constructor) rather than
        re-implementing ``as_chained``.
        """
        cls._check_constructor_args(default_processor_class=default_processor_class, rules=rules)
        inst = cls.__new__(cls)
        inst.repository = None
        inst.is_chained = True
        inst.default_processor_class = default_processor_class
        inst.rules = rules if rules else []
        inst.processed_count = 0
        inst.failed_count = 0
        inst.active_target_keys = set()
        inst._init_chained()
        return inst

    def _init_chained(self):
        """
        Hook for subclasses to initialize any additional state required for chained
        processing. The base implementation does nothing.
        """
        pass

    @staticmethod
    def _check_constructor_args(source_repository=None, default_processor_class=None, rules=None):
        if source_repository:
            assert isinstance(source_repository, AbstractRepository), "source_repository must be an instance of AbstractRepository"
        assert default_processor_class or rules, "Either default_processor_class or rules must be given"
        if default_processor_class:
            assert issubclass(default_processor_class, AbstractProcessor), "default_processor_class must be a subclass of AbstractProcessor"

        if rules:
            for func, cls in rules:
                assert callable(func), "Matcher must be a callable function."
                assert issubclass(cls, AbstractProcessor), "Processor class %s must be a subclass of AbstractProcessor" % cls.__name__

    @abstractmethod
    def process_all(self, tr, commit_batch_size=None):
        # type: (ApiTransaction, int) -> None
        """
        Contains the main loop that iterates over all records from the
        repository, delegating the per-record work to :meth:`process_row` and
        taking care of error accounting and batch commits.
        """
        raise NotImplementedError("Subclasses must implement process_all()")

    @abstractmethod
    def process_row(self, tr, row_bo):
        # type: (ApiTransaction, ApiBObject) -> ApiBObject
        """
        Process a single record from the repository and return the resulting
        target business object (or ``undefined`` when the record was skipped).

        This is the natural per-record extension point of a factory. Overriding
        it gives full control over how one record is turned into a target BO,
        while the surrounding :meth:`process_all` loop continues to handle
        iteration, error accounting and commits. For most customizations the
        dedicated hooks (:meth:`prepare_source_record`, :meth:`should_process`,
        :meth:`build_processor`, etc.) are preferable to overriding this method.
        """
        raise NotImplementedError("Subclasses must implement process_row()")

    @abstractmethod
    def get_source_bo(self, tr, row_bo):
        # type: (ApiTransaction, ApiBObject) -> ApiBObject
        """
        Find and return the source business object.
        """
        raise NotImplementedError("Subclasses must implement get_source_bo()")

    @abstractmethod
    def get_target_bo(self, tr, row_bo):
        # type: (ApiTransaction, ApiBObject) -> ApiBObject
        """
        Find and return the target business object.
        """
        raise NotImplementedError("Subclasses must implement get_target_bo()")

    # --------------------------------------------------------------------------
    # Customization hooks
    #
    # These hooks are the intended extension points of a factory. Subclasses can
    # override them to mangle/enrich the data provided by the repository or to
    # customize how processors are instantiated, without having to re-implement
    # the whole `process_all` loop. All base implementations preserve the
    # default behaviour, so overriding is entirely optional.
    # --------------------------------------------------------------------------

    def prepare_source_record(self, tr, row_bo):
        # type: (ApiTransaction, ApiBObject) -> ApiBObject
        """
        Hook to normalize or enrich a raw staging record before it is used.

        Called once per record (after :meth:`get_source_bo`) before the target
        BO is resolved and before mapping happens. Override this to mangle the
        data provided by the repository, e.g. to trim/upcase key fields, derive
        additional values, or look up reference data.

        Returning ``None`` (or :data:`undefined`) signals that the record should
        be skipped.

        The base implementation returns the record unchanged.
        """
        return row_bo

    def should_process(self, tr, source_record):
        # type: (ApiTransaction, ApiBObject) -> bool
        """
        Hook to decide whether a (prepared) source record should be processed.

        Override to implement a per-record filter. Returning ``False`` skips the
        record entirely (no target is resolved and no processor is built).

        The base implementation processes every record.
        """
        return True

    def build_processor(self, tr, processor_class, source_bo, target_bo, **kwargs):
        # type: (ApiTransaction, type, ApiBObject, ApiBObject, **kwargs) -> AbstractProcessor
        """
        Hook that is the single place where processors are instantiated.

        Override this to customize processor construction, e.g. to pass extra
        constructor arguments, inject shared services/caches, or wrap the
        processor instance. All factory code routes processor creation through
        this method.

        The base implementation simply constructs ``processor_class`` with the
        given arguments.
        """
        return processor_class(tr, source_bo, target_bo, **kwargs)

    def on_target_created(self, tr, target_bo, source_record):
        # type: (ApiTransaction, ApiBObject, ApiBObject) -> None
        """
        Hook invoked when a brand new target BO has been created for a record.

        Override for cross-cutting logic on creation (e.g. stamping audit
        fields). The base implementation does nothing.
        """
        pass

    def on_target_found(self, tr, target_bo, source_record):
        # type: (ApiTransaction, ApiBObject, ApiBObject) -> None
        """
        Hook invoked when an existing target BO has been matched for a record.

        Override for cross-cutting logic on update. The base implementation does
        nothing.
        """
        pass

    def on_record_failed(self, tr, row_bo, exc):
        # type: (ApiTransaction, ApiBObject, Exception) -> None
        """
        Hook invoked when processing a record raised an exception.

        Called after the framework has accounted for the failure (counters,
        logging). Override for custom error handling, such as writing the error
        back to the staging record. The base implementation does nothing.
        """
        pass

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
        # type: (ApiTransaction, set) -> None
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
        # type: (ApiTransaction, ApiBObject) -> None
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
        self.processor = processor
        self.source_field_name = source_field_name
        self.target_field_name = target_field_name
        self._value_store = {}

    def store_value(self, field, value):
        # type: (AbstractField, Any) -> None
        field_name = field.target_field
        self._value_store[field_name] = value

    def get_value(self, field):
        # type: (AbstractField) -> Any
        field_name = field.target_field
        return self._value_store.get(field_name, undefined)

    def get_source(self):
        return self.processor.source
    source = property(get_source)

    def get_target(self):
        return self.processor.target
    target = property(get_target)

    def add_touched_object(self, bo):
        self.processor.add_touched_object(bo)

    def get_transaction(self):
        return self.processor.transaction
    transaction = property(get_transaction)

    @property
    def is_create(self):
        return self.processor.is_create

    @property
    def is_update(self):
        return self.processor.is_update

class MappingProcessor(AbstractProcessor):
    """Default implementation of a Processor"""
    __metaclass__ = ProcessorMetaclass

    _generate_key = None

    def __init__(self, tr, source_bo, target_bo, is_create=False):
        super(MappingProcessor, self).__init__(tr, source_bo, target_bo)
        self.add_touched_object(target_bo)
        self.is_create = is_create
        self.is_update = not is_create

    def process(self):
        log_("Applying declarative mappings using %s..." % self.__class__.__name__, VM.LOG_FINER, self.source)
        if not hasattr(self, '__processing_order__'):
            log_("`__processing_order__` not defined for %s. Field processing order is not guaranteed." % self.__class__.__name__, VM.LOG_FINER, self.source)

        queue = []
        for descriptor in self.meta.fields:
            try:
                context = ProcessingContext(self, descriptor.source_field, descriptor.target_field)
                descriptor.map_value(context)
            except ValidationError as e:
                raise
            except Exception as e:
                log_("Could not map field '%s' to target '%s': %s" % (descriptor.source_field, descriptor.target_field, e), VM.LOG_WARN, self.source)
            else:
                queue.append((descriptor, context))

        for descriptor, context in queue:
            try:
                descriptor.set_target_value(context)
            except Exception as e:
                log_("Could not save value form '%s' to target '%s': %s" % (descriptor.source_field, descriptor.target_field, e), VM.LOG_WARN, self.source)



    @classmethod
    def generate_key(cls):
        if cls._generate_key is None:
            target_type = getattr(cls.meta, "target_type", None)
            if target_type:
                return True if target_type.getBusinessKeyAttrName() else False
        else:
            cls._generate_key

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

    def __init__(self, source_field, processor_func=None, match_key=False, **kwargs):
        # type: (str, callable, bool, **Any) -> None
        """
        Initializes the field descriptor.
        Arguments:
            source_field (str): The name of the field on the source business object.
            processor_func (callable): An optional function that processes the value
                before setting it on the target business object. The signature should be:
                `processor_func(context: ProcessingContext, source_value: Any) -> Any`
        """
        super(PlainField, self).__init__(source_field=source_field, processor_func=processor_func, match_key=match_key, **kwargs)

    def get_processed_value(self, context):
        source_bo = context.get_source()
        source_value = source_bo.getBOField(self.source_field).getValue()
        if self.processor_func:
            return self.processor_func(context, source_value)
        else:
            return source_value

    def map_value(self, context):
        final_value = self.get_processed_value(context)
        context.store_value(self, final_value)

class StaticField(AbstractField):
    """A descriptor for setting a static, predefined value on a target field."""
    def __init__(self, value=undefined, processor_func=None, **kwargs):
        if value is undefined:
            assert processor_func, "StaticField must have a value or a processor function defined."
        super(StaticField, self).__init__(processor_func=processor_func, **kwargs)
        self.value = value

    def get_processed_value(self, context):
        if self.processor_func:
            return self.processor_func(context, self.value)
        else:
            return self.value

    def map_value(self, context):
        final_value = self.get_processed_value(context)
        context.store_value(self, final_value)


class RelationField(AbstractField):
    """
    A descriptor for mapping a value to a related Business Object.
    It supports find-or-create logic for the related object.
    """
    def __init__(self, source_field, target_bo_name, target_lookup_field=None, on_not_found_create=None, processor_func=None, target_lookup_func=None, **kwargs):
        super(RelationField, self).__init__(source_field, processor_func)
        self.target_bo_name = target_bo_name
        self.target_type = VM.getBOType(target_bo_name)
        self.target_lookup_field = target_lookup_field
        self.on_not_found_create = on_not_found_create
        self.generate_key = True if self.target_type.getBusinessKeyAttrName() else False
        self.target_lookup_func = target_lookup_func
        self.kwargs = kwargs

    def _create_related_bo(self, context, lookup_value):
        """Creates a new related BO based on the on_not_found_create config."""
        source_bo = context.get_source()
        log_("Creating new related object for '%s' in BO '%s'" % (lookup_value, self.target_bo_name), VM.LOG_INFO, source_bo)

        attribute_dict = {}
        for field, value_source in self.on_not_found_create.items():
            if not isinstance(value_source, ValueSource):
                raise TypeError("Value for 'on_not_found_create' must be an instance of a ValueSource class (Static, FromSource, etc.)")

            value = value_source.get_value(context)
            if value is None:
                continue
            attribute_dict[field] = value

        tr = context.get_transaction()
        new_bo = self.target_type.createBO(tr, self.generate_key)

        for field, value in attribute_dict.items():
            bo_field = new_bo.getBOField(field)
            if bo_field.isCollectionLink():
                rel_bo = link_nm(new_bo, value, field)
                if rel_bo:
                    context.add_touched_object(rel_bo)
            elif bo_field.isObjectLink():
                bo_field.setObject(value)
            else:
                bo_field.setValue(value)

        return new_bo

    def get_lookup_value(self, context):
        source_bo = context.get_source()
        return source_bo.getBOField(self.source_field).getValue()

    def map_value(self, context):
        """Resolve the related BO for this mapping and store it on the context.

        Reads the lookup value from the source BO and determines the related
        target BO using one of the configured resolution strategies (in order
        of precedence):

        1. ``processor_func`` — custom callable that returns the related BO,
           ``undefined`` to skip the mapping, or a falsy value if no result.
        2. ``target_lookup_field`` — build a simple equality condition against
           the configured field and query for the related BO.
        3. ``target_lookup_func`` — custom callable that builds the query
           condition dynamically.

        If no BO is found via strategies 2 or 3 and ``on_not_found_create`` is
        enabled, a new related BO is created via ``_create_related_bo``.

        The value stored on the context is one of:

        * ``CLEAR_LINK`` — the source lookup value is empty and no
          ``target_lookup_func`` is configured; the target ObjectLink should
          be cleared.
        * A related BO instance — resolved successfully; should be applied to
          the target field.
        * ``undefined`` — nothing actionable (processor skipped, lookup
          failed, or no strategy applied); the target field should be left
          untouched.

        A DEBUG message is logged when a lookup strategy fails to find or
        create a related BO despite a non-empty lookup value.

        Note:
            This method only resolves and stores the related BO. Applying it
            to the target field is the responsibility of
            :meth:`set_target_value`.

        Args:
            context: The mapping context providing access to the source BO,
                target BO, transaction, and value storage.
        """

        result = self.get_processed_value(context)
        context.store_value(self, result)

    def get_processed_value(self, context):
        """Resolve the related BO for this mapping without storing it.

        Applies the same resolution strategies as :meth:`map_value` (processor
        function, ``target_lookup_field`` or ``target_lookup_func``, optionally
        followed by find-or-create) and returns the resolved value. This is the
        value :meth:`map_value` stores on the context and is also used when a
        ``RelationField`` acts as a match key (see
        ``MappingProcessorFactory.get_target_bo``).

        Returns one of:

        * ``CLEAR_LINK`` — the source lookup value is empty and no
          ``target_lookup_func`` is configured.
        * A related BO instance — resolved successfully.
        * ``undefined`` — nothing actionable (processor skipped, lookup failed,
          or no strategy applied).

        Args:
            context: The mapping context providing access to the source BO,
                target BO, transaction, and value storage.
        """
        lookup_value = self.get_lookup_value(context)

        result = undefined

        if not lookup_value and not self.target_lookup_func:
            result = CLEAR_LINK
        elif self.processor_func:
            result = self.processor_func(context, lookup_value)
        else:
            tr = context.get_transaction()
            if self.target_lookup_field:
                condition = "%s == '%s'" % (self.target_lookup_field, lookup_value)
            elif self.target_lookup_func:
                condition = self.target_lookup_func(context, lookup_value)
            else:
                condition = None

            if condition is not None:
                related_bo = get_bo(tr, self.target_type, condition, strict=True)
                if not related_bo and self.on_not_found_create and lookup_value:
                    related_bo = self._create_related_bo(context, lookup_value)

                if related_bo:
                    result = related_bo
                elif lookup_value:
                    log_("Could not find or create a related object for '%s' in BO '%s'"
                         % (lookup_value, self.target_bo_name), VM.LOG_DEBUG, context.source)
                    # result stays undefined

        return result


    def set_target_value(self, context):
        """Apply the previously resolved related BO to the target field.

        Consumes the value stored on the context by :meth:`map_value` and
        updates the target BO accordingly:

        * ``undefined`` — the mapping is skipped (a FINER log entry is
          emitted) and the target field is left untouched.
        * ``CLEAR_LINK`` — if the target field is an ObjectLink, it is
          cleared via ``setObject(None)``.
        * A related BO — applied to the target field based on its type:

            - **CollectionLink**: a link BO is created via ``link_nm``, any
              configured ``kwargs`` (including ``ValueSource`` values
              resolved against the context) are written to it, and the link
              BO is registered as touched on the context.
            - **ObjectLink**: the related BO is assigned via
              ``setObject(related_bo)``.

          The related BO itself is also registered as touched on the
          context.

        Note:
            :meth:`map_value` must have been called on the same context
            beforehand to populate the stored value.

        Args:
            context: The mapping context providing access to the target BO
                and the previously stored related BO.
        """
        related_bo = context.get_value(self)

        if related_bo is undefined:
            log_("Processor function returned undefined for '%s'. Skipping mapping."
                 % self.source_field, VM.LOG_FINER, context.get_source())
            return

        target_bo = context.get_target()
        target_field = target_bo.getBOField(self.target_field)

        if related_bo is CLEAR_LINK:
            if target_field.isObjectLink() and not target_field.isCollectionLink():
                target_field.setObject(None)
            return

        if target_field.isCollectionLink() and target_bo and related_bo:
            link_bo = link_nm(target_bo, related_bo, self.target_field)
            if link_bo:
                for f, v in self.kwargs.items():
                    val = v.get_value(context) if isinstance(v, ValueSource) else v
                    link_bo.getBOField(f).setValue(val)
                context.add_touched_object(link_bo)
        elif target_field.isObjectLink():
            target_field.setObject(related_bo)

        context.add_touched_object(related_bo)

class ChainedRelationField(RelationField):
    """
    A specialized RelationField that uses an internal factory to resolve the related BO.
    This is useful for handling more complex relationships that require multiple fields
    or custom logic to resolve.
    """
    def __init__(self, source_field, processor_or_factory, **kwargs):
        if isinstance(processor_or_factory, AbstractFactory):
            self.factory = processor_or_factory
            assert self.factory.is_chained, \
                "The factory passed to ChainedRelationField must be built via as_chained()."
        elif isinstance(processor_or_factory, type) and issubclass(processor_or_factory, AbstractProcessor):
            self.factory = MappingProcessorFactory.as_chained(default_processor_class=processor_or_factory)
        else:
            raise TypeError(
                "processor_or_factory must be a chained AbstractFactory or an "
                "AbstractProcessor subclass, got %r" % (processor_or_factory,))

        # The target BO type is dictated by the chained factory's processor mapping.
        target_type = self.factory.target_type
        assert target_type is not None, \
            "Could not determine the target BO type for the chained factory."
        target_bo_name = target_type.getName()

        super(ChainedRelationField, self).__init__(
            source_field, target_bo_name, **kwargs)

    def map_value(self, context):
        lookup_value = self.get_lookup_value(context)

        if not lookup_value:
            result = CLEAR_LINK
        else:
            result = self.factory.process_chained(context)

        if result is not undefined:
            context.store_value(self, result)
 

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
    def __init__(self, value, processor_func=None):
        self.value = value
        self.processor_func = processor_func

    def get_value(self, context):
        if self.processor_func:
            return self.processor_func(context, self.value)
        else:
            return self.value

class FromSource(ValueSource):
    """An instruction to get a value from a field on the source record."""
    def __init__(self, source_field, processor_func=None):
        self.source_field = source_field
        self.processor_func = processor_func

    def get_value(self, context):
        source_bo = context.get_source()
        value = source_bo.getBOField(self.source_field).getValue()
        if self.processor_func:
            value = self.processor_func(context, value)

        return value

class FromAnywhere(ValueSource):
    """
    An instruction to get a value by executing a custom function.
    The signature of the function should be:
        `callable_func(context: ProcessingContext) -> Any`
    """
    def __init__(self, processor_func):
        self.processor_func = processor_func

    def get_value(self, context):
        return self.processor_func(context, None)



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
        rel_bo = None
        rel_attr_name = type(self).rel_attr_name
        source_bo = self.source
        target_bo = self.target
        rel_field = source_bo.getBOField(rel_attr_name)
        if rel_field.isCollectionLink():
            if target_bo:
                rel_bo = link_nm(source_bo, target_bo, rel_attr_name)
                if rel_bo:
                    self.add_touched_object(rel_bo)
        else:
            rel_field.setObject(target_bo)

        return rel_bo

    def pre_process(self): pass

    def post_process(self): pass

    def add_touched_object(self, bo):
        super(RelationshipProcessor, self).add_touched_object(bo)

    def get_active_keys(self):
        return super(RelationshipProcessor, self).get_active_keys()

    # Let abc take care of checking subclasses define this property as documented above.
    @abstractproperty
    def rel_attr_name(cls): pass

class _RulesMixin(object):
    def _get_processor_class(self, tr, staging_record):
        matching_classes = [p_class for matcher, p_class in self.rules if matcher(staging_record)]

        if len(matching_classes) > 1:
            raise AmbiguousProcessorError(
                "More than one processor rule matched for record.",
                matching_processors=[cls.__name__ for cls in matching_classes]
            )

        processor_class = matching_classes[0] if matching_classes else self.default_processor_class

        if processor_class:
            log_("Selected processor '%s' for record." % processor_class.__name__, VM.LOG_FINE, staging_record)

        return processor_class

class RelationProcessorFactoryBase(_RulesMixin, AbstractFactory):
    """
    This is a base class for a relationship processor factory that MUST be subclassed.
    It handles relationships between business objects.
    This factory processes records from a repository and creates relationships
    between source and target business objects based on the defined rules.

    Arguments:
        source_repository (AbstractRepository): An object that provides the relationship data rows.
        default_processor_class (AbstractProcessor): A processor that creates the relation betwee given
        source and target bos.
        rules (list of tuples): A list of tuples where each tuple contains a matcher function and
            a processor class. The matcher function should take a staging record and return True if it matches
            the rule. The processor class should be a subclass of AbstractProcessor that will handle the matched
            records. If no rules are provided, the default_processor_class will be used for all records.
            The signature of the matcher function should be:
            `matcher(staging_record: Any) -> bool`
    """
    ###########################################
    # Subclasses must define these attributes #
    source_bo_name = None
    source_bo_key_attribute = None
    stage_bo_source_attribute = None

    target_bo_name = None
    target_bo_key_attribute = None
    stage_bo_target_attribute = None
    ###########################################

    def __init__(self, source_repository, default_processor_class=None, rules=None):
        # type: (AbstractRepository, AbstractProcessor, List[Tuple]) -> None
        super(RelationProcessorFactoryBase, self).__init__(source_repository, default_processor_class, rules)
        cls = type(self)
        self.source_bo_type = VM.getBOType(cls.source_bo_name)
        self.target_bo_type = VM.getBOType(cls.target_bo_name)

        self.rules = rules if rules else []
        for func, cls in self.rules:
            assert callable(func), "Matcher must be a callable function."
            assert issubclass(cls, AbstractProcessor), "Processor class must be a subclass of AbstractProcessor"


    def process_all(self, tr, commit_batch_size=None):
        """
        Executes the relationship import process.
        """
        log_("--- Starting Relationship Import Phase ---", VM.LOG_INFO)

        for row_bo in self.repository.get_unprocessed_records(tr):

            try:
                self.process_row(tr, row_bo)
            except Exception as e:
                self.failed_count += 1
                if isinstance(e, AmbiguousProcessorError):
                    error_message = "%s: %s" % (type(e).__name__, e.message)
                    error_message += " Conflicting processors: %s" % e.matching_processors
                elif isinstance(e, ValidationError):
                    error_message = "%s: %s" % (type(e).__name__, e.message)
                else:
                    error_message = str(e)
                log_(error_message, VM.LOG_ERROR, row_bo)
                # Hook C: custom error handling.
                self.on_record_failed(tr, row_bo, e)
            except:
                log_("Failed to process %s" % row_bo.getMoniker(), VM.LOG_ERROR, row_bo)
                log_(traceback.format_exc(), VM.LOG_EXCEPTION, row_bo)
                self.failed_count += 1
            else:
                self.processed_count += 1

    def get_source_condition(self, tr, row_bo):
        cls = type(self)
        source_condition = "%s == '%s'" % (
            cls.source_bo_key_attribute,
            row_bo.getBOField(cls.stage_bo_source_attribute).getValue()
        )
        return source_condition

    def get_target_condition(self, tr, row_bo):
        cls = type(self)
        target_condition = "%s == '%s'" % (
            cls.target_bo_key_attribute,
            row_bo.getBOField(cls.stage_bo_target_attribute).getValue()
        )
        return target_condition

    def get_source_bo(self, tr, row_bo):
        source_condition = self.get_source_condition(tr, row_bo)
        source_bo = get_bo(tr, self.source_bo_type, source_condition, strict=True)

        return source_bo

    def get_target_bo(self, tr, row_bo):
        target_condition = self.get_target_condition(tr, row_bo)
        target_bo = get_bo(tr, self.target_bo_type, target_condition, strict=True)

        return target_bo

    def process_row(self, tr, row_bo):
        # type: (ApiTransaction, ApiBObject) -> ApiBObject
        """
        Processes a single relationship row from the data source: resolves the
        source and target BOs and creates the relationship between them.

        This is the per-record extension point of the relationship factory. The
        surrounding :meth:`process_all` loop calls it for every row and handles
        error accounting. Returns the related target BO, or :data:`undefined`
        when the row was skipped.
        """
        # Hook B: allow subclasses to mangle/enrich the raw relationship row.
        row_bo = self.prepare_source_record(tr, row_bo)
        if row_bo is None or row_bo is undefined:
            return undefined
        # Hook C: per-record filter.
        if not self.should_process(tr, row_bo):
            log_("Skipping relationship row '%s' (should_process returned False)." % row_bo.getMoniker(), VM.LOG_DEBUG, row_bo)
            return undefined
        source_bo = self.get_source_bo(tr, row_bo)
        target_bo = self.get_target_bo(tr, row_bo)
        if not source_bo or not target_bo:
            log_("Source or target BO not found for row: %s" % row_bo.getMoniker(), VM.LOG_WARN, row_bo)
            return undefined
        ProcessorClass = self._get_processor_class(tr, row_bo)
        # Hook A: route processor instantiation through the build hook.
        processor = self.build_processor(tr, ProcessorClass, source_bo, target_bo, row_bo=row_bo)
        processor.pre_process()
        processor.process()
        processor.post_process()
        self.active_target_keys.add(source_bo.getMoniker())
        self.active_target_keys.add(target_bo.getMoniker())
        self.active_target_keys.update(processor.get_active_keys())
        return target_bo

    def _process_row(self, tr, row_bo):
        """Deprecated alias for :meth:`process_row`, kept for backward compatibility."""
        return self.process_row(tr, row_bo)

    def get_summary(self):
        return "Processed relationship links: %d\nFailed rows: %d" % (self.processed_count, self.failed_count)

    # Let abc take care of checking subclasses define these properties as documented above.
    @abstractproperty
    def source_bo_name(cls): pass
    @abstractproperty
    def source_bo_key_attribute(cls): pass
    @abstractproperty
    def stage_bo_source_attribute(cls): pass
    @abstractproperty
    def target_bo_name(cls): pass
    @abstractproperty
    def target_bo_key_attribute(cls): pass
    @abstractproperty
    def stage_bo_target_attribute(cls): pass

# ==============================================================================
# 5. CONCRETE FACTORY AND REPOSITORY IMPLEMENTATIONS
# ==============================================================================

class MappingProcessorFactory(_RulesMixin, AbstractFactory):
    """A factory that processes records from a repository and applies
    mapping rules to create or update target business objects.
    This factory uses a rule-based system to select the appropriate processor
    for each record based on the defined rules.
    Arguments:
        repository (AbstractRepository): An object that provides the records to be processed.
        default_processor_class (AbstractProcessor): The default processor class to use for processing source records.
        target_bo_name (str): The name of the target business object type.
        source_key (str): The key field on the source business object to match against the target.
        target_key (str): The key field on the target business object to match against the source.
        rules (list of tuples): A list of tuples where each tuple contains a matcher function and
            a processor class. The matcher function should take a staging record and return True if it matches
            the rule. The processor class should be a subclass of AbstractProcessor that will handle the matched
            records. If no rules are provided, the default_processor_class will be used for all records.
            The signature of the matcher function should be:
            `matcher(staging_record: Any) -> bool`
    """
    def __init__(self, repository, default_processor_class=None, target_bo_name=None, source_key=None, target_key=None, rules=None):
        # type: (AbstractRepository, AbstractProcessor, str, str, str, List[Tuple]) -> None
        super(MappingProcessorFactory, self).__init__(repository, default_processor_class=default_processor_class, rules=rules)
        self._setup_mapping(target_bo_name=target_bo_name, source_key=source_key, target_key=target_key)

    def _init_chained(self):
        # When built via as_chained() the regular __init__ (and therefore _setup_mapping)
        # has not run, so the mapping-specific attributes must be initialized here.
        self._setup_mapping()

    def _setup_mapping(self, target_bo_name=None, source_key=None, target_key=None):
        self.generate_key = None
        self.target_type = None
        if self.default_processor_class:
            self.target_type = getattr(self.default_processor_class.meta, "target_type", None)

        if not self.target_type and target_bo_name:
            self.target_type = VM.getBOType(target_bo_name)

        self.source_key = source_key
        self.target_key = target_key
        self.match_key_field = None
        processor_classes = [c for f, c in self.rules]
        if self.default_processor_class:
            processor_classes.append(self.default_processor_class)

        for cls in processor_classes:
            rules_target_type = getattr(cls.meta, "target_type", None)
            assert rules_target_type or self.target_type, \
                "No target_bo_name on processor class %s. target_bo_name argument must be provided!" % cls.__name__
            field = cls.get_match_key()
            if not field:
                assert self.source_key, \
                    "No match_key in processor class %s. source_key argument must be provided!" % cls.__name__
                assert self.target_key, \
                    "No match_key in processor class %s. target_key argument must be provided!" % cls.__name__

    def get_source_bo(self, tr, row_bo):
        return row_bo

    def get_target_bo(self, tr, row_bo, processor_class):
        match_key_field = processor_class.get_match_key() or self.match_key_field
        source_key = match_key_field.source_field if match_key_field else self.source_key
        target_key = match_key_field.target_field if match_key_field else self.target_key
        target_type = getattr(processor_class.meta, "target_type", None) or self.target_type

        key_value = row_bo.getBOField(source_key).getValue()

        # Apply field processing if the match_key field has a processor_func
        if match_key_field and match_key_field.processor_func:
            # Create a minimal context for the field's get_processed_value method
            class MinimalContext:
                def __init__(self, transaction, source, source_field, target_field):
                    self.transaction = transaction
                    self.source = source
                    self.source_field_name = source_field
                    self.target_field_name = target_field
                    self.is_create = False  # This is for lookup, not creation
                    self.is_update = False

                def get_source(self):
                    return self.source

                def get_target(self):
                    return None  # No target yet during lookup

                def get_transaction(self):
                    return self.transaction

                def add_touched_object(self, bo):
                    pass  # Not needed during lookup

            context = MinimalContext(tr, row_bo, source_key, target_key)
            # Use the field's get_processed_value method instead of calling processor_func directly
            key_value = match_key_field.get_processed_value(context)

        condition = "%s == '%s'" % (target_key, key_value)
        target_bo = get_bo(tr, target_type, condition, strict=True)

        return target_bo

    def get_or_create_target(self, tr, staging_record, processor_class):
        target_bo = self.get_target_bo(tr, staging_record, processor_class)
        if not target_bo:
            generate_key = self.generate_key if self.generate_key is not None else processor_class.generate_key()
            target_type = getattr(processor_class.meta, "target_type", None) or self.target_type
            target_bo = target_type.createBO(tr, generate_key)
            created = True
        else:
            created = False

        return target_bo, created

    def process_all(self, tr, commit_batch_size=None):
        iterator = self.repository.get_unprocessed_records(tr)
        if commit_batch_size:
            iterator.committedAfter(commit_batch_size)

        for record in iterator:
            # The per-record work lives in process_row(); the loop is only
            # responsible for iteration, error accounting and commits. The private
            # _process_record() also reports whether a brand new BO was created so
            # that it can be rolled back on a ValidationError.
            created = None
            target_bo = None
            try:
                target_bo, created = self._process_record(tr, record)
                if target_bo is undefined:
                    continue
                self._mark_as_processed(record, "PROCESSED")
                self.processed_count += 1
            except Exception as e:
                self.failed_count += 1
                if isinstance(e, AmbiguousProcessorError):
                    error_message = "%s: %s" % (type(e).__name__, e.message)
                    error_message += " Conflicting processors: %s" % e.matching_processors
                elif isinstance(e, ValidationError):
                    error_message = "%s: %s" % (type(e).__name__, e.message)
                    if created and target_bo and target_bo is not undefined:
                        target_bo.remove()
                else:
                    error_message = None
                    log_(traceback.format_exc(), VM.LOG_EXCEPTION, record)
                self._mark_as_processed(record, "FAILED", error_message)
                if error_message:
                    log_(error_message, VM.LOG_ERROR, record)
                # Hook C: custom error handling.
                self.on_record_failed(tr, record, e)

    def process_row(self, tr, record):
        # type: (ApiTransaction, ApiBObject) -> ApiBObject
        """
        Resolve (find-or-create) and map the target BO for a single staging
        record, running the full processor lifecycle.

        This is the per-record extension point of the mapping factory. The
        surrounding :meth:`process_all` loop calls it for every record and takes
        care of error accounting and commits, while :meth:`process_chained`
        reuses it for chained relations.

        Returns the resolved/mapped target BO, or :data:`undefined` when the
        record was skipped (filtered out, no matching processor, or a skipped
        update).
        """
        target_bo, _created = self._process_record(tr, record)
        return target_bo

    def _process_record(self, tr, record):
        # type: (ApiTransaction, ApiBObject) -> tuple
        """
        Internal worker for :meth:`process_row` that additionally reports whether
        the target BO was newly created.

        Returns a ``(target_bo, created)`` tuple. ``target_bo`` is :data:`undefined`
        when the record was skipped, in which case ``created`` is ``False``.
        """
        # Hook B: allow subclasses to mangle/enrich the raw staging record.
        source_record = self.prepare_source_record(tr, self.get_source_bo(tr, record))
        if source_record is None or source_record is undefined:
            log_("Skipping record '%s' (prepare_source_record returned no record)." % record.getMoniker(), VM.LOG_DEBUG, record)
            return undefined, False

        # Hook C: per-record filter.
        if not self.should_process(tr, source_record):
            log_("Skipping record '%s' (should_process returned False)." % record.getMoniker(), VM.LOG_DEBUG, record)
            return undefined, False

        processor_class = self._get_processor_class(tr, source_record)
        if not processor_class:
            log_("No processor class matched for record '%s'." % source_record.getMoniker(), VM.LOG_WARN, source_record)
            return undefined, False

        target_bo, created = self.get_or_create_target(tr, source_record, processor_class)
        if created:
            log_("Created new target object '%s' for record." % target_bo.getMoniker(), VM.LOG_DEBUG, record)
            # Hook C: notify on creation.
            self.on_target_created(tr, target_bo, source_record)
        else:
            if self._skip_update(target_bo):
                log_("Skipping update of '%s' for record '%s'." % (target_bo.getMoniker(), record.getMoniker()), VM.LOG_DEBUG, target_bo)
                return undefined, False
            # Hook C: notify on match.
            self.on_target_found(tr, target_bo, source_record)

        # Hook A: route processor instantiation through the build hook.
        processor_instance = self.build_processor(tr, processor_class, source_record, target_bo, is_create=created)
        processor_instance.pre_process()
        processor_instance.process()
        processor_instance.post_process()

        self.active_target_keys.update(processor_instance.get_active_keys())

        return target_bo, created

    def process_chained(self, context):
        # type: (ProcessingContext) -> ApiBObject
        """
        Resolve (find-or-create) and map a single related target BO for a chained
        relationship, driven by the parent's source record.

        Unlike :meth:`process_all`, this does not iterate a repository. It uses the
        source BO provided by ``context`` (the parent processor's source record) as
        the staging record, selects the appropriate processor class, finds or creates
        the related target BO, runs the full processor lifecycle on it, and returns
        the resolved target BO.

        Any objects touched while mapping the related BO are registered on the parent
        ``context`` so that reconciliation correctly treats them as active.

        Returns:
            The resolved related target BO, or ``undefined`` if the record could not
            be processed.
        """
        assert self.is_chained, "process_chained() may only be called on a factory built via as_chained()."

        tr = context.get_transaction()
        # Reuse the shared per-record logic. The parent's source record acts as
        # the staging record here; process_row() handles preparation, filtering,
        # find-or-create and the full processor lifecycle.
        target_bo = self.process_row(tr, context.get_source())
        if target_bo is undefined:
            return undefined

        # Propagate touched objects up to the parent context so the related BO (and
        # anything it touched) is counted as active during reconciliation. The keys
        # were already collected onto self.active_target_keys by process_row().
        context.add_touched_object(target_bo)

        return target_bo

    def _skip_update(self, target_bo):
        if target_bo.getBOFields().contains("ifUpdate"):
            return False if target_bo.getBOField("ifUpdate").getValue() else True
        else:
            return False

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
    def __init__(self, target_bo_name, active_condition="status == 'ACTIVE'"):
        self.target_bo_name = target_bo_name
        self.active_condition = active_condition

    def get_all_active_records(self, tr):
        return VM.getBOType(self.target_bo_name).createIterator(tr, self.active_condition)

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

