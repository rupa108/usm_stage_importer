# -*- coding: utf-8 -*-
"""In-memory fakes for the USM API.

These classes emulate just enough of the USM ``de.usu.s3.api`` surface
(``ApiBObject``, ``ApiBOType``, ``ApiTransaction`` and the global ``VM``) to be
able to exercise :mod:`stage_importer_framework` without the real USM runtime.

They are intentionally simple and store everything in plain Python data
structures so that unit tests can inspect the resulting state directly.

The module is Python 2 / Jython compatible (the framework targets that runtime).
"""


# ==============================================================================
# Business object field
# ==============================================================================
class ConfigurationError(BaseException):
    pass

class FakeBoField(object):
    """A single attribute of a :class:`FakeBo`.

    A field can hold a scalar value, a single linked object (ObjectLink) or a
    collection of linked objects (CollectionLink). Which kind of link it is is
    declared on the owning BO type via ``object_link_fields`` /
    ``collection_fields``.
    """

    def __init__(self, name, is_object_link=False, is_collection_link=False):
        self.name = name
        self.value = None
        self.object = None
        self._is_object_link = is_object_link
        self._is_collection_link = is_collection_link
        self._collection = FakeCollection() if is_collection_link else None

    # --- scalar value ---------------------------------------------------------
    def getValue(self):
        return self.value

    def setValue(self, value):
        self.value = value

    # --- object / collection link ---------------------------------------------
    def isObjectLink(self):
        return self._is_object_link or self._is_collection_link

    def isCollectionLink(self):
        return self._is_collection_link

    def setObject(self, obj):
        self.object = obj

    def getObject(self):
        return self.object

    def getCollection(self):
        if self._collection is None:
            self._collection = FakeCollection()
        return self._collection

    def __repr__(self):
        return "<FakeBoField %s: %s>" % (self.name, self.value)

    def __str__(self):
        return self.__repr__()


class FakeCollection(object):
    """A minimal stand-in for a USM collection field."""

    def __init__(self, items=None):
        self._items = items or []
        assert isinstance(self._items, list)
        self._links = {}

    def indexOf(self, target):
        try:
            return self._items.index(target)
        except ValueError:
            return -1

    def add(self, target):
        self._items.append(target)
        return len(self._items) - 1

    def linkItem(self, index):
        link = self._links.get(index)
        if link is None:
            link = FakeBo("__Link")
            self._links[index] = link
        return link

    def isEmpty(self):
        return len(self._items) == 0

    def size(self):
        return len(self._items)

    def items(self):
        return list(self._items)
    
    def get(self, index):
        return self._items[index]
    
    def __repr__(self):
        return "<FakeCollection %r>" % (self._items,)
    
    def __iter__(self):
        return iter(self._items)


class FakeBoFields(object):
    """Represents the set of fields of a BO (returned by ``getBOFields``)."""

    def __init__(self, bo):
        self._bo = bo

    def contains(self, field_name):
        return field_name in self._bo._field_names()


# ==============================================================================
# Business object
# ==============================================================================

class FakeBo(dict):
    """A business object backed by a plain ``dict``.

    Fields are created lazily on first access via :meth:`getBOField`. Whether a
    field behaves as an ObjectLink or CollectionLink is determined by the owning
    :class:`FakeBOType` (see its ``object_link_fields`` / ``collection_fields``).
    """

    _moniker_counter = 0

    def __init__(self, bo_type, iterable=None, bo_type_obj=None):
        super(FakeBo, self).__init__(iterable if iterable is not None else {})
        self['__type'] = bo_type
        self._bo_type_obj = bo_type_obj
        self._removed = False
        FakeBo._moniker_counter += 1
        self._moniker = "%s:%d" % (bo_type, FakeBo._moniker_counter)

    def getBOField(self, field_name):
        if field_name not in self:
            is_obj = is_coll = False
            if self._bo_type_obj is not None:
                is_coll = field_name in self._bo_type_obj.collection_fields
                is_obj = field_name in self._bo_type_obj.object_link_fields or is_coll
            self[field_name] = FakeBoField(field_name, is_object_link=is_obj, is_collection_link=is_coll)
        return self[field_name]

    def getBOFields(self):
        return FakeBoFields(self)

    def _field_names(self):
        return [k for k in self.keys() if not k.startswith('__')]

    def getBOType(self):
        return self['__type']

    def getMoniker(self):
        return self._moniker

    def remove(self):
        self._removed = True

    def __hash__(self):
        return hash(self._moniker)

    def __eq__(self, other):
        return isinstance(other, FakeBo) and other._moniker == self._moniker

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "<FakeBo %s: %s>" % (
            self['__type'],
            dict((k, v) for k, v in self.items() if not k.startswith('__')),
        )

    def __str__(self):
        return self.__repr__()


# ==============================================================================
# Find result / iterator
# ==============================================================================

class FakeFindResult(object):
    """Result of ``ApiBOType.find`` - exposes the first match and an is-more flag."""

    def __init__(self, matches):
        self._matches = list(matches)

    def getBObject(self):
        return self._matches[0] if self._matches else None

    def isMore(self):
        return len(self._matches) > 1
    
    def isOne(self):
        return len(self._matches) == 1

    def isEmpty(self):
        return len(self._matches) == 0


class FakeFilter(object):
    """Result of ``createFilterForNewObjects`` - the new (uncommitted) matches."""

    def __init__(self, matches):
        self._matches = list(matches)

    def isEmpty(self):
        return len(self._matches) == 0

    def get(self, index):
        return self._matches[index]


class FakeIterator(object):
    """Iterates over BOs; supports ``committedAfter`` used for batch commits."""

    def __init__(self, records):
        self._records = list(records)
        self.commit_batch_size = None

    def committedAfter(self, size):
        self.commit_batch_size = size

    def __iter__(self):
        return iter(self._records)


# ==============================================================================
# Business object type
# ==============================================================================

class FakeBOType(object):
    """Emulates ``ApiBOType`` backed by an in-memory store of BOs.

    A single store is shared across all type instances for the same name via the
    owning :class:`FakeVMApi`, so find/create/iterate operations stay consistent.
    """

    def __init__(self, name, store, business_key_attr=None, object_link_fields=None, collection_fields=None):
        self.name = name
        self._store = store  # list shared per type name
        self.business_key_attr = business_key_attr
        self.object_link_fields = set(object_link_fields or [])
        self.collection_fields = set(collection_fields or [])

    def getName(self):
        return self.name

    def getBusinessKeyAttrName(self):
        return self.business_key_attr

    def createBO(self, tr=None, generate_key=False):
        bo = FakeBo(self.name, bo_type_obj=self)
        if generate_key and self.business_key_attr:
            FakeBo._moniker_counter += 1
            bo.getBOField(self.business_key_attr).setValue("KEY-%d" % FakeBo._moniker_counter)
        self._store.append(bo)
        return bo

    # --- querying -------------------------------------------------------------
    def _matches(self, condition):
        if not condition:
            return [bo for bo in self._store if not bo._removed]
        field, value = _parse_condition(condition)
        result = []
        for bo in self._store:
            if bo._removed:
                continue
            if field in bo and bo.getBOField(field).getValue() == value:
                result.append(bo)
        return result

    def find(self, tr, condition, trl_type=None):
        return FakeFindResult(self._matches(condition))

    def createFilterForNewObjects(self, tr, condition):
        # In these fakes there is no separate "new/uncommitted" pool, so the
        # filter is always empty and lookups fall through to find().
        return FakeFilter([])

    def createIterator(self, tr, condition=""):
        return FakeIterator(self._matches(condition))

    def __repr__(self):
        return "<FakeBOType %s>" % self.name


def _parse_condition(condition):
    """Parse a trivial ``field == 'value'`` condition used by the framework."""
    if "==" not in condition:
        raise ValueError("Unsupported condition for FakeBOType: %r" % condition)
    left, right = condition.split("==", 1)
    field = left.strip()
    value = right.strip().strip("'\"")
    return field, value


# ==============================================================================
# Transaction
# ==============================================================================

class FakeTransaction(object):
    """A no-op transaction that simply tracks commit calls."""

    def __init__(self):
        self.commit_count = 0

    def containsBO(self, bo):
        return True

    def get(self, bo):
        return bo

    def doCommitResume(self):
        self.commit_count += 1


# ==============================================================================
# VM (the Jython global)
# ==============================================================================

class FakeVMApi(object):
    """Stand-in for the global ``VM`` object used throughout the framework."""

    # Logging levels
    LOG_INFO = 10
    LOG_WARN = 20
    LOG_ERROR = 30
    LOG_DEBUG = 40
    LOG_EXCEPTION = 50
    LOG_FINE = 60
    LOG_FINER = 70
    LOG_FINEST = 80

    # TRL constants
    TRL_ALL = "TRL_ALL"
    TRL_CURRENT = "TRL_CURRENT"

    def __init__(self):
        self._stores = {}
        self._type_config = {}
        self.log_messages = []

    def reset(self):
        """Clear all in-memory stores while keeping registered type metadata.

        Useful for test isolation when the same ``VM`` instance must be reused
        (e.g. because processor classes cached their ``target_type`` against it
        at import time)."""
        for store in self._stores.values():
            del store[:]
        del self.log_messages[:]

    def configure_type(self, name, business_key_attr=None, object_link_fields=None, collection_fields=None):
        """Register metadata for a BO type so links/keys behave correctly."""
        if name in self._type_config:
            config = self._type_config[name]
            updates = (
                business_key_attr != config["business_key_attr"],
                sorted(object_link_fields or []) != sorted(config["object_link_fields"]),
                sorted(collection_fields or []) != sorted(config["collection_fields"]),
            )
            if any(updates):
                raise ConfigurationError("Type <%s> allread configured. Reconfiguration not allowed!" % name)
        self._type_config[name] = {
            "business_key_attr": business_key_attr,
            "object_link_fields": object_link_fields or [],
            "collection_fields": collection_fields or [],
        }

    def getBOType(self, name):
        store = self._stores.setdefault(name, [])
        cfg = self._type_config.get(name, {})
        return FakeBOType(
            name,
            store,
            business_key_attr=cfg.get("business_key_attr"),
            object_link_fields=cfg.get("object_link_fields"),
            collection_fields=cfg.get("collection_fields"),
        )

    def get_store(self, name):
        """Test helper: direct access to the in-memory store for a BO type."""
        return self._stores.setdefault(name, [])

    def findBObjectMoniker(self, moniker, tr):
        for store in self._stores.values():
            for bo in store:
                if bo.getMoniker() == moniker:
                    return bo
        return None

    def persistentLogMessage(self, domain, message, a, b, bo, level, flag):
        self.log_messages.append((domain, message, level))

    def create_bo(self, bo_type, **kwargs):
        bo = self.getBOType(bo_type).createBO()
        for k, v in kwargs.items():
            bo_field = bo.getBOField(k)
            if bo_field.isCollectionLink():
                collection = bo_field.getCollection()
                for item in v:
                    collection.add(item)            
            elif bo_field.isObjectLink():
                bo_field.setObject(v)

            else:
                bo_field.setValue(v)
        return bo


