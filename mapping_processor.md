# MappingProcessor Class Documentation

## Overview

The `MappingProcessor` class is the default implementation of the `AbstractProcessor` interface. It provides a declarative, metadata-driven approach to mapping fields from a source business object to a target business object. The class uses a metaclass-based system to automatically discover and manage field descriptors in a specific processing order.

## Key Features

### 1. **Declarative Field Mapping**
The `MappingProcessor` uses declarative field descriptors to define how source fields map to target fields. You define fields as class attributes, and the metaclass automatically collects and orders them.

```python
class ExampleProcessor(MappingProcessor):
    pk = PlainField(source_field="ID", match_key=True)
    name = PlainField(source_field="NAME")
    status = StaticField(value="ACTIVE")
    
    class Meta:
        target_bo_name = "ExampleBO"
```

### 2. **Processing Order Control**
The `__processing_order__` attribute allows you to explicitly control the order in which fields are processed. This is essential when later fields depend on values of earlier fields.

```python
class OrderedProcessor(MappingProcessor):
    pk = PlainField(source_field="ID", match_key=True)
    status = StaticField(value="ACTIVE")
    name = PlainField(source_field="NAME")
    
    __processing_order__ = ("pk", "status", "name")
    
    class Meta:
        target_bo_name = "TargetBO"
```

**Without `__processing_order__`**, fields are processed in the order they were declared in the class (based on creation counter).

### 3. **Create vs. Update Context**
The processor tracks whether it's performing a create or update operation through the `is_create` and `is_update` flags.

```python
processor = MappingProcessor(tr, source_bo, target_bo, is_create=True)
# Inside a field's processor_func:
if context.is_create:
    # Logic for new objects
else:
    # Logic for updates
```

## Constructor

```python
def __init__(self, tr, source_bo, target_bo, is_create=False)
```

**Parameters:**
- `tr` (ApiTransaction): The transaction context for database operations
- `source_bo` (ApiBObject): The source business object containing data to map
- `target_bo` (ApiBObject): The target business object to be populated
- `is_create` (bool): Whether this is a create operation (default: False)

**Side Effects:**
- Automatically adds the `target_bo` to the internal touched objects set
- Sets `is_create` and `is_update` flags

## Main Methods

### `process()`
The primary method that executes all field mappings in order. It performs a two-phase approach:
1. **Map Phase**: Calls `map_value()` on each field descriptor in order, storing results in a temporary queue
2. **Set Phase**: Calls `set_target_value()` on each queued field in order, applying values to the target BO

**Behavior:**
- Logs a warning if `__processing_order__` is not defined (order is not guaranteed)
- Catches exceptions during mapping and continues processing
- Re-raises `ValidationError` exceptions immediately
- Logs all errors at appropriate levels

**Example:**
```python
processor = MyProcessor(tr, source_bo, target_bo, is_create=True)
processor.process()  # Executes all field mappings
```

### `get_field(field_name)` [Class Method]
Retrieves a field descriptor by its target field name.

**Returns:**
- The `AbstractField` instance if found, `None` otherwise

**Example:**
```python
field = MyProcessor.get_field("pk")
```

### `get_pending_value(field_name)`
Retrieves a value that has already been processed and stored on the internal queue. This is useful when a field mapping depends on the processed value of an earlier field.

**Parameters:**
- `field_name` (str): The name of the field to retrieve

**Returns:**
- The processed value, or `undefined` if not yet processed

**Example:**
```python
def processor_func(context, value):
    # Get value from earlier field
    earlier_value = context.get_pending_value("status")
    if earlier_value == "ACTIVE":
        return value.upper()
    return value
```

### `generate_key()` [Class Method]
Determines whether business keys should be auto-generated for the target BO type.

**Returns:**
- `True` if the target type has a business key attribute, `False` otherwise

### `pre_process()` and `post_process()`
Lifecycle hooks for custom logic before and after field mapping.

**Default Implementation:**
- Both are empty (no-op) in the base class
- Override in subclasses for custom logic

**Example:**
```python
class CustomProcessor(MappingProcessor):
    # ... field definitions ...
    
    def pre_process(self):
        # Validate source data
        if not self.source.getBOField("ID").getValue():
            raise ValidationError("ID is required")
    
    def post_process(self):
        # Perform post-mapping calculations
        total = self.target.getBOField("amount").getValue()
        self.target.getBOField("tax").setValue(total * 0.1)
```

### `add_touched_object(bo)`
Adds a business object to the internal set of touched objects. This tracks which objects were modified or created.

### `get_active_keys()`
Returns a set of monikers for all touched objects. Used by factories for reconciliation tracking.

## Metadata System

### The Meta Class
Each processor defines a `Meta` inner class that specifies metadata:

```python
class ExampleProcessor(MappingProcessor):
    pk = PlainField(source_field="ID", match_key=True)
    
    class Meta:
        target_bo_name = "ExampleBO"
```

**Meta Attributes:**
- `target_bo_name` (str): The name of the target business object type (required for most operations)

### The ProcessorMetaclass
The `ProcessorMetaclass` metaclass automatically processes the `Meta` class and field descriptors:

1. **Collects Fields**: Discovers all `AbstractField` instances in the class
2. **Inherits Fields**: Merges fields from parent classes
3. **Orders Fields**: Applies `__processing_order__` if specified, otherwise uses declaration order
4. **Resolves Types**: Looks up the target BO type from the VM
5. **Stores Metadata**: Sets `meta.fields` and `meta.target_type`

**Example of inheritance:**
```python
class BaseProcessor(MappingProcessor):
    pk = PlainField(source_field="ID", match_key=True)
    
    class Meta:
        target_bo_name = "ExampleBO"

class ExtendedProcessor(BaseProcessor):
    name = PlainField(source_field="NAME")
    status = StaticField(value="ACTIVE")
    # Inherits pk from BaseProcessor
```

## Field Descriptor Types

### PlainField
Maps a simple field value from source to target with optional processing.

```python
name = PlainField(
    source_field="NAME",
    processor_func=lambda ctx, val: val.upper(),
    match_key=True  # Used to identify existing records
)
```

### StaticField
Sets a predefined static value on the target field.

```python
status = StaticField(value="ACTIVE")
```

### RelationField
Maps to a related business object, with find-or-create capability.

```python
type = RelationField(
    source_field="TYPE",
    target_bo_name="TypeBO",
    target_lookup_field="type_name"
)
```

### ChainedRelationField
Uses an internal factory to resolve complex related objects.

```python
category = ChainedRelationField(
    source_field="CATEGORY_ID",
    processor_or_factory=CategoryMappingProcessor
)
```

## Processing Context

During field mapping, a `ProcessingContext` object provides access to:

```python
def processor_func(context, source_value):
    source_bo = context.get_source()
    target_bo = context.get_target()
    transaction = context.get_transaction()
    
    is_creating = context.is_create
    is_updating = context.is_update
    
    pending_value = context.get_pending_value("other_field")
    context.add_touched_object(related_bo)
    
    return processed_value
```

## Complete Example

```python
from stage_importer_framework import (
    MappingProcessor,
    PlainField,
    StaticField,
    RelationField
)

class CompanyProcessor(MappingProcessor):
    # Match key for finding existing records
    company_id = PlainField(
        source_field="COMPANY_ID",
        match_key=True
    )
    
    # Simple field mapping with transformation
    name = PlainField(
        source_field="COMPANY_NAME",
        processor_func=lambda ctx, val: val.strip().upper()
    )
    
    # Static value
    status = StaticField(value="ACTIVE")
    
    # Related object mapping
    country = RelationField(
        source_field="COUNTRY_CODE",
        target_bo_name="CountryBO",
        target_lookup_field="code"
    )
    
    # Explicit processing order (important if dependencies exist)
    __processing_order__ = ("company_id", "name", "status", "country")
    
    class Meta:
        target_bo_name = "CompanyBO"
    
    def pre_process(self):
        """Validate source data before mapping"""
        if not self.source.getBOField("COMPANY_ID").getValue():
            raise ValidationError("COMPANY_ID is required")
    
    def post_process(self):
        """Custom logic after all fields are mapped"""
        # Set created date if this is a new record
        if self.is_create:
            import datetime
            self.target.getBOField("created_date").setValue(
                datetime.datetime.now()
            )

# Usage
processor = CompanyProcessor(transaction, source_bo, target_bo, is_create=True)
processor.process()
```

## Error Handling

### ValidationError
Raised during field mapping to indicate validation failure. The entire record processing stops.

```python
def processor_func(context, value):
    if not value:
        raise ValidationError("Value is required")
    return value
```

### Exception Handling
Non-validation exceptions are caught and logged at `LOG_WARN` level, but processing continues for other fields.

### Logging
The processor logs at various levels:
- `LOG_FINER`: Field mapping start, skipped fields
- `LOG_WARN`: Field mapping errors
- `LOG_EXCEPTION`: Full stack traces for errors

## Integration with MappingProcessorFactory

The `MappingProcessorFactory` orchestrates multiple processors:

```python
factory = MappingProcessorFactory(
    repository=StagingRepository(staging_bo_name="StagingBO"),
    default_processor_class=CompanyProcessor,
)

factory.process_all(tr, commit_batch_size=100)
```

The factory calls:
1. `processor.pre_process()`
2. `processor.process()`
3. `processor.post_process()`
4. `processor.get_active_keys()` (for reconciliation)

## Best Practices

1. **Always define `__processing_order__`** when field mappings have dependencies
2. **Use match_key** on the field that uniquely identifies records for updates
3. **Implement validation in `pre_process()`** to fail fast
4. **Use `processor_func` for transformations** rather than post-processing
5. **Leverage `get_pending_value()`** for inter-field dependencies
6. **Handle create vs. update differently** using `context.is_create`
7. **Keep processors focused** - separate concerns into multiple processors if needed

## Performance Considerations

- **Two-phase processing**: Map phase stores all results before setting values. This allows inter-field dependencies and error recovery.
- **Queue-based storage**: Uses internal `_queue` list to maintain processed values in order
- **Early validation**: Use `pre_process()` to fail before mapping begins
- **Batch commits**: Use `MappingProcessorFactory` with `commit_batch_size` for large datasets
