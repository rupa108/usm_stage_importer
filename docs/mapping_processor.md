# MappingProcessor Class Documentation

## Overview

The `MappingProcessor` class is the default implementation of the `AbstractProcessor` interface. It provides a declarative, metadata-driven approach to mapping fields from a source business object to a target business object. The class uses a metaclass-based system to automatically discover and manage field descriptors in a specific processing order.

## 1. Declarative Field Mapping
The `MappingProcessor` uses declarative field descriptors to define how source fields map to target fields. You define fields as class attributes, and the metaclass automatically collects and orders them.

```python
class ExampleProcessor(MappingProcessor):
    pk = PlainField(source_field="ID", match_key=True)
    name = PlainField(source_field="NAME")
    status = StaticField(value="ACTIVE")
    
    class Meta:
        target_bo_name = "ExampleBO"
```
> **Note:** The variable name assigned to the fields, such as `pk`, `name`, and `status`, determines the target attribute name where the value is written.

## 2. Class Meta
The purpose of class Meta is to provide a place to store configuration or metadata about the processor. This metadata is used to configure the processor's behavior.
Currently the following options are supported:
- **`include_inactive`**: A boolean flag indicating whether to include inactive records in the processing. Defaults to `False`
- **`collect_bos`**: A boolean flag indicating whether to collect business objects seen during processing. This is required to be `True` if you want to use the reconsiliation functionality. You can set this to `False` in order to reduce memory usage if reconsiliation is not required. Defaults to `True`
- **`target_bo_name`**: The name of the target business object. This is used to identify the

## 3. Processing Order Control
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

## 4. Value transformation
You can apply transformations to values using the processor_func parameter in a field. This function is expected to have the following signature:
```python
def my_processing_func(context, value):
    ...
    return transformed_value
```
`contex` is an instance of `ProcessingContext` and `value` is what is extracted form the source. The return value will be written to the target field.

The special value `undefined` can be returned to indicate that the field should not be written to the target.
This is useful when you want to skip writing a field under certain conditions e.g. only write on create.

The special exception `ValidationError` can be raised to indicate that the value is invalid and processing of the current record should be aborted.

## 5. `ProcessingContext`

During field mapping, the `ProcessingContext` object provides access to runtime information and the current runtime state.

**Example:**

```python
def processor_func(context, source_value):
    source_bo = context.source # The stage record where information is extracted from
    target_bo = context.target # The target record where information is mapped to
    transaction = context.transaction # The transaction object
    
    target_is_created = context.is_create # True if the target record is being created
    target_is_updated = context.is_update # True if the target record is being updated
    
    # Since during the mapping phase the extracted values are not written directly to the target object,
    # but are stored internally. You can access allready processed field values with the `get_pending_value` method.
    pending_value = context.get_pending_value("other_field")
    
    # If you create or update objects here that is not handled by the mapping processor otherwise, you need to add them to the
    # context in order to be accounted for by the `Reconciler` in the reconciliation phase.
    context.add_touched_object(related_bo)
    ...
    # Do some processing
    return processed_value
```
## 6. Field Types
There are various field types that can be used to map fields from source to target.
### `PlainField`
Maps a simple field value from source to target with optional processing.

**Arguments:** 
* **`source_field`** (*str*): The name of the source field.
* **`processor_func`** (*callable*): A function for value transformation.
* **`match_key`** (*bool*): If True, this field is used to identify the target object. Processing in `processor_func()` is taken into account.

**Example:**
```python
# Reads from source field "NAME", transforms it, and writes to target field `name`.
# Uses the transformed value for matching target object. If no match is found, a new object is created.
name = PlainField(
    source_field="NAME",
    processor_func=lambda ctx, val: val.upper(),
    match_key=True
)
```

### `StaticField`
Sets a predefined static value on the target field.

**Arguments:**
* **`value`** (*any*): The value to be set to the target.
* **`processor_func`** (*callable*): A function for value transformation. (Why would one need this in a `StaticField`? Answer: e.g. for only_on_create logic.)

**Example:**
```python
status = StaticField(
    value="ACTIVE",
    processor_func=lambda c, v: v if c.is_create else undefined
    )
```

### `RelationField`
Maps to a related business object, with find-or-create capability.

**Arguments:**
* **`source_field`** (*str*): The source field to read from.
* **`target_bo_name`** (*str*): The name of the target BOType.
* **`target_lookup_field`** (*str*): The field to use for lookup in the target BOType.
* **`on_not_found_create`** (*dict*): A python dict of values to create the target BO with, if it is not found. This is a simplyfied version for creating related objects. For fully featured approach use the `ChainedRelationField`.
```python
create_dict = {
    "typeName": FromSouce("dbType"), 
    "description": Static("Created by mapping")
    "typeClass": FromAnywhere(lambda c, _: get_type_class(c))
}
# create
type = RelationField(
    source_field="dbType",
    target_bo_name="Type",
    target_lookup_field="typeName",
    on_not_found_create=create_dict,
)
```

### `ChainedRelationField`
Uses an internal factory to resolve complex related objects.

```python
category = ChainedRelationField(
    source_field="CATEGORY_ID",
    processor_or_factory=CategoryMappingProcessor
)
```

## 7. `MappingProcessor` Class Main Methods

### `process()`
The primary method that executes all field mappings in order. It is nor recommended to overwrite this method in subclasses. Use `pre_process()` and `post_process()` instead. 
It performs a two-phase approach:
1. **Map Phase**: Calls `map_value()` on each field descriptor in order, storing results in a temporary queue
2. **Set Phase**: Calls `set_target_value()` on each queued field in order, applying values to the target BO

**Behavior:**
- Catches exceptions and logs errors during mapping and continues processing
- Re-raises `ValidationError` exceptions immediately and theby skips processing of current record

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
    if earlier_value == "ACT":
        return "OK"
    else:
        return "UN"
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

# Complete Example

```python
from stage_importer_framework import (
    MappingProcessor,
    PlainField,
    StaticField,
    RelationField
)

# Used to create a new coutry record if the one we are looking for doesn't exist.
create_country_dict = {
    "code": FromSource("countryCode"),
    "name": FromSource("countryName"),
}

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
        source_field="countryCode",
        target_bo_name="CountryBO",
        target_lookup_field="code",
        on_not_found_create=create_country_dict,
    )
    
    # Explicit processing order (optional)
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
```
