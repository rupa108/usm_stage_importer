from de.usu.s3.api import ApiBObject, S3ApiException
from stage import Static, get_bo, link_nm, RelationField, FromSource, undefined, Reconciler, RelationshipProcessor, link_nm, ValidationError
from helper import calc_date
from vm.tools.boa import query


fp = VM.getFunctionProvider()
Usagetype = VM.getBOType("Usagetype")
yesterday = calc_date(fp.getCurrentDate(), days=-1, convert=True)


def only_on_create(context, value):
    return value if context.is_create else undefined

def link_usagetype(processor, ut_name, ut_description):

    _ut_create = {
        "usageType" : ut_name,
        "usageTypeDesc": ut_description,
        "valid": True,
    }
    system = processor.target #type: ApiBObject["System"]
    cls = type(processor)
    tr = processor.transaction
    ut = cls._assert_cached_bo(tr, "_usage_type", Usagetype, _ut_create, "usageType == %s" % fp.getAsQueryLiteral(ut_name))
    link_nm(system, ut, "sysusages")


class ValidtoReconciler(Reconciler):
    validfrom = "validfrom"
    validto = "validto"

    def deactivate_record(self, tr, bo):
        cls = type(self)
        f_validto = bo.getBOField(cls.validto)
        f_validfrom = bo.getBOField(cls.validfrom)

        f_validto.setValue(yesterday)
        if yesterday.before(f_validfrom.getValue()):
            f_validfrom.setValue(yesterday)

class SystemReconciler(ValidtoReconciler):
    def deactivate_record(self, tr, bo):
        super(SystemReconciler, self).deactivate_record(tr, bo)
        bo.getBOField("status").setValue('INACT')

class LinkOutgoingSystemProcessor(RelationshipProcessor):
    """
    A generic processor that links System to System via the syssyss.sysusage relation
    """
    rel_attr_name = "syssyss"
    _usage_type = None
    ut_name = "Service-Usage"
    propagate_interface = False

    def __init__(self, *args, **kwargs):
        self.row_bo = kwargs.get("row_bo")
        super(LinkOutgoingSystemProcessor, self).__init__(*args, **kwargs)
        self.sysusage = None


    @property
    def usage_type(self):
        # type: () -> ApiBObject["Usagetype"]
        ut_name = type(self).ut_name
        create_attrs = {"usageType": ut_name, "valid": True}
        tr = self.transaction
        ut = self._assert_cached_bo(tr, "_usage_type", VM.getBOType("Usagetype"), create_attrs, query(usageType=ut_name))
        self.add_touched_object(ut)

        return ut

    def pre_process(self):
        # assert System-Usagetype "Service-Usage" is present.
        cls = type(self)

        target = self.target #type: ApiBObject["System"]
        self.sysusage = link_nm(target, self.usage_type, "sysusages")

        if cls.propagate_interface:
            if_attr = "xInterface"
            interface = self.row_bo.getBOField(if_attr).getValue()
            self.sysusage.getBOField(if_attr).setValue(interface)
            self.sysusage.getBOField("xForHistory").setValue(False)

        self.add_touched_object(self.sysusage)

    def process(self):
        cls = type(self)
        source = self.source # type: ApiBObject["System"]
        rel_attr_name = type(self).rel_attr_name
        syssys = link_nm(source, self.sysusage, rel_attr_name)

        if cls.propagate_interface:
            if_attr = "xInterface"
            interface = self.row_bo.getBOField(if_attr).getValue()
            syssys.getBOField(if_attr).setValue(interface)
            syssys.getBOField("xForHistory").setValue(False)

        self.add_touched_object(syssys)

class LinkIncomingSystemProcessor(LinkOutgoingSystemProcessor):
    """
    Variant of LinkOutgoingSystemProcessor with swapped source and target.
    """
    def __init__(self, *args, **kwargs):
        super(LinkIncomingSystemProcessor, self).__init__(*args, **kwargs)
        self.source, self.target = self.target, self.source

class LinkOutgoingComponentProcessor(RelationshipProcessor):
    """
    A generic processor that links Component to System via the component.compsysconnections
    """
    rel_attr_name = "compsysconnections"
    _usage_type = None
    ut_name = "Service-Usage"
    propagate_interface = False

    def __init__(self, *args, **kwargs):
        self.row_bo = kwargs.get("row_bo")
        super(LinkOutgoingComponentProcessor, self).__init__(*args, **kwargs)
        self.sysusage = None

    @property
    def usage_type(self):
        # type: () -> ApiBObject["Usagetype"]
        ut_name = type(self).ut_name
        create_attrs = {"usageType": ut_name, "valid": True}
        tr = self.transaction
        ut = self._assert_cached_bo(tr, "_usage_type", VM.getBOType("Usagetype"), create_attrs, query(usageType=ut_name))
        self.add_touched_object(ut)

        return ut

    def pre_process(self):
        cls = type(self)
        target = self.target #type: ApiBObject["System"]

    def process(self):
        cls = type(self)

        # component (source) gets linked to system (target)
        source = self.source # type: ApiBObject["Component"]
        target = self.target #type: ApiBObject["System"]
        rel_attr_name = type(self).rel_attr_name
        compSysConnection = link_nm(source, target, rel_attr_name)


        self.add_touched_object(compSysConnection)

def validate_valueset(bo_type, field_name, value):
    #type: (ApiBOType, str, str) -> str
    """
    Validates if the given value is part of the valueset defined for the given field
    in the given BOType.
    Args:
        bo_type (ApiBOType): The BOType to that holds the field with the valueset
        field_name (str): The name of the BOField that holds the valueset
        value (str): The value that to be validated.
    Returns:
        The same value that is passed in if it is a valid value for the valueset.
    Raises:
        ValidationError: If the value is not valid for the valueset.
    """
    if not value:
        return
    bo_field = bo_type.getBOTypeField(field_name)
    valueset = bo_field.getValueSet()
    try:
        vset_item = valueset.find(value)
    except S3ApiException as e:
        raise ValidationError('"%s" is an invalid value for %s.%s' % (value, bo_type.getName(), field_name))
    else:
        return value