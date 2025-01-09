from nautobot.apps import jobs
import codecs
import contextlib
from io import BytesIO

from django.contrib.contenttypes.models import ContentType
from django.core.exceptions import PermissionDenied
from django.db import transaction
from rest_framework import exceptions as drf_exceptions, serializers

from nautobot.core.api.exceptions import SerializerNotFound
from nautobot.core.api.parsers import NautobotCSVParser
from nautobot.core.api.utils import get_serializer_for_model
from nautobot.core.exceptions import AbortTransaction
from nautobot.extras.jobs import BooleanVar, ChoiceVar, FileVar, Job, ObjectVar, RunJobTaskFailed, StringVar, TextVar
from nautobot.extras.models import Status
from nautobot.dcim.models.locations import Location, LocationType


us_states = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia"
}


name = "Data Import"  # optional, but recommended to define a grouping name


class CustomLocationSerializer(
    serializers.Serializer,
):
    name = serializers.CharField()
    city = serializers.CharField()
    state = serializers.CharField()


class CustomCSVImport(Job):
    """Custom Job to import CSV data to create a set of objects."""

    content_type = ObjectVar(
        model=ContentType,
        description="Type of objects to import",
        query_params={"can_add": True, "has_serializer": True},
    )
    csv_data = TextVar(label="CSV Data", required=False)
    csv_file = FileVar(label="CSV File", required=False)
    roll_back_if_error = BooleanVar(
        label="Rollback Changes on Failure",
        required=False,
        default=True,
        description="If an error is encountered when processing any row of data, rollback the entire import such that no data is imported.",
    )

    template_name = "system_jobs/import_objects.html"

    class Meta:
        name = "Custom CSV Import"
        description = "Import objects from CSV-formatted data."
        has_sensitive_variables = False
        # Importing large files may take substantial processing time
        soft_time_limit = 1800
        time_limit = 2000

    def _perform_atomic_operation(self, data, serializer_class, queryset):
        new_objs = []
        with contextlib.suppress(AbortTransaction):
            with transaction.atomic():
                new_objs, validation_failed = self._perform_operation(data, serializer_class, queryset)
                if validation_failed:
                    raise AbortTransaction
                return new_objs, validation_failed
        # If validation failed return an empty list, since all objs created where rolled back
        self.logger.warning("Rolling back all %s records.", len(new_objs))
        return [], validation_failed

    def _perform_operation(self, data, serializer_class, queryset):
        new_objs = []
        validation_failed = False
        for row, entry in enumerate(data, start=1):
            serializer = serializer_class(data=entry, context={"request": None})
            if serializer.is_valid():
                try:
                    with transaction.atomic():
                        new_obj = serializer.save()
                        if not queryset.filter(pk=new_obj.pk).exists():
                            raise AbortTransaction()
                    self.logger.info('Row %d: Created record "%s"', row, new_obj, extra={"object": new_obj})
                    new_objs.append(new_obj)
                except AbortTransaction:
                    self.logger.error(
                        'Row %d: User "%s" does not have permission to create an object with these attributes',
                        row,
                        self.user,
                    )
                    validation_failed = True
            else:
                validation_failed = True
                for field, err in serializer.errors.items():
                    self.logger.error("Row %d: `%s`: `%s`", row, field, err[0])
        return new_objs, validation_failed

    def run(self, *, content_type, csv_data=None, csv_file=None, roll_back_if_error=False):
        if not self.user.has_perm(f"{content_type.app_label}.add_{content_type.model}"):
            self.logger.error('User "%s" does not have permission to create %s objects', self.user, content_type.model)
            raise PermissionDenied("User does not have create permissions on the requested content-type")

        model = content_type.model_class()
        if model is None:
            self.logger.error(
                'Could not find the "%s.%s" data model. Perhaps an app is uninstalled?',
                content_type.app_label,
                content_type.model,
            )
            raise RunJobTaskFailed("Model not found")
        try:
            serializer_class = get_serializer_for_model(model)
        except SerializerNotFound:
            self.logger.error(
                'Could not find the "%s.%s" data serializer. Unable to process CSV for this model.',
                content_type.app_label,
                content_type.model,
            )
            raise
        queryset = model.objects.restrict(self.user, "add")

        if not csv_data and not csv_file:
            raise RunJobTaskFailed("Either csv_data or csv_file must be provided")
        if csv_file:
            # data_encoding is utf-8 and file_encoding is utf-8-sig
            # Bytes read from the original file are decoded according to file_encoding, and the result is encoded using data_encoding.
            csv_bytes = codecs.EncodedFile(csv_file, "utf-8", "utf-8-sig")
        else:
            csv_bytes = BytesIO(csv_data.encode("utf-8"))

        new_objs = []
        try:
            data = NautobotCSVParser().parse(
                stream=csv_bytes,
                # parser_context={"request": None, "serializer_class": serializer_class},
                parser_context={"request": None, "serializer_class": CustomLocationSerializer},
            )
            self.logger.info("Processing %d rows of data", len(data))
            # self.logger.info("Processed data", data)


            type_state = LocationType.objects.get(name="State")
            type_city = LocationType.objects.get(name="City")
            type_branch = LocationType.objects.get(name="Branch")
            type_data_center = LocationType.objects.get(name="Data Center")
            active_status = Status.objects.get(name="Active")

            validation_failed = False
            for obj in data:
                state_name = obj["state"]
                if state_name in us_states:
                    state_name = us_states[state_name]
                if state_name not in us_states.values():
                    validation_failed = True
                    break
                state, created = Location.objects.get_or_create(name=obj["state"], type=type_state, status=active_status)
                city, created = Location.objects.get_or_create(name=obj["city"], type=type_city, status=active_status)

            # if roll_back_if_error:
            #     new_objs, validation_failed = self._perform_atomic_operation(data, serializer_class, queryset)
            # else:
            #     new_objs, validation_failed = self._perform_operation(data, serializer_class, queryset)
        except drf_exceptions.ParseError as exc:
            validation_failed = True
            self.logger.error("`%s`", exc)

        if new_objs:
            self.logger.info(
                "Created %d %s object(s) from %d row(s) of data", len(new_objs), content_type.model, len(data)
            )
        else:
            self.logger.warning("No %s objects were created", content_type.model)

        if validation_failed:
            if roll_back_if_error:
                raise RunJobTaskFailed("CSV import not successful, all imports were rolled back, see logs")
            raise RunJobTaskFailed("CSV import not fully successful, see logs")


jobs.register_jobs(CustomCSVImport)
