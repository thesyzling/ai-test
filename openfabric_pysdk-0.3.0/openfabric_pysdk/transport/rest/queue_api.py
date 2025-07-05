import uuid
from datetime import datetime
from marshmallow import fields, ValidationError
from flask import request, jsonify

from openfabric_pysdk.context import Ray, RaySchema
from openfabric_pysdk.engine import engine
from openfabric_pysdk.fields import fields
from openfabric_pysdk.flask.rest import *
from openfabric_pysdk.loader import *
from openfabric_pysdk.logger import logger
from openfabric_pysdk.service import PersistenceService
from openfabric_pysdk.transport import ResourceDescriptor

from .rest_api import WebApi


#######################################################
#  Execution Queue API
#######################################################
class PaginatedRaySchema(Schema):
    rays = fields.Nested(RaySchema, many=True, default=[])
    next_cursor = fields.Integer(allow_none=True, default=None)


class QueueGetApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    @doc(description="Get the response for the indicated request", tags=["Queue"])
    @use_kwargs({'qid': fields.String(required=True)}, location='query')
    def get(self, qid: str):
        self.check_user()
        schema = getSchemaInst('out')

        return PersistenceService.get_asset(qid, 'out', schema.load)


class QueueDeleteApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    @doc(description="Remove the indicated requests and the associated results", tags=["Queue"])
    @use_kwargs({'qid': fields.String(required=True)}, location='query')
    @marshal_with(RaySchema)
    def delete(self, qid: str, *args):
        self.check_user()

        app = self._descriptor.app
        return engine.delete(qid, app)


class QueueListApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    @doc(description="Get list of existing requests and their status optionaly filtered by date", tags=["Queue"])
    @use_kwargs({
        'start_date': fields.DateTime(missing=None),
        'end_date': fields.DateTime(missing=datetime.now),
        'limit': fields.Integer(missing=0),
        'cursor': fields.String(missing=None)
    }, location='query')
    @marshal_with(PaginatedRaySchema)
    def get(self, start_date: datetime, end_date: datetime, limit: int, cursor: str, *args):
        uid = self.check_user()
        headers = request.headers
        requeste_uid = headers.get("uid", uid)

        if requeste_uid is None:
            return {'rays': [], 'next_cursor': None}

        cursor_index = 0
        if cursor is not None:
            try:
                cursor_index = int(cursor)
            except (ValueError, TypeError):
                logger.warning(f"Invalid cursor value: {cursor}. Defaulting cursor_index to 0.")
                cursor_index = 0

        def criteria(ray: Ray):
            if ray is None:
                return False
            if requeste_uid != None and ray.uid != requeste_uid:
                return False
            if start_date is not None and ray.created_at < start_date:
                return False
            if end_date is not None and ray.created_at > end_date:
                return False
            return True

        available_rays = engine.pending_rays(criteria)

        if start_date is not None:
            available_rays.sort(key=lambda r: r.created_at)
        else:
            available_rays.sort(key=lambda r: r.created_at, reverse=True)

        total_count = len(available_rays)
        cursor_index = max(0, min(cursor_index, total_count))
        remaining_rays = available_rays[cursor_index:]

        if limit != 0 and limit < len(remaining_rays):
            result_rays = remaining_rays[:limit]
        else:
            result_rays = remaining_rays

        next_cursor = None
        if 0 < limit == len(result_rays) and cursor_index + limit < total_count:
            next_cursor = cursor_index + limit

        if start_date is None:
            result_rays = list(reversed(result_rays))

        return {
            'rays': result_rays,
            'next_cursor': next_cursor
        }


class QueuePostApi(WebApi):
    # ------------------------------------------------------------------------
    def __init__(self, descriptor: ResourceDescriptor = None):
        super().__init__(descriptor)

    @doc(description="Queue a new request", tags=["Queue"])
    @marshal_with(RaySchema)
    def post(self):
        uid = self.check_user()
        schema = getSchemaInst('in')

        data = request.get_json()
        if not data:
            return jsonify({"error": "No payload received"}), 400

        validated_data = None

        try:
            validated_data = schema.load(data)
        except ValidationError as err:
            return jsonify({"error": err.messages}), 400

        sid = uuid.uuid4().hex
        rid = uuid.uuid4().hex
        app = self._descriptor.app
        qid = engine.prepare(app, validated_data, sid=sid, uid=uid, rid=rid)
        ray = engine.ray(qid)
        return ray
