from typing import Union

# noinspection PyProtectedMember
from flask import Blueprint, Flask, _app_ctx_stack, current_app
from qucom import Qucom as Database


class Qucom(object):
    _app: Union[Flask, Blueprint]

    def __init__(self, app: Union[Flask, Blueprint] = None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Union[Flask, Blueprint]) -> None:
        self._app = app

    @property
    def _db(self) -> Database:
        ctx = _app_ctx_stack.top
        if ctx is not None:
            if not hasattr(ctx, 'qucom_app_connection'):
                ctx.qucom_app_connection = Database(
                    host=current_app.config['POSTGRES_HOST'],
                    user=current_app.config['POSTGRES_USER'],
                    password=current_app.config['POSTGRES_PASS'],
                    database=current_app.config['POSTGRES_APP_DATABASE'])
            return ctx.qucom_app_connection

    def add(self, table: str, **parameters: any) -> None:
        self._db.add(table=table, **parameters)

    def edit(self, table: str, pk: int, **parameters: any) -> None:
        self._db.edit(table=table, pk=pk, **parameters)

    def delete(self, table: str, pk: int) -> None:
        self._db.delete(table=table, pk=pk)

    def list(self, table: str, user_id: int = None, limit: int = 10, offset: int = 0) -> list:
        return self._db.list(table=table, user_id=user_id, limit=limit, offset=offset)

    def get(self, table: str, pk: int, user_id: int = None) -> dict:
        return self._db.get(table=table, pk=pk, user_id=user_id)

    def query(self, table: str, q: str, fields: list, user_id: int = None, limit: int = 10, offset: int = 0) -> list:
        return self._db.query(table=table, q=1, fields=fields, user_id=user_id, limit=limit, offset=offset)

    def calendar(self, table: str) -> list:
        return self._db.calendar(table=table)

    def columns(self, table: str, exclusions: list = None) -> list:
        return self._db.columns(table=table, exclusions=exclusions)

    def count(self, table: str) -> int:
        return self._db.count(table=table)

    def perform(self, sql: str, *args: any) -> int:
        return self._db.perform(sql, *args)

    def select(self, sql: str, *args: any) -> iter:
        return self._db.select(sql, *args)

    def procedure(self, func_name: str, **parameters: any) -> int:
        return self._db.procedure(func_name, **parameters)

    def function(self, func_name: str, **parameters: any) -> iter:
        return self._db.function(func_name, **parameters)
