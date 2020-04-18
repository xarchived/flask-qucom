from typing import Union

# noinspection PyProtectedMember
from flask import Blueprint, Flask, _app_ctx_stack, current_app
from qedgal import Qedgal as Database


class Qedgal(object):
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
            if not hasattr(ctx, 'qedgal_app_connection'):
                ctx.qedgal_app_connection = Database(
                    host=current_app.config['POSTGRES_HOST'],
                    user=current_app.config['POSTGRES_USER'],
                    password=current_app.config['POSTGRES_PASS'],
                    database=current_app.config['POSTGRES_APP_DATABASE'])
            return ctx.qedgal_app_connection

    def add(self, table: str, **parameters: any) -> None:
        self._db.add(table, **parameters)

    def edit(self, table: str, pk: int, **parameters: any) -> None:
        self._db.edit(table, pk, **parameters)

    def delete(self, table: str, pk: int) -> None:
        self._db.delete(table, pk)

    def list(self, table: str, user_id: int = None) -> list:
        return self._db.list(table, user_id)

    def get(self, table: str, pk: int, user_id: int = None) -> dict:
        return self._db.get(table, pk, user_id)

    def query(self, table: str, q: str, fields: list, user_id: int = None) -> list:
        return self._db.query(table, q, fields, user_id)

    def perform(self, sql: str, *args: any) -> int:
        return self._db.perform(sql, *args)

    def select(self, sql: str, *args: any) -> iter:
        return self._db.select(sql, *args)

    def procedure(self, func_name: str, **parameters: any) -> int:
        return self._db.procedure(func_name, **parameters)

    def function(self, func_name: str, **parameters: any) -> iter:
        return self._db.function(func_name, **parameters)
