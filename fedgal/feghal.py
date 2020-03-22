from typing import Union

from flask import Blueprint
from flask import Flask
from flask import current_app
from patabase.postgres import Database


class Feghal(object):
    _db = None

    def __init__(self, app: Union[Flask, Blueprint] = None):
        self.app = app
        if app is not None:
            self.init_app(app)

    def init_app(self, app: Union[Flask, Blueprint]) -> None:
        self.app = app
        self._db = Database(
            host=current_app.config['POSTGRES_HOST'],
            user=current_app.config['POSTGRES_USER'],
            password=current_app.config['POSTGRES_PASS'],
            database=current_app.config['POSTGRES_APP_DATABASE']
        )

    def add(self, table: str, **parameters: any) -> None:
        placeholders = ['%s' for _ in parameters]

        sql = f'''
            insert into {table} ({', '.join(parameters)})
            values ({', '.join(placeholders)})
        '''

        self._db.perform(sql, *parameters.values())

    def edit(self, table: str, pk: int, **parameters: any) -> None:
        fields = [f'{key} = %s' for key in parameters if parameters[key]]
        values = [parameters[key] for key in parameters if parameters[key]]

        sql = f'''
            update {table}
            set {', '.join(fields)}
            where id = %s
        '''

        self._db.perform(sql, *values, pk)  # TODO: check if "pk" exists

    def delete(self, table: str, pk: int) -> None:
        sql = f'''
            delete
            from {table}
            where id = %s
        '''

        self._db.perform(sql, pk)  # TODO: check if "pk" exists

    def list(self, table: str, user_id: int = None) -> list:
        sql = f'''
            select * 
            from {table}_facade
        '''

        if user_id:
            sql += f' where {user_id} = any(user_ids)'

        return list(self._db.select(sql))

    def get(self, table: str, pk: int, user_id: int = None) -> dict:
        sql = f'''
            select * 
            from {table}_facade
            where id = %s
        '''

        if user_id:
            sql += f' and {user_id} = any(user_ids)'

        return next(self._db.select(sql, pk), dict())

    def query(self, table: str, q: str, fields: list, user_id: int = None) -> list:
        filters = [f"{key}::varchar like %s" for key in fields]
        values = [f'%{q}%' for _ in fields]

        sql = f'''
            select *
            from {table}_facade
            where {' or '.join(filters)}
        '''

        if user_id:
            sql += f' and {user_id} = any(user_ids)'

        return list(self._db.select(sql, *values))

    def perform(self, sql: str, *args: any) -> int:
        return self._db.perform(sql, *args)

    def select(self, sql: str, *args: any) -> iter:
        return self._db.select(sql, *args)

    def procedure(self, func_name: str, **parameters: any) -> int:
        return self._db.procedure(func_name, **parameters)

    def function(self, func_name: str, **parameters: any) -> iter:
        return self._db.function(func_name, **parameters)
