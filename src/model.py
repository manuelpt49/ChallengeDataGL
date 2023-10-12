import datetime as _dt
import sqlalchemy as _sql
import sqlalchemy.orm as _orm

import database as _database

class Department(_database.base):
    __tablename__ = "departments"
    id = _sql.Column(_sql.Integer, primary_key=True, index=True)
    department = _sql.Column(_sql.String)

    departments = _orm.relationship("employees", back_populates="departments")


class Job(_database.base):
    __tablename__ = "jobs"
    id = _sql.Column(_sql.Integer, primary_key=True, index=True)
    job = _sql.Column(_sql.String)

    jobs = _orm.relationship("employees", back_populates="jobs")


class Employee(_database.base):
    __tablename__ = "employees"
    id = _sql.Column(_sql.Integer, primary_key=True, index=True)
    name = _sql.Column(_sql.String)
    datetime = _sql.Column(_sql.DateTime, default=_dt.datetime.utcnow)
    department_id = _sql.Column(_sql.Integer, _sql.ForeignKey("departments.id"))
    job_id = _sql.Column(_sql.Integer, _sql.ForeignKey("jobs.id"))

    jobs = _orm.relationship("jobs", back_populates="employees")
    jobs = _orm.relationship("department", back_populates="employees")

