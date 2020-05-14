from sqlalchemy import (
                    BigInteger,
                    Column,
                    Date,
                    DateTime,
                    ForeignKey,
                    Integer,
                    String
                )

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


# ORM Tutorial
# https://docs.sqlalchemy.org/en/13/orm/tutorial.html

Base = declarative_base()


class Term(Base):
    __tablename__ = 'term'

    canvas_id = Column(Integer, primary_key=True)
    name = Column(String(100))
    sis_id = Column(Integer)
    start_at = Column(DateTime(timezone=True))
    end_at = Column(DateTime(timezone=True))

    course = relationship('Course', back_populates='term')

    def __repr__(self):
        return (
            f'<Term(canvas_id={self.canvas_id}, name={self.name}, ' +
            f'sis_id={self.sis_id}, start_at={self.start_at}, end_at={self.end_at})>'
        )


class Course(Base):
    __tablename__ = 'course'

    canvas_id = Column(Integer, primary_key=True)
    sis_id = Column(BigInteger)
    name = Column(String(100))
    account_id = Column(Integer)
    term_id = Column(Integer, ForeignKey('term.canvas_id'))
    created_at = Column(DateTime(timezone=True))
    published_at = Column(DateTime(timezone=True))
    workflow_state = Column(String(25))

    canvas_course_usage = relationship('CanvasCourseUsage', back_populates='course')
    enrollment = relationship('Enrollment', back_populates='course')
    term = relationship('Term', back_populates='course')

    def __repr__(self):
        return (
            f'<Course(canvas_id={self.canvas_id}, sis_id={self.sis_id}, ' +
            f'name={self.name}, account_id={self.account_id}, term_id={self.term_id}), ' +
            f'created_at={self.created_at}, published_at={self.published_at}, ' +
            f'workflow_state={self.workflow_state})>'
        )


class CourseSection(Base):
    __tablename__ = 'course_section'

    canvas_id = Column(Integer, primary_key=True)
    sis_id = Column(BigInteger)
    name = Column(String(200))
    
    enrollment = relationship('Enrollment', back_populates='course_section')

    def __repr__(self):
        return (
            f'<CourseSection(canvas_id={self.canvas_id}, sis_id={self.sis_id}, name={self.name})>'
        )


class Enrollment(Base):
    __tablename__ = 'enrollment'

    canvas_id = Column(Integer, primary_key=True)
    course_id = Column(Integer, ForeignKey('course.canvas_id'))
    course_section_id = Column(Integer, ForeignKey('course_section.canvas_id'))
    user_id = Column(BigInteger)
    workflow_state = Column(String(25))
    role_type = Column(String(25))

    course = relationship('Course', back_populates='enrollment')
    course_section = relationship('CourseSection', back_populates='enrollment')

    def __repr__(self):
        return (
            f'<Enrollment(canvas_id={self.canvas_id}, course_id={self.course_id}, '
            f'course_section_id={self.course_section_id}, user_id={self.user_id}, '
            f'workflow_state={self.workflow_state}, role_type={self.role_type})>'
        )


class CanvasCourseUsage(Base):
    __tablename__ = 'canvas_course_usage'

    id = Column(BigInteger, primary_key=True)
    course_id = Column(Integer, ForeignKey('course.canvas_id'))
    views = Column(Integer)
    participations = Column(Integer)
    date = Column(Date)

    course = relationship('Course', back_populates='canvas_course_usage')

    def __repr__(self):
        return (
            f'<CanvasCourseUsage(id={self.id}, course_id={self.course_id}, '
            f'views={self.views}, participations={self.participations}, '
            f'date={self.date})>'
        )


class MivideoMediaStartedHourly(Base):
    __tablename__ = 'mivideo_media_started_hourly'

    id = Column(BigInteger, primary_key=True)
    event_hour_utc = Column(String(20))
    course_id = Column(Integer)
    event_time_utc_latest = Column(DateTime(timezone=True))
    event_count = Column(BigInteger)

    def __repr__(self):
        return (
            f'<MivideoMediaStartedHourly(id={self.id}, event_hour_utc={self.event_hour_utc}, '
            f'course_id={self.course_id}, event_time_utc_latest={self.event_time_utc_latest}, '
            f'event_count={self.event_count})>'
        )


class JobRun(Base):
    __tablename__ = 'job_run'

    id = Column(Integer, primary_key=True)
    job_name = Column(String(50))
    started_at = Column(DateTime(timezone=True))
    finished_at = Column(DateTime(timezone=True))

    data_source_status = relationship('DataSourceStatus', back_populates='job_run')

    def __repr__(self):
        return (
            f'<JobRun(id={self.id}, job_name={self.job_name}, '
            f'started_at={self.started_at}, finished_at={self.finished_at})>'
        )


class DataSourceStatus(Base):
    __tablename__ = 'data_source_status'

    id = Column(Integer, primary_key=True)
    data_source_name = Column(String(50))
    data_updated_at = Column(DateTime(timezone=True))
    job_run_id = Column(Integer, ForeignKey('job_run.id'))

    job_run = relationship('JobRun', back_populates='data_source_status')

    def __repr__(self):
        return (
            f'<DataSourceStatus(id={self.id}, data_source_name={self.data_source_name}, '
            f'data_updated_at={self.data_updated_at}, job_run_id={self.job_run_id})>'
        )
