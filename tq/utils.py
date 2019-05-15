import pytz

from calendar import timegm
from datetime import datetime as dt, datetime, time, timedelta


job_latency_threshold = 60 * 5

hour = 60 * 60
day_in_secs = hour * 24


weekdays = {
    'mon': 0,
    'tue': 1,
    'wed': 2,
    'thu': 3,
    'fri': 4,
    'sat': 5,
    'sun': 6
}

dt_format = '%m/%d/%Y'


def to_text(value):
    if value is not None:
        if isinstance(value, str):
            return value
        elif isinstance(value, bytes):
            return value.decode()
        else:
            return value
    return None


def decode(data):
    return None if data is None else data.decode()


def decode_hash(data):
    return dict((to_text(key), data[key]) for key in data)


def time_from_ts(timestamp):
    time = dt.fromtimestamp(timestamp)
    time = time.strftime('%H:%M:%S')
    return time


def ts(_dt):
    return timegm(_dt.timetuple())


def should_run_fallback(ts_now, job):
    ''' return True, if the job is being run (5) mins later than it should've'''
    return (ts_now - job.scheduled_at) > job_latency_threshold


def get_hour_offset(job):
    hour_count = job.every_hour

    latency = job.ran_at - job.scheduled_at

    if latency > job_latency_threshold:
        diff = int(latency / hour)
        hour_count += diff

    return hour_count * hour


def to_weekdays(days):
    return [weekdays[day] for day in days]


def to_utc(from_tz, dt):
    return from_tz.localize(dt).astimezone(pytz.utc)


def dt_to_utc(d, t, fromtz):
    return ts(to_utc(fromtz, datetime.combine(d, t)))


def make_time(t):
    ''' put time objects in a array from list of time strings'''
    t = [int(_time) for _time in t.split(':')]
    return time(hour=t[0], minute=t[1])


def sorted_slots(timeslots):
    return sorted(list(map(make_time, timeslots)))


def get_next_day(last_date, days):
    '''last_weekday, because posts need to be scheduled, even if the days have been missed
       so they can run run the fallback and set the posts, to failed.,
       returns seconds `to` next day.
    '''
    last_weekday = last_date.weekday()
    days_count = len(days)

    if last_weekday not in days:
        next_day_index = 0
    else:
        next_day_index = days.index(last_weekday) + 1

    #  go back to the first day if, there are no more days.
    next_day_index = 0 if next_day_index == days_count else next_day_index

    next_day = days[next_day_index]

    if next_day > last_weekday:
        diff = next_day - last_weekday
    else:
        # if the day is on next week, (7 - d) == num of days to complete the week
        diff = (7 - last_weekday) + next_day

    return last_date + timedelta(days=diff)


def next_timeslot(timeslots, tz):
    '''Check if job can be run today, depending on timeslots
       (if its too late to run a job),
       timeslots => ['12:32', '16:34']
       tz => pytz timezone object.
    '''
    timeslot_hours = sorted_slots(timeslots)

    today = datetime.now(tz=tz)
    time_now = today.time()

    for timeslot in timeslot_hours:
        if time_now < timeslot:
            dt = datetime.combine(today.date(), timeslot)
            localized = tz.localize(dt, is_dst=None).astimezone(pytz.utc)
            return ts(localized)

    return None


def _get_next_ts(days, timeslots, tz):
    '''tz=> pytz object'''
    today = datetime.now(tz=tz)
    next_date = get_next_day(today.date(), days)
    first_slot = make_time(timeslots[0])
    upcoming_dt = datetime.combine(next_date, first_slot)
    return dt_to_utc(next_date, first_slot, tz)


def get_scheduled_at(exec_info):
    """TO-REMEMBER- incase of date, it picks the first slot,
       need a check to ensure, user doesnt schedule for time, less than.
    """
    tz = pytz.timezone(exec_info.get('timezone'))
    date = exec_info.get('date')

    timeslots = exec_info.get('timeslots')
    today = datetime.now(tz=tz)

    if date is not None:
        #  if job is scheduled on a date, get the date and the first slot,
        #  and convert to ts
        scheduled_date = datetime.strptime(date, dt_format).date()
        slot = sorted_slots(timeslots)[0]
        return dt_to_utc(scheduled_date, slot, tz)


    today_weekday = today.weekday()
    days = exec_info.get('days')

    if today_weekday in days:
        #  if it can run today, return the timeslot from today.
        today_slot_ts = next_timeslot(timeslots, tz)
        if today_slot_ts:
            return today_slot_ts

    return _get_next_ts(days, timeslots, tz)


def get_next_ts(job):
    scheduled_at = job.scheduled_at
    if job.every_hour:
        #   if job runs on every hour basis,
        #   get_hour_offset returns number of hours,
        #   (to offset, in case worker was down)
        offset = get_hour_offset(job)
        return scheduled_at + offset

    tz = pytz.timezone(job.timezone)
    timeslots = job.timeslots
    last_weekday = datetime.fromtimestamp(scheduled_at)

    next_ts = next_timeslot(timeslots, tz)

    if not next_ts and not job.has_date:
        next_ts = _get_next_ts(job.scheduled_days, timeslots, tz)

    return next_ts
