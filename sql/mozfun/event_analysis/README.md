# event_analysis

These functions are specific for use with the `events_daily` and `event_types` tables.
By themselves, these two tables are nearly impossible to use since the event history
is compressed; however, these stored procedures should make the data accessible.

The `events_daily` table is created as a result of two steps:
1. Map each event to a single UTF8 char which will represent it
2. Group each client-day and store a string that records, using the
    compressed format, that clients' event history for that day.
    The characters are ordered by the timestamp which they appeared
    that day.

The best way to access this data is to create a view to do the heavy lifting.
For example, to see which clients completed a certain action, you can create a view using these functions that
knows what that action's representation is (using the compressed mapping from 1.) and
create a regex string that checks for the presence of that event. The view makes this
transparent, and allows users to simply query a boolean field representing the presence
of that event on that day.
