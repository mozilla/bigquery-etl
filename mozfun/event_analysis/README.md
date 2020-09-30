# Event Analysis

These functions are specific for use with the `events_daily` and `event_types` tables.
By themselves, these two tables are nearly impossible to use since the event history
is compressed; however, these stored procedures should make the data accessible.

The `events_daily` table is created as a result of two steps:
1. Map each event to a single UTF8 char which will represent it
2. Group each client-day and store a string that records, using the
    compressed format, that clients' event history for that day.
    The characters are ordered by the timestamp which they appeared
    that day.

We primarily enable access to this data by creating views which do the heavy lifting.
For example, to see which clients completed a certain action, we can create a view that
knows what that action's representation is (using the compressed mapping from 1.) and
create a regex string that checks for the presence of that event. The view makes this
transparent, and allows users to simply query a boolean field representing the presence
of that event on that day.
