CREATE
KEYSPACE streamlayer
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE streamlayer;

CREATE TABLE bus_pass_count
(
    bus_id     int,
    timestamp  timestamp,
    capacity   int,
    pass_count int,
    PRIMARY KEY (bus_id, timestamp)
);

// inserts dummy data of passengers count for each bus - it is required for the first app execution.
INSERT INTO bus_pass_count values (1, '2011-02-03T04:05:00', 500, 160);
INSERT INTO bus_pass_count values (2, '2011-02-03T04:05:00', 500, 160);
INSERT INTO bus_pass_count values (3, '2011-02-03T04:05:00', 500, 160);