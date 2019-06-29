-- Table: tomato."Message"

-- DROP TABLE tomato."Message";

CREATE TABLE tomato."Message"
(
    id integer,
    value json,
    processed boolean
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE tomato."Message"
    OWNER to tomato;

--------------------------------------------------------
CREATE TABLE tomato."Neighbour2"
(
    source integer,
    dest integer,
    type smallint,
    weight double precision
)
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

ALTER TABLE tomato."Neighbour2"
    OWNER to tomato;

--------------------------------------------------------


-- Table: tomato."Neighbour"

-- DROP TABLE tomato."Neighbour";

CREATE TABLE tomato."Neighbour"
(
    source integer,
    dest integer,
    type smallint,
    weight double precision
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE tomato."Neighbour"
    OWNER to tomato;

----------------------------------------------------------

-- Table: tomato."Node"

-- DROP TABLE tomato."Node";

CREATE TABLE tomato."Node"
(
    id integer,
    best_edge integer default -1,
    test_edge integer default -1,
    in_branch integer default -1,
    level integer default 0,
    find_count integer default -1,
    state smallint default 1,
    fragment_id character varying COLLATE pg_catalog."default" default '',
    best_weight character varying COLLATE pg_catalog."default" default '1.7976931348623157E308#0#1'
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE tomato."Node"
    OWNER to tomato;

-----------------------------------------------------
ALTER TABLE tomato."Node" ADD COLUMN iid SERIAL PRIMARY KEY;

ALTER SEQUENCE "Node_iid_seq" RESTART WITH 1;

UPDATE "Node" SET iid=nextval('"Node_iid_seq"');
------------------------------

CREATE TABLE tomato."Node2"
(
    id integer,
    best_edge integer default -1,
    test_edge integer default -1,
    in_branch integer default -1,
    level integer default 0,
    find_count integer default -1,
    state smallint default 1,
    fragment_id character varying COLLATE pg_catalog."default" default '',
    best_weight character varying COLLATE pg_catalog."default" default '1.7976931348623157E308#0#1'
)
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

ALTER TABLE tomato."Node2"
    OWNER to tomato;

-----------------------------------------------------
ALTER TABLE tomato."Node2" ADD COLUMN iid SERIAL PRIMARY KEY;

ALTER SEQUENCE "Node2_iid_seq" RESTART WITH 1;

UPDATE "Node2" SET iid=nextval('"Node_iid_seq"');

-----------------------------------------------

---------------------------
CREATE TABLE tomato."edge_temp"
(
    source integer,
    dest integer,
    weight double precision
)
    WITH (
        OIDS = FALSE
    )
    TABLESPACE pg_default;

drop index tta;

CREATE index tta on tomato."Neighbour" using btree (source, dest);