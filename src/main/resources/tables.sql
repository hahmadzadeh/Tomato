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
    best_edge integer,
    test_edge integer,
    in_branch integer,
    level integer,
    find_count integer,
    state smallint,
    fragment_id character varying COLLATE pg_catalog."default",
    best_weight character varying COLLATE pg_catalog."default"

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
